# -*- coding: utf-8 -*-
# webhook_bot.py — Telegram-бот (aiogram 3.x) для Render (Web Service)
# Доступ без пароля: при /start бот просит ФИО (как в CSV) и регистрирует.
# Команды: /start /logout /today /tomorrow /week /whoami /set_timezone /notify_on /notify_off
# Кнопки: Сегодня / Завтра / Неделя / Мой профиль / 🔔 Вкл / 🔕 Выкл / 🚪 Выйти
# Уведомления: утреннее 08:00 + за 10 минут до урока (по личному TZ).

import os
import asyncio
import logging
import csv
import sqlite3
from contextlib import closing
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ---------- ENV ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Env BOT_TOKEN is empty")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me")
BASE_URL = os.getenv("BASE_URL")  # https://<твой-сервис>.onrender.com

SCHEDULE_CSV      = "personal_schedule_all.csv"
DEFAULT_TZ        = "Europe/Moscow"
REMIND_BEFORE_MIN = 10

# ---------- LOG ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# ---------- CORE ----------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# ---------- UI ----------
BTN_TODAY = "Сегодня"
BTN_TOMORROW = "Завтра"
BTN_WEEK = "Неделя"
BTN_PROFILE = "Мой профиль"
BTN_ON = "🔔 Вкл"
BTN_OFF = "🔕 Выкл"
BTN_LOGOUT = "🚪 Выйти"
BUTTON_SET = {BTN_TODAY, BTN_TOMORROW, BTN_WEEK, BTN_PROFILE, BTN_ON, BTN_OFF, BTN_LOGOUT}

def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
            [KeyboardButton(text=BTN_WEEK), KeyboardButton(text=BTN_PROFILE)],
            [KeyboardButton(text=BTN_ON), KeyboardButton(text=BTN_OFF)],
            [KeyboardButton(text=BTN_LOGOUT)],
        ],
        resize_keyboard=True,
    )

# ---------- CSV расписание в памяти ----------
PERSONAL = []
DAY_MAP = {"Mon":"Пн","Tue":"Вт","Wed":"Ср","Thu":"Чт","Fri":"Пт","Sat":"Сб","Sun":"Вс"}

def load_personal_csv():
    """Читает CSV в оперативку. Формат: ФИО,день,урок,начало,конец,класс-столбец,предмет"""
    global PERSONAL
    PERSONAL = []
    with open(SCHEDULE_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["урок"] = int(row["урок"])
            row["ФИО"] = row["ФИО"].strip()
            row["день"] = row["день"].strip()
            row["класс-столбец"] = row["класс-столбец"].strip()
            row["предмет"] = row["предмет"].strip()
            PERSONAL.append(row)
    logger.info(f"Загружено {len(PERSONAL)} строк из {SCHEDULE_CSV}")

def personal_for(full_name: str, day_ru: str):
    rows = [r for r in PERSONAL if r["ФИО"].lower()==full_name.lower() and r["день"]==day_ru]
    rows.sort(key=lambda r: r["урок"])
    return rows

def strata_of_student(full_name: str):
    st = {r["класс-столбец"] for r in PERSONAL if r["ФИО"].lower()==full_name.lower()}
    return ", ".join(sorted(st)) or "—"

# ---------- DB ----------
DB = "school_bot.db"
def db(): return sqlite3.connect(DB)

def init_db():
    with closing(db()) as conn, conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_id INTEGER UNIQUE NOT NULL,
          full_name TEXT,
          timezone TEXT DEFAULT '{DEFAULT_TZ}',
          notify_enabled INTEGER DEFAULT 1
        );
        """)

def get_user(tg_id: int):
    with closing(db()) as conn, conn:
        row = conn.execute(
            "SELECT tg_id, full_name, timezone, notify_enabled FROM users WHERE tg_id=?",
            (tg_id,)
        ).fetchone()
        if not row: return None
        return {"tg_id":row[0],"full_name":row[1],"timezone":row[2],
                "notify_enabled":bool(row[3])}

def ensure_user(tg_id:int):
    if not get_user(tg_id):
        with closing(db()) as conn, conn:
            conn.execute("INSERT INTO users(tg_id) VALUES(?)", (tg_id,))

def set_full_name(tg_id:int, full_name:str|None):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET full_name=? WHERE tg_id=?", (full_name, tg_id))

def set_notify(tg_id:int, enabled:bool):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET notify_enabled=? WHERE tg_id=?", (1 if enabled else 0, tg_id))

def set_timezone(tg_id:int, tz:str):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET timezone=? WHERE tg_id=?", (tz, tg_id))

# ---------- Рендер сообщений ----------
def fmt_lesson(r:dict)->str:
    return f"<b>{r['начало']}-{r['конец']}</b> • {r['предмет']} ({r['класс-столбец']})"

def render_day(full_name:str, day:date, tz:ZoneInfo)->str:
    day_ru = DAY_MAP[day.strftime("%a")]
    rows = personal_for(full_name, day_ru)
    head = f"<b>{day_ru}, {day.strftime('%d.%m.%Y')}</b>"
    if not rows:
        return head + "\nНет уроков 🎉"
    return "\n".join([head] + ["• " + fmt_lesson(r) for r in rows])

def render_week(full_name:str, base_day:date, tz:ZoneInfo)->str:
    monday = base_day - timedelta(days=base_day.weekday())
    days = [monday + timedelta(days=i) for i in range(5)]
    lines = [f"<b>Неделя {days[0].strftime('%d.%m')}–{days[-1].strftime('%d.%m')}</b>"]
    for d in days:
        day_ru = DAY_MAP[d.strftime("%a")]
        rows = personal_for(full_name, day_ru)
        if not rows:
            lines.append(f"{day_ru}: —")
        else:
            lines.append(f"<u>{day_ru}</u>")
            for r in rows:
                lines.append("  • " + fmt_lesson(r))
    return "\n".join(lines)

async def send(tg_id:int, text:str):
    try:
        await bot.send_message(tg_id, text)
    except Exception as e:
        logger.warning(f"send fail to {tg_id}: {e}")

# ---------- Планировщик (утро + за 10 минут) ----------
def schedule_daily_jobs():
    scheduler.remove_all_jobs()
    with closing(db()) as conn, conn:
        users = conn.execute("SELECT tg_id, full_name, timezone, notify_enabled FROM users").fetchall()

    for tg_id, full_name, tz, enabled in users:
        if not full_name:  # не зарегистрирован — пропускаем
            continue
        tzinfo = ZoneInfo(tz or DEFAULT_TZ)

        # Утренняя рассылка 08:00 местного времени
        if enabled:
            morning = datetime.now(tzinfo).replace(hour=8, minute=0, second=0, microsecond=0)
            if morning < datetime.now(tzinfo):
                morning += timedelta(days=1)
            trig_morning = CronTrigger(
                year=morning.year, month=morning.month, day=morning.day,
                hour=morning.hour, minute=morning.minute, second=0, timezone=tzinfo
            )
            async def morning_msg(chat_id=tg_id, fio=full_name, tzinfo=tzinfo):
                today = datetime.now(tzinfo).date()
                await send(chat_id, f"🌞 Доброе утро, <b>{fio}</b>!\n" + render_day(fio, today, tzinfo))
            scheduler.add_job(morning_msg, trigger=trig_morning)

        # Напоминания за 10 минут (только сегодня)
        if enabled and full_name:
            today_local = datetime.now(tzinfo).date()
            day_ru = DAY_MAP[datetime.now(tzinfo).strftime("%a")]
            rows = personal_for(full_name, day_ru)
            now_local = datetime.now(tzinfo)
            for r in rows:
                hh, mm = map(int, r["начало"].split(":"))
                dt = datetime.combine(today_local, time(hh, mm), tzinfo)
                remind_at = dt - timedelta(minutes=REMIND_BEFORE_MIN)
                if remind_at > now_local:
                    trig = CronTrigger(year=remind_at.year, month=remind_at.month, day=remind_at.day,
                                       hour=remind_at.hour, minute=remind_at.minute, second=0, timezone=tzinfo)
                    text = f"🔔 Скоро урок: <b>{r['предмет']}</b> в {r['начало']} — {r['класс-столбец']}"
                    scheduler.add_job(send, trigger=trig, args=[tg_id, text])

    # Перестраиваем каждый день в 00:05 UTC
    scheduler.add_job(schedule_daily_jobs, trigger=CronTrigger(hour=0, minute=5, timezone="UTC"))

# ---------- Гварды ----------
def guard_or_msg(u):
    if not u or not u["full_name"]:
        return "👋 Привет! Напиши свои <b>имя и фамилию</b> (как в списке), чтобы я нашёл твоё расписание."
    return None

def tz_for(u): return ZoneInfo(u["timezone"] or DEFAULT_TZ)

# ---------- Хендлеры ----------
@dp.message(Command("start"))
async def start_cmd(m: Message):
    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)
    if u and u["full_name"]:
        return await m.answer("Ты уже зарегистрирован ✅", reply_markup=main_kb())
    await m.answer(
        "Привет! Этот бот показывает твоё личное расписание.\n"
        "Напиши свои <b>имя и фамилию</b> ровно как в списке (пример: <i>Голованова Диана</i>).",
        reply_markup=main_kb()
    )

@dp.message(Command("logout"))
async def logout_cmd(m: Message):
    ensure_user(m.from_user.id)
    set_full_name(m.from_user.id, None)
    set_notify(m.from_user.id, False)
    schedule_daily_jobs()
    await m.answer(
        "Ты вышел из профиля. Чтобы зайти снова — напиши свои <b>имя и фамилию</b> как в списке.",
        reply_markup=main_kb()
    )

@dp.message(Command("whoami"))
async def whoami(m: Message):
    u = get_user(m.from_user.id)
    if not u or not u["full_name"]:
        return await m.answer("Ты пока не зарегистрирован. Пришли свои ФИО.")
    await m.answer(
        f"<b>Ты</b>: {u['full_name']}\n"
        f"Часовой пояс: {u['timezone']}\n"
        f"Уведомления: {'вкл' if u['notify_enabled'] else 'выкл'}\n"
        f"Твои колонки/страты: {strata_of_student(u['full_name'])}"
    )

def day_for_user(u, delta_days=0):
    tz = tz_for(u)
    return (datetime.now(tz) + timedelta(days=delta_days)).date(), tz

@dp.message(Command("today"))
async def today_cmd(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg: return await m.answer(msg)
    d, tz = day_for_user(u, 0)
    await m.answer(render_day(u["full_name"], d, tz))

@dp.message(Command("tomorrow"))
async def tomorrow_cmd(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg: return await m.answer(msg)
    d, tz = day_for_user(u, 1)
    await m.answer(render_day(u["full_name"], d, tz))

@dp.message(Command("week"))
async def week_cmd(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg: return await m.answer(msg)
    d, tz = day_for_user(u, 0)
    await m.answer(render_week(u["full_name"], d, tz))

@dp.message(Command("set_timezone"))
async def set_tz(m: Message):
    u = get_user(m.from_user.id)
    ensure_user(m.from_user.id)
    parts = (m.text or "").split(maxsplit=1)
    if len(parts)==1:
        return await m.answer("Пример: /set_timezone Europe/Moscow")
    tz = parts[1].strip()
    try:
        ZoneInfo(tz)
    except Exception:
        return await m.answer("Не знаю такой таймзоны 😢")
    set_timezone(m.from_user.id, tz)
    schedule_daily_jobs()
    await m.answer(f"Часовой пояс установлен: <b>{tz}</b>")

@dp.message(Command("notify_on"))
async def notify_on(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg: return await m.answer(msg)
    set_notify(m.from_user.id, True)
    schedule_daily_jobs()
    await m.answer("Уведомления включены ✅")

@dp.message(Command("notify_off"))
async def notify_off(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg: return await m.answer(msg)
    set_notify(m.from_user.id, False)
    schedule_daily_jobs()
    await m.answer("Уведомления выключены ⛔️")

# ---- Кнопки ----
@dp.message(F.text.in_(BUTTON_SET))
async def buttons_router(m: Message):
    if m.text == BTN_TODAY:    return await today_cmd(m)
    if m.text == BTN_TOMORROW: return await tomorrow_cmd(m)
    if m.text == BTN_WEEK:     return await week_cmd(m)
    if m.text == BTN_PROFILE:  return await whoami(m)
    if m.text == BTN_ON:       return await notify_on(m)
    if m.text == BTN_OFF:      return await notify_off(m)
    if m.text == BTN_LOGOUT:   return await logout_cmd(m)

# ---- Регистрация ФИО (текст) — НЕ перехватываем команды и кнопки ----
@dp.message(
    F.text,
    ~Command(commands={"start","logout","today","tomorrow","week","whoami","set_timezone","notify_on","notify_off"}),
    ~F.text.in_(BUTTON_SET)
)
async def register_name(m: Message):
    ensure_user(m.from_user.id)
    name = (m.text or "").strip()
    found = any(r for r in PERSONAL if r["ФИО"].lower()==name.lower())
    if not found:
        return await m.answer("Не нашёл такое ФИО 🙈 Проверь написание и пришли ещё раз.")
    set_full_name(m.from_user.id, name)
    schedule_daily_jobs()
    await m.answer(
        f"Нашёл! 👋 Привет, <b>{name}</b>.\nКоманды: /today /tomorrow /week",
        reply_markup=main_kb()
    )

# ---------- WEBHOOK (aiohttp) ----------
async def on_startup():
    init_db()
    load_personal_csv()
    schedule_daily_jobs()
    scheduler.start()

    if not BASE_URL:
        logger.warning("BASE_URL не задан. Задай env BASE_URL=адрес сервиса и сделай redeploy.")
        return

    webhook_url = f"{BASE_URL.rstrip('/')}/webhook/{WEBHOOK_SECRET}"
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)
    logger.info(f"Webhook set to: {webhook_url}")

async def on_shutdown(app: web.Application):
    try:
        await bot.session.close()
    finally:
        scheduler.shutdown(wait=False)

def create_app() -> web.Application:
    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(
        app, path=f"/webhook/{WEBHOOK_SECRET}"
    )
    setup_application(app, dp, bot=bot)
    app.on_startup.append(lambda app: asyncio.create_task(on_startup()))
    app.on_shutdown.append(on_shutdown)
    return app

if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
