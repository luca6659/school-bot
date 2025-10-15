# -*- coding: utf-8 -*-
# webhook_bot.py — Telegram-бот (aiogram 3.x) для Render (Web Service)
# Доступ только для своих: ACCESS_CODE + проверка ФИО в CSV.
# Команды: /start /code /reset /today /tomorrow /week /whoami /set_timezone /notify_on /notify_off
# Кнопки: Сегодня / Завтра / Неделя / Мой профиль / 🔔 Вкл / 🔕 Выкл
# Функции: утреннее расписание в 08:00 (по личному TZ) + напоминания за 10 минут до урока.

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

# FSM (для ввода кода)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ---------- ENV ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Env BOT_TOKEN is empty")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me")
BASE_URL = os.getenv("BASE_URL")  # https://<твой-сервис>.onrender.com
ACCESS_CODE = os.getenv("ACCESS_CODE", "").strip()
if not ACCESS_CODE:
    raise RuntimeError("Env ACCESS_CODE is empty (set it in Render)")

SCHEDULE_CSV      = "personal_schedule_all.csv"
DEFAULT_TZ        = "Europe/Moscow"
REMIND_BEFORE_MIN = 10

# ---------- LOG ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# ---------- CORE ----------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()  # встроенное in-memory FSM здесь же
scheduler = AsyncIOScheduler()

# ---------- UI ----------
BTN_TODAY = "Сегодня"
BTN_TOMORROW = "Завтра"
BTN_WEEK = "Неделя"
BTN_PROFILE = "Мой профиль"
BTN_ON = "🔔 Вкл"
BTN_OFF = "🔕 Выкл"
BUTTON_SET = {BTN_TODAY, BTN_TOMORROW, BTN_WEEK, BTN_PROFILE, BTN_ON, BTN_OFF}

def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
            [KeyboardButton(text=BTN_WEEK), KeyboardButton(text=BTN_PROFILE)],
            [KeyboardButton(text=BTN_ON), KeyboardButton(text=BTN_OFF)],
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
          notify_enabled INTEGER DEFAULT 1,
          verified INTEGER DEFAULT 0
        );
        """)
        # мягкие миграции
        try: conn.execute("ALTER TABLE users ADD COLUMN verified INTEGER DEFAULT 0;")
        except Exception: pass
        try: conn.execute("ALTER TABLE users ADD COLUMN full_name TEXT;")
        except Exception: pass

def get_user(tg_id: int):
    with closing(db()) as conn, conn:
        row = conn.execute(
            "SELECT tg_id, full_name, timezone, notify_enabled, verified FROM users WHERE tg_id=?",
            (tg_id,)
        ).fetchone()
        if not row: return None
        return {"tg_id":row[0],"full_name":row[1],"timezone":row[2],
                "notify_enabled":bool(row[3]), "verified":bool(row[4])}

def user_exists(tg_id: int) -> bool:
    with closing(db()) as conn, conn:
        (n,) = conn.execute("SELECT COUNT(1) FROM users WHERE tg_id=? AND verified=1", (tg_id,)).fetchone()
        return n>0

def add_or_update_user(tg_id:int, full_name:str|None=None, tz:str|None=None, verified:bool|None=None):
    with closing(db()) as conn, conn:
        cur = conn.execute("SELECT tg_id FROM users WHERE tg_id=?", (tg_id,)).fetchone()
        if cur:
            conn.execute("""
                UPDATE users
                   SET full_name=COALESCE(?, full_name),
                       timezone=COALESCE(?, timezone),
                       verified=COALESCE(?, verified)
                 WHERE tg_id=?
            """, (full_name, tz, (1 if verified else 0) if verified is not None else None, tg_id))
        else:
            conn.execute("""
                INSERT INTO users(tg_id, full_name, timezone, verified)
                VALUES(?,?,?,?)
            """, (tg_id, full_name or None, tz or DEFAULT_TZ, 1 if verified else 0))

def set_verified(tg_id:int, flag:bool):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET verified=? WHERE tg_id=?", (1 if flag else 0, tg_id))

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
        users = conn.execute("SELECT tg_id, full_name, timezone, notify_enabled, verified FROM users").fetchall()

    for tg_id, full_name, tz, enabled, verified in users:
        if not verified:  # не прошёл код — не беспокоим
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

        # Напоминания за 10 минут до уроков (только сегодня)
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

# ---------- FSM для кода доступа ----------
class Reg(StatesGroup):
    wait_code = State()

def guard_or_msg(u):
    if not u or not u["verified"]:
        return "🔒 Сначала введите код доступа: /start"
    if not u["full_name"]:
        return "Пришлите свои имя и фамилию (как в списке)."
    return None

def tz_for(u): return ZoneInfo(u["timezone"] or DEFAULT_TZ)

# ---------- Хендлеры ----------
@dp.message(Command("reset"))
async def reset_cmd(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Сбросил состояние. Нажми /start и введи код доступа снова.")

@dp.message(Command("start"))
async def start_cmd(m: Message, state: FSMContext):
    # deep-link: /start <код>
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) == 2:
        supplied = parts[1].strip()
        if supplied == ACCESS_CODE:
            # проходим верификацию, без ФИО пока
            add_or_update_user(m.from_user.id, full_name=None, verified=True)
            await state.clear()
            return await m.answer(
                "Код принят ✅ Теперь пришлите свои <b>имя и фамилию</b> (как в списке).",
                reply_markup=main_kb()
            )

    u = get_user(m.from_user.id)
    if u and u["verified"]:
        if u["full_name"]:
            return await m.answer("Ты уже зарегистрирован ✅", reply_markup=main_kb())
        else:
            return await m.answer("Код уже принят ✅ Пришли свои имя и фамилию (как в списке).", reply_markup=main_kb())

    await state.set_state(Reg.wait_code)
    await m.answer(
        "Привет! Этот бот только для своего класса.\n"
        "🔒 Введи <b>код доступа</b> одним сообщением.\n"
        f"Либо так: <code>/code {ACCESS_CODE}</code>",
        reply_markup=main_kb()
    )

@dp.message(Command("code"))
async def code_cmd(m: Message, state: FSMContext):
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Напиши: /code <твой_код>")
    supplied = parts[1].strip()
    if supplied == ACCESS_CODE:
        add_or_update_user(m.from_user.id, verified=True)
        await state.clear()
        return await m.answer(
            "Код верный ✅ Теперь пришли свои <b>имя и фамилию</b> (как в списке).",
            reply_markup=main_kb()
        )
    else:
        return await m.answer("Код неверный ❌ Попробуй ещё раз или спроси учителя.")

# Принимаем либо код, либо ФИО — НО не перехватываем кнопки/команды
@dp.message(
    F.text,
    ~Command(commands={"start","code","reset","today","tomorrow","week","whoami","set_timezone","notify_on","notify_off"}),
    ~F.text.in_(BUTTON_SET)
)
async def gate_and_register(m: Message, state: FSMContext):
    u = get_user(m.from_user.id)

    # Если нет в БД — создаём пустого
    if not u:
        add_or_update_user(m.from_user.id, verified=False)

    # 1) Если не верифицирован — ожидаем код
    u = get_user(m.from_user.id)
    if not u["verified"]:
        supplied = (m.text or "").strip()
        if supplied == ACCESS_CODE:
            set_verified(m.from_user.id, True)
            return await m.answer("Код принят ✅ Теперь пришли свои <b>имя и фамилию</b> (как в списке).")
        else:
            return await m.answer("❌ Код неверный. Попробуй ещё раз или напиши /code <код>.")

    # 2) Код принят, ждём ФИО
    if not u["full_name"]:
        name = (m.text or "").strip()
        found = any(r for r in PERSONAL if r["ФИО"].lower()==name.lower())
        if not found:
            return await m.answer("Не нашёл такое ФИО 🙈 Проверь написание и пришли ещё раз.")
        add_or_update_user(m.from_user.id, full_name=name)
        schedule_daily_jobs()  # на всякий случай перестроим уведомления
        return await m.answer(
            f"Нашёл! 👋 Привет, <b>{name}</b>.\nКоманды: /today /tomorrow /week",
            reply_markup=main_kb()
        )
    # 3) Иначе — пропускаем к другим хендлерам (кнопки/команды)

# ---- Профиль и расписание ----
@dp.message(Command("whoami"))
async def whoami(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg: return await m.answer(msg)
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
    msg = guard_or_msg(u)
    if msg: return await m.answer(msg)
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

# ---- Кнопки (важно отдельно, чтобы не перехватывались регистрацией) ----
@dp.message(F.text.in_(BUTTON_SET))
async def buttons_router(m: Message):
    if m.text == BTN_TODAY:
        return await today_cmd(m)
    if m.text == BTN_TOMORROW:
        return await tomorrow_cmd(m)
    if m.text == BTN_WEEK:
        return await week_cmd(m)
    if m.text == BTN_PROFILE:
        return await whoami(m)
    if m.text == BTN_ON:
        return await notify_on(m)
    if m.text == BTN_OFF:
        return await notify_off(m)

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
    await bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)  # secret обязателен
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
