# webhook_bot.py — Telegram-бот на aiogram 3.x через webhook (для Render Free Web Service)
# Команды: /start /today /tomorrow /week /whoami /set_timezone /notify_on /notify_off /reload_csv

import os
import asyncio
import logging
import csv
import sqlite3
from contextlib import closing
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# ---------- ENV ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Env BOT_TOKEN is empty")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me")   # любой безопасный ключ без пробелов
BASE_URL = os.getenv("BASE_URL")                             # https://<твой-сервис>.onrender.com

SCHEDULE_CSV   = "personal_schedule_all.csv"
DEFAULT_TZ     = "Europe/Moscow"
REMIND_BEFORE_MIN = 10
ADMIN_IDS = set()  # можно добавить свой Telegram ID (int), чтобы работала /reload_csv

# ---------- LOG ----------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# ---------- CORE ----------
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# ---------- UI ----------
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Сегодня"), KeyboardButton(text="Завтра")],
            [KeyboardButton(text="Неделя"), KeyboardButton(text="Мой профиль")],
            [KeyboardButton(text="🔔 Вкл"), KeyboardButton(text="🔕 Выкл")],
        ],
        resize_keyboard=True,
    )

# ---------- DATA (CSV in RAM) ----------
PERSONAL = []
DAY_MAP = {"Mon":"Пн","Tue":"Вт","Wed":"Ср","Thu":"Чт","Fri":"Пт","Sat":"Сб","Sun":"Вс"}

def load_personal_csv():
    global PERSONAL
    PERSONAL = []
    with open(SCHEDULE_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["урок"] = int(row["урок"])
            PERSONAL.append(row)
    logger.info(f"Загружено {len(PERSONAL)} строк из {SCHEDULE_CSV}")

def personal_for(full_name: str, day_ru: str):
    return sorted(
        [r for r in PERSONAL if r["ФИО"].strip().lower()==full_name.strip().lower() and r["день"]==day_ru],
        key=lambda r: r["урок"]
    )

# ---------- DB ----------
DB = "school_bot.db"
def db(): return sqlite3.connect(DB)

def init_db():
    with closing(db()) as conn, conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_id INTEGER UNIQUE NOT NULL,
          full_name TEXT NOT NULL,
          timezone TEXT DEFAULT '{DEFAULT_TZ}',
          notify_enabled INTEGER DEFAULT 1
        );""")

def get_user(tg_id: int):
    with closing(db()) as conn, conn:
        cur = conn.execute("SELECT tg_id, full_name, timezone, notify_enabled FROM users WHERE tg_id=?", (tg_id,))
        row = cur.fetchone()
        if not row: return None
        return {"tg_id":row[0],"full_name":row[1],"timezone":row[2],"notify_enabled":bool(row[3])}

def upsert_user(tg_id: int, full_name: str, timezone: str|None=None):
    with closing(db()) as conn, conn:
        if get_user(tg_id):
            conn.execute("UPDATE users SET full_name=?, timezone=COALESCE(?, timezone) WHERE tg_id=?",
                         (full_name, timezone, tg_id))
        else:
            conn.execute("INSERT INTO users(tg_id, full_name, timezone) VALUES(?,?,?)",
                         (tg_id, full_name, timezone or DEFAULT_TZ))

def set_notify(tg_id:int, enabled:bool):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET notify_enabled=? WHERE tg_id=?", (1 if enabled else 0, tg_id))

def set_timezone(tg_id:int, tz:str):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET timezone=? WHERE tg_id=?", (tz, tg_id))

# ---------- RENDER ----------
def fmt_lesson(r:dict)->str:
    return f"<b>{r['начало']}-{r['конец']}</b> • {r['предмет']}"

def render_day(full_name:str, day:date, tz:ZoneInfo)->str:
    day_ru = DAY_MAP[day.strftime("%a")]
    rows = personal_for(full_name, day_ru)
    head = f"<b>{day_ru}, {day.strftime('%d.%m.%Y')}</b>"
    if not rows: return head + "\nНет уроков 🎉"
    return "\n".join([head] + ["• " + fmt_lesson(r) for r in rows])

def render_week(full_name:str, base_day:date, tz:ZoneInfo)->str:
    monday = base_day - timedelta(days=base_day.weekday())
    lines = [f"<b>Неделя {monday.strftime('%d.%m')}–{(monday+timedelta(days=4)).strftime('%d.%m')}</b>"]
    for i in range(5):
        d = monday + timedelta(days=i)
        day_ru = DAY_MAP[d.strftime("%a")]
        rows = personal_for(full_name, day_ru)
        if not rows:
            lines.append(f"{day_ru}: —")
        else:
            lines.append(f"<u>{day_ru}</u>")
            lines += ["  • " + fmt_lesson(r) for r in rows]
    return "\n".join(lines)

# ---------- NOTIFY ----------
async def send(tg_id:int, text:str):
    try:
        await bot.send_message(tg_id, text)
    except Exception as e:
        logger.warning(f"send fail to {tg_id}: {e}")

def schedule_daily_jobs():
    scheduler.remove_all_jobs()
    with closing(db()) as conn, conn:
        users = conn.execute("SELECT tg_id, full_name, timezone, notify_enabled FROM users").fetchall()
    for tg_id, full_name, tz, enabled in users:
        if not enabled: continue
        tzinfo = ZoneInfo(tz or DEFAULT_TZ)
        today_local = datetime.now(tzinfo).date()
        day_ru = DAY_MAP[today_local.strftime("%a")]
        rows = personal_for(full_name, day_ru)
        now = datetime.now(tzinfo)
        for r in rows:
            hh, mm = map(int, r["начало"].split(":"))
            dt = datetime.combine(today_local, time(hh, mm), tzinfo)
            remind_at = dt - timedelta(minutes=REMIND_BEFORE_MIN)
            if remind_at > now:
                trig = CronTrigger(year=remind_at.year, month=remind_at.month, day=remind_at.day,
                                   hour=remind_at.hour, minute=remind_at.minute, second=0, timezone=tzinfo)
                text = f"🔔 Скоро урок: <b>{r['предмет']}</b> в {r['начало']} (через {REMIND_BEFORE_MIN} мин)"
                scheduler.add_job(send, trigger=trig, args=[tg_id, text])
        scheduler.add_job(schedule_daily_jobs, trigger=CronTrigger(hour=0, minute=5, timezone=tzinfo))

# ---------- HANDLERS ----------
@dp.message(Command("start"))
async def start_cmd(m: Message):
    u = get_user(m.from_user.id)
    if u:
        await m.answer("Ты уже зарегистрирован ✅", reply_markup=main_kb())
        return
    await m.answer("Привет! Напиши свои <b>имя и фамилию</b> (точно как в списке).", reply_markup=main_kb())

@dp.message(
    F.text,
    ~Command(commands={"today","tomorrow","week","whoami","set_timezone","notify_on","notify_off","reload_csv"})
)
async def register_by_name(m: Message):
    if get_user(m.from_user.id):
        return
    name = m.text.strip()
    found = any(r for r in PERSONAL if r["ФИО"].strip().lower() == name.lower())
    if not found:
        await m.answer("Не нашёл такое ФИО 🙈 Попробуй ещё раз (точно как в журнале).")
        return
    upsert_user(m.from_user.id, name)
    await m.answer(f"Нашёл! 👋 Привет, <b>{name}</b>.\nКоманды: /today /tomorrow /week", reply_markup=main_kb())

@dp.message(Command("whoami"))
async def whoami(m: Message):
    u = get_user(m.from_user.id)
    if not u: return await m.answer("Ты ещё не зарегистрирован.")
    await m.answer(f"<b>Ты</b>: {u['full_name']}\nЧасовой пояс: {u['timezone']}\n"
                   f"Уведомления: {'вкл' if u['notify_enabled'] else 'выкл'}")

def day_for_user(u, delta_days=0):
    tz = ZoneInfo(u["timezone"] or DEFAULT_TZ)
    return (datetime.now(tz) + timedelta(days=delta_days)).date(), tz

@dp.message(Command("today"))
async def today_cmd(m: Message):
    u = get_user(m.from_user.id)
    if not u: return await m.answer("Сначала регистрация.")
    d, tz = day_for_user(u, 0); await m.answer(render_day(u["full_name"], d, tz))

@dp.message(Command("tomorrow"))
async def tomorrow_cmd(m: Message):
    u = get_user(m.from_user.id)
    if not u: return await m.answer("Сначала регистрация.")
    d, tz = day_for_user(u, 1); await m.answer(render_day(u["full_name"], d, tz))

@dp.message(Command("week"))
async def week_cmd(m: Message):
    u = get_user(m.from_user.id)
    if not u: return await m.answer("Сначала регистрация.")
    d, tz = day_for_user(u, 0); await m.answer(render_week(u["full_name"], d, tz))

@dp.message(Command("set_timezone"))
async def set_tz(m: Message):
    parts = m.text.split(maxsplit=1)
    if len(parts)==1: return await m.answer("Пример: /set_timezone Europe/Moscow")
    tz = parts[1].strip()
    try: ZoneInfo(tz)
    except Exception: return await m.answer("Не знаю такой таймзоны 😢")
    set_timezone(m.from_user.id, tz); await m.answer(f"Часовой пояс установлен: <b>{tz}</b>")

@dp.message(Command("notify_on"))
async def notify_on(m: Message):
    set_notify(m.from_user.id, True); schedule_daily_jobs(); await m.answer("Уведомления включены ✅")

@dp.message(Command("notify_off"))
async def notify_off(m: Message):
    set_notify(m.from_user.id, False); schedule_daily_jobs(); await m.answer("Уведомления выключены ⛔️")

@dp.message(Command("reload_csv"))
async def reload_csv(m: Message):
    if m.from_user.id not in ADMIN_IDS: return
    load_personal_csv(); schedule_daily_jobs(); await m.answer("CSV перечитан и уведомления перестроены.")

@dp.message(F.text.in_({"Сегодня","Завтра","Неделя","Мой профиль","🔔 Вкл","🔕 Выкл"}))
async def buttons_router(m: Message):
    t = m.text
    if t=="Сегодня": await today_cmd(m)
    elif t=="Завтра": await tomorrow_cmd(m)
    elif t=="Неделя": await week_cmd(m)
    elif t=="Мой профиль": await whoami(m)
    elif t=="🔔 Вкл": await notify_on(m)
    elif t=="🔕 Выкл": await notify_off(m)

# ---------- WEBHOOK (aiohttp) ----------
async def on_startup():
    init_db()
    load_personal_csv()
    schedule_daily_jobs()
    scheduler.start()

    if not BASE_URL:
        logger.warning("BASE_URL не задан. Задай env BASE_URL=полный_адрес_сервиса и сделай redeploy.")
        return

    webhook_url = f"{BASE_URL.rstrip('/')}/webhook/{WEBHOOK_SECRET}"
    # ВАЖНО: секрет передаём в set_webhook, иначе Telegram не пришлёт заголовок и будет 401
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
    # Обработчик входящих обновлений с проверкой секретного токена
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(
        app, path=f"/webhook/{WEBHOOK_SECRET}"
    )
    setup_application(app, dp, bot=bot)
    app.on_startup.append(lambda app: asyncio.create_task(on_startup()))
    app.on_shutdown.append(on_shutdown)
    return app

if __name__ == "__main__":
    # Render пробрасывает PORT в env; дефолт — 10000
    web.run_app(create_app(), host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
