# -*- coding: utf-8 -*-
# Telegram-бот (aiogram 3.x) для Render (Web Service)
# ✔ Без пароля: регистрация по ФИО из CSV
# ✔ Кнопки: Сегодня / Завтра / Неделя / Мой профиль / 🔔 Вкл / 🔕 Выкл / 🎮 Игры / 🚪 Выйти
# ✔ Команды: /start /logout /today /tomorrow /week /whoami /set_timezone /notify_on /notify_off /reload
# ✔ Игры: орёл/решка, камень-ножницы-бумага, угадай число, мини-квиз
# ✔ Уведомления: утром 08:00 + «Сюрприз дня» в 08:05 + напоминания за 10 минут
# ✔ Надёжность уведомлений: misfire_grace_time, пересборка каждые 30 минут, построение «сегодня» на старте
# ✔ Диагностика: /test_notify и /debug_notify

import os
import asyncio
import logging
import csv
import sqlite3
import difflib
import random
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
from apscheduler.triggers.date import DateTrigger

# ---------- ENV ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Env BOT_TOKEN is empty")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me")
BASE_URL = os.getenv("BASE_URL")  # https://<твой-сервис>.onrender.com
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x]

SCHEDULE_CSV      = "personal_schedule_all.csv"
DEFAULT_TZ        = "Europe/Moscow"
REMIND_BEFORE_MIN = int(os.getenv("REMIND_BEFORE_MIN", "10"))  # можно менять через ENV

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
BTN_GAMES = "🎮 Игры"

BUTTON_SET = {BTN_TODAY, BTN_TOMORROW, BTN_WEEK, BTN_PROFILE, BTN_ON, BTN_OFF, BTN_LOGOUT, BTN_GAMES}

def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
            [KeyboardButton(text=BTN_WEEK), KeyboardButton(text=BTN_PROFILE)],
            [KeyboardButton(text=BTN_ON), KeyboardButton(text=BTN_OFF)],
            [KeyboardButton(text=BTN_GAMES), KeyboardButton(text=BTN_LOGOUT)],
        ],
        resize_keyboard=True,
    )

# ---------- CSV расписание ----------
PERSONAL = []
ALL_NAMES = []  # список всех ФИО для подсказок
DAY_MAP = {"Mon":"Пн","Tue":"Вт","Wed":"Ср","Thu":"Чт","Fri":"Пт","Sat":"Сб","Sun":"Вс"}

def load_personal_csv():
    """Читает CSV в память. Формат: ФИО,день,урок,начало,конец,класс-столбец,предмет"""
    global PERSONAL, ALL_NAMES
    PERSONAL, ALL_NAMES = [], []
    with open(SCHEDULE_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["урок"] = int(row["урок"])
            row["ФИО"] = row["ФИО"].strip()
            row["день"] = row["день"].strip()
            row["класс-столбец"] = row["класс-столбец"].strip()
            row["предмет"] = row["предмет"].strip()
            PERSONAL.append(row)
    ALL_NAMES = sorted({r["ФИО"] for r in PERSONAL})
    logger.info(f"Загружено {len(PERSONAL)} строк из {SCHEDULE_CSV}; уникальных ФИО: {len(ALL_NAMES)}")

def personal_for(full_name: str, day_ru: str):
    rows = [r for r in PERSONAL if r["ФИО"].lower()==full_name.lower() and r["день"]==day_ru]
    rows.sort(key=lambda r: r["урок"])
    return rows

def strata_of_student(full_name: str):
    st = {r["класс-столбец"] for r in PERSONAL if r["ФИО"].lower()==full_name.lower()}
    return ", ".join(sorted(st)) or "—"

def normalize_name(s: str) -> str:
    return " ".join(s.strip().split()).lower()

# ---------- DB ----------
DB = "school_bot.db"
def db(): return sqlite3.connect(DB)

def init_db():
    with closing(db()) as conn, conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tg_id INTEGER UNIQUE NOT NULL,
          full_name TEXT NOT NULL DEFAULT '',
          timezone TEXT NOT NULL DEFAULT '{DEFAULT_TZ}',
          notify_enabled INTEGER NOT NULL DEFAULT 1
        );
        """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS game_stats(
          tg_id INTEGER PRIMARY KEY,
          rps_wins INTEGER NOT NULL DEFAULT 0,
          rps_losses INTEGER NOT NULL DEFAULT 0,
          rps_draws INTEGER NOT NULL DEFAULT 0,
          quiz_score INTEGER NOT NULL DEFAULT 0
        );
        """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS poll_votes(
          tg_id INTEGER NOT NULL,
          iso_year INTEGER NOT NULL,
          iso_week INTEGER NOT NULL,
          choice TEXT NOT NULL,
          PRIMARY KEY (tg_id, iso_year, iso_week)
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
            conn.execute(
                "INSERT INTO users(tg_id, full_name, timezone, notify_enabled) VALUES(?, ?, ?, ?)",
                (tg_id, "", DEFAULT_TZ, 1)
            )

def set_full_name(tg_id:int, full_name:str|None):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET full_name=? WHERE tg_id=?", (full_name or "", tg_id))

def set_notify(tg_id:int, enabled:bool):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET notify_enabled=? WHERE tg_id=?", (1 if enabled else 0, tg_id))

def set_timezone(tg_id:int, tz:str):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET timezone=? WHERE tg_id=?", (tz, tg_id))

# ---------- Helpers ----------
def is_admin(tg_id:int)->bool:
    return tg_id in ADMIN_IDS

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

# ---------- Сюрприз дня ----------
SURPRISES = [
    "✨ Факт: У улиток 14 000 зубов, но они всё равно едят медленно.",
    "🧩 Задача: Сколько будет 15% от 240? (Подумай и проверь 😉)",
    "💭 Цитата: «Не ошибается тот, кто ничего не делает» — Т. Рузвельт.",
    "🧠 Лайфхак: 25–5–5 — 25 мин учёбы, 5 отдых, 5 повторение.",
    "📚 Мини-квиз: Сколько граней у додекаэдра? (12)",
    "🎯 Совет: Делите сложное на маленькие шаги.",
    "🔬 Факт: Бананы слегка радиоактивны из-за калия-40.",
    "🗺 Факт: В России 11 часовых поясов!",
]
def surprise_text(for_date: date)->str:
    idx = (for_date.toordinal() + 17) % len(SURPRISES)
    return "🎁 <b>Сюрприз дня</b>\n" + SURPRISES[idx]

# ---------- Планировщик: надёжная схема ----------
def job_id(prefix:str, tg_id:int, extra:str=""):
    return f"{prefix}_{tg_id}_{extra}"

def schedule_user_daily(u):
    """Планирует ежедневные утренние задачи (08:00 расписание, 08:05 сюрприз)."""
    tg_id = u["tg_id"]
    full_name = u["full_name"]
    tzinfo = ZoneInfo(u["timezone"] or DEFAULT_TZ)

    if not full_name.strip():
        return

    # Утреннее сообщение
    scheduler.add_job(
        func=lambda chat_id=tg_id, fio=full_name, tzinfo=tzinfo: asyncio.create_task(
            send(chat_id, f"🌞 Доброе утро, <b>{fio}</b>!\n" +
                 render_day(fio, datetime.now(tzinfo).date(), tzinfo))
        ),
        trigger=CronTrigger(hour=8, minute=0, timezone=tzinfo),
        id=job_id("morning", tg_id),
        replace_existing=True,
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
    )
    # Сюрприз
    scheduler.add_job(
        func=lambda chat_id=tg_id, tzinfo=tzinfo: asyncio.create_task(
            send(chat_id, surprise_text(datetime.now(tzinfo).date()))
        ),
        trigger=CronTrigger(hour=8, minute=5, timezone=tzinfo),
        id=job_id("surprise", tg_id),
        replace_existing=True,
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
    )
    logger.info(f"[schedule_user_daily] set for {tg_id} ({full_name}) tz={tzinfo}")

def schedule_user_today_reminders(u):
    """Планирует напоминания 'сегодня' за REMIND_BEFORE_MIN мин, без дублей."""
    tg_id = u["tg_id"]
    full_name = u["full_name"]
    tzinfo = ZoneInfo(u["timezone"] or DEFAULT_TZ)
    if not full_name.strip():
        return

    today = datetime.now(tzinfo).date()
    day_ru = DAY_MAP[datetime.now(tzinfo).strftime("%a")]
    rows = personal_for(full_name, day_ru)
    now_local = datetime.now(tzinfo)

    for r in rows:
        hh, mm = map(int, r["начало"].split(":"))
        start_dt = datetime.combine(today, time(hh, mm), tzinfo)
        remind_at = start_dt - timedelta(minutes=REMIND_BEFORE_MIN)
        if remind_at <= now_local:
            continue
        jid = job_id("remind", tg_id, f"{r['предмет']}_{r['начало']}")
        # Планируем (перезапишет если было)
        scheduler.add_job(
            func=lambda chat_id=tg_id, subj=r['предмет'], st=r['начало'], col=r['класс-столбец']:
                asyncio.create_task(send(chat_id, f"🔔 Скоро урок: <b>{subj}</b> в {st} — {col}")),
            trigger=DateTrigger(run_date=remind_at),
            id=jid,
            replace_existing=True,
            misfire_grace_time=3600,
            coalesce=True,
            max_instances=1,
        )
        logger.info(f"[schedule_user_today_reminders] + {jid} at {remind_at.isoformat()}")

def schedule_all_users():
    """Полная пересборка задач: утро + сегодняшние напоминания для всех с notify_enabled=1."""
    with closing(db()) as conn, conn:
        users = conn.execute(
            "SELECT tg_id, full_name, timezone, notify_enabled FROM users WHERE notify_enabled=1"
        ).fetchall()

    # утренний пересборщик
    scheduler.add_job(schedule_all_users,
                      trigger=CronTrigger(hour=0, minute=5, timezone="UTC"),
                      id="rebuild_daily_utc",
                      replace_existing=True,
                      misfire_grace_time=3600)

    # подстраховка каждые 30 минут — перестроить «сегодня» (не дублирует)
    scheduler.add_job(schedule_all_today_reminders,
                      trigger=CronTrigger(minute="*/30", timezone="UTC"),
                      id="rebuild_today_halfhour",
                      replace_existing=True,
                      misfire_grace_time=3600)

    count = 0
    for tg_id, full_name, tz, enabled in users:
        u = {"tg_id": tg_id, "full_name": full_name, "timezone": tz, "notify_enabled": bool(enabled)}
        if not u["full_name"].strip():
            continue
        schedule_user_daily(u)
        count += 1
    logger.info(f"[schedule_all_users] daily scheduled for {count} users")

def schedule_all_today_reminders():
    """Перестроить напоминания 'сегодня' для всех (без дублей, replace_existing=True)."""
    with closing(db()) as conn, conn:
        users = conn.execute(
            "SELECT tg_id, full_name, timezone, notify_enabled FROM users WHERE notify_enabled=1"
        ).fetchall()
    cnt = 0
    for tg_id, full_name, tz, enabled in users:
        u = {"tg_id": tg_id, "full_name": full_name, "timezone": tz, "notify_enabled": bool(enabled)}
        if not u["full_name"].strip():
            continue
        schedule_user_today_reminders(u)
        cnt += 1
    logger.info(f"[schedule_all_today_reminders] reminders refreshed for {cnt} users")

# ---------- Опрос недели ----------
POLL_CHOICES = ["🔥 Отлично", "🙂 Нормально", "😅 Сложно"]
def current_iso() -> tuple[int,int]:
    y, w, _ = datetime.now().isocalendar()
    return int(y), int(w)

def poll_vote(tg_id:int, choice:str):
    y, w = current_iso()
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT OR REPLACE INTO poll_votes(tg_id, iso_year, iso_week, choice) VALUES(?,?,?,?)",
            (tg_id, y, w, choice)
        )

def poll_results(iso_y:int=None, iso_w:int=None):
    if iso_y is None or iso_w is None:
        iso_y, iso_w = current_iso()
    with closing(db()) as conn, conn:
        rows = conn.execute("SELECT choice, COUNT(*) FROM poll_votes WHERE iso_year=? AND iso_week=? GROUP BY choice",
                            (iso_y, iso_w)).fetchall()
    counts = {c:0 for c in POLL_CHOICES}
    for c, cnt in rows:
        if c in counts:
            counts[c] = cnt
    total = sum(counts.values())
    return iso_y, iso_w, counts, total

# ---------- Гварды ----------
def guard_fullname(u):
    if not u or not u["full_name"] or not u["full_name"].strip():
        return ("✍️ Пришли свои <b>имя и фамилию</b> как в списке (пример: <i>Абдуллаев Абдула</i>).")
    return None

def tz_for(u): return ZoneInfo(u["timezone"] or DEFAULT_TZ)

# ---------- Команды сервиса/диагностики ----------
@dp.message(Command("reload"))
async def reload_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Команда только для админов.")
    load_personal_csv()
    schedule_all_today_reminders()
    await m.answer("CSV перечитан ✅ Напоминания на сегодня перестроены.")

@dp.message(Command("test_notify"))
async def test_notify(m: Message):
    """Тест: через 1 минуту придёт сообщение (проверка планировщика)."""
    u = get_user(m.from_user.id) or {}
    tzinfo = ZoneInfo((u.get("timezone") or DEFAULT_TZ))
    run_at = datetime.now(tzinfo) + timedelta(minutes=1)
    jid = job_id("test", m.from_user.id, str(int(run_at.timestamp())))
    scheduler.add_job(lambda: asyncio.create_task(send(m.from_user.id, "✅ Тест уведомлений: работает.")),
                      trigger=DateTrigger(run_date=run_at),
                      id=jid, replace_existing=True, misfire_grace_time=3600)
    await m.answer(f"Ок! Тест-уведомление запланировано на {run_at.strftime('%H:%M:%S')} ({u.get('timezone', DEFAULT_TZ)}).")

@dp.message(Command("debug_notify"))
async def debug_notify(m: Message):
    """Покажет все задания APScheduler, относящиеся к текущему пользователю."""
    uid = m.from_user.id
    jobs = [j for j in scheduler.get_jobs() if f"_{uid}" in j.id]
    if not jobs:
        return await m.answer("Для тебя сейчас нет активных задач.")
    lines = ["🔧 Твои задачи:"]
    for j in jobs:
        lines.append(f"• {j.id} → {j.next_run_time.isoformat() if j.next_run_time else '—'}")
    await m.answer("\n".join(lines))

# ---------- Базовые команды ----------
@dp.message(Command("start"))
async def start_cmd(m: Message):
    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)
    if u and u["full_name"] and u["full_name"].strip():
        return await m.answer(
            f"Ты уже в системе как <b>{u['full_name']}</b> ✅\n"
            "Команды: /today /tomorrow /week /whoami /notify_on /notify_off /surprise /poll /games /logout",
            reply_markup=main_kb()
        )
    hint = "Напиши свои <b>имя и фамилию</b> ровно как в списке (пример: <i>Абдуллаев Абдула</i>)."
    sample = ""
    if ALL_NAMES:
        sample = "\n\nПримеры из списка:\n• " + "\n• ".join(ALL_NAMES[:6])
    await m.answer("Привет! Я покажу твоё личное расписание.\n" + hint + sample, reply_markup=main_kb())

@dp.message(Command("logout"))
async def logout_cmd(m: Message):
    ensure_user(m.from_user.id)
    set_full_name(m.from_user.id, None)
    set_notify(m.from_user.id, False)
    await m.answer("Ты вышел из профиля. Чтобы зайти снова — пришли своё ФИО.", reply_markup=main_kb())

@dp.message(Command("whoami"))
async def whoami(m: Message):
    u = get_user(m.from_user.id)
    if msg := guard_fullname(u): return await m.answer(msg)
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
    if msg := guard_fullname(u): return await m.answer(msg)
    d, tz = day_for_user(u, 0)
    await m.answer(render_day(u["full_name"], d, tz))

@dp.message(Command("tomorrow"))
async def tomorrow_cmd(m: Message):
    u = get_user(m.from_user.id)
    if msg := guard_fullname(u): return await m.answer(msg)
    d, tz = day_for_user(u, 1)
    await m.answer(render_day(u["full_name"], d, tz))

@dp.message(Command("week"))
async def week_cmd(m: Message):
    u = get_user(m.from_user.id)
    if msg := guard_fullname(u): return await m.answer(msg)
    d, tz = day_for_user(u, 0)
    await m.answer(render_week(u["full_name"], d, tz))

@dp.message(Command("set_timezone"))
async def set_tz(m: Message):
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
    # перестроим задачи
    u = get_user(m.from_user.id)
    schedule_user_daily(u)
    schedule_user_today_reminders(u)
    await m.answer(f"Часовой пояс установлен: <b>{tz}</b>")

@dp.message(Command("notify_on"))
async def notify_on(m: Message):
    u = get_user(m.from_user.id)
    if msg := guard_fullname(u): return await m.answer(msg)
    set_notify(m.from_user.id, True)
    # сразу перестроим задачи этому пользователю
    u = get_user(m.from_user.id)
    schedule_user_daily(u)
    schedule_user_today_reminders(u)
    await m.answer("Уведомления включены ✅ (утро + напоминания за 10 мин).")

@dp.message(Command("notify_off"))
async def notify_off(m: Message):
    u = get_user(m.from_user.id)
    if msg := guard_fullname(u): return await m.answer(msg)
    set_notify(m.from_user.id, False)
    # удалим его задачи
    for j in list(scheduler.get_jobs()):
        if f"_{m.from_user.id}_" in j.id or j.id.endswith(f"_{m.from_user.id}"):
            scheduler.remove_job(j.id)
    await m.answer("Уведомления выключены ⛔️")

# ---------- Игры ----------
from random import choice as rnd_choice

@dp.message(Command("games"))
async def games_cmd(m: Message):
    await m.answer(
        "🎮 Игры:\n• 🪙 орёл/решка — напиши «монета»\n• камень/ножницы/бумага — «камень», «ножницы» или «бумага»\n"
        "• угадай число — «угадай», затем числа 1–20\n• мини-квиз — «квиз»",
        reply_markup=main_kb()
    )

GUESS = {}  # tg_id -> {"n":int, "tries":int}
QUIZ  = {}  # tg_id -> {"a":int,"b":int,"op":"+|*|−","ans":int}

def new_guess(tg_id:int):
    GUESS[tg_id] = {"n": random.randint(1, 20), "tries": 0}

def new_quiz(tg_id:int):
    a,b = random.randint(2,9), random.randint(2,9)
    op = random.choice(["+", "*", "-"])
    ans = a+b if op=="+" else (a*b if op=="*" else a-b)
    QUIZ[tg_id] = {"a":a,"b":b,"op":op,"ans":ans}

@dp.message(F.text.func(lambda s: s and s.lower()=="монета"))
async def coin(m: Message):
    await m.answer(f"🪙 Выпало: <b>{rnd_choice(['Орёл','Решка'])}</b>")

@dp.message(F.text.func(lambda s: s and s.lower()=="угадай"))
async def guess_start(m: Message):
    new_guess(m.from_user.id)
    await m.answer("Я загадал число 1–20. Пиши число! (стоп — «стоп»).")

@dp.message(F.text.regexp(r"^\d+$"))
async def guess_digit(m: Message):
    tg_id = m.from_user.id
    if tg_id not in GUESS:
        return
    n = int(m.text)
    if not (1 <= n <= 20):
        return await m.answer("Число от 1 до 20 😉")
    GUESS[tg_id]["tries"] += 1
    target = GUESS[tg_id]["n"]
    if n == target:
        tries = GUESS[tg_id]["tries"]
        del GUESS[tg_id]
        return await m.answer(f"🎉 Верно! Это <b>{target}</b>. Попыток: {tries}.")
    elif n < target:
        return await m.answer("Моё число больше ↑")
    else:
        return await m.answer("Моё число меньше ↓")

@dp.message(F.text.func(lambda s: s and s.lower() in {"стоп","stop","выйти"}))
async def guess_stop(m: Message):
    if m.from_user.id in GUESS:
        del GUESS[m.from_user.id]
        await m.answer("Ок, игру остановили.")

@dp.message(F.text.func(lambda s: s and s.lower() in {"квиз"}))
async def quiz_start(m: Message):
    new_quiz(m.from_user.id)
    q = QUIZ[m.from_user.id]
    await m.answer(f"Сколько будет: <b>{q['a']} {q['op']} {q['b']}</b> ? Ответ — числом.")

@dp.message(F.text.regexp(r"^-?\d+$"))
async def quiz_answer(m: Message):
    tg_id = m.from_user.id
    if tg_id not in QUIZ:
        return
    ans = int(m.text)
    q = QUIZ[tg_id]
    if ans == q["ans"]:
        new_quiz(tg_id)
        q2 = QUIZ[tg_id]
        return await m.answer(f"✅ Правильно!\nСледующий: <b>{q2['a']} {q2['op']} {q2['b']}</b> = ?")
    else:
        return await m.answer(f"❌ Неверно. Правильно: <b>{q['ans']}</b>. Напиши «квиз», чтобы начать заново.")

# ---------- Сюрприз дня ----------
@dp.message(Command("surprise")))
async def surprise_cmd(m: Message):
    u = get_user(m.from_user.id)
    tzinfo = ZoneInfo((u["timezone"] if u else DEFAULT_TZ))
    await m.answer(surprise_text(datetime.now(tzinfo).date()))

# ---------- Опрос недели ----------
@dp.message(Command("poll"))
async def poll_cmd(m: Message):
    y,w = current_iso()
    choices = "\n".join(f"{i+1}. {c}" for i,c in enumerate(POLL_CHOICES))
    await m.answer(
        f"🗳 <b>Опрос недели {w}/{y}</b>\nКак прошла неделя?\n{choices}\n\n"
        "Отправь номер ответа (1–3)."
    )

@dp.message(F.text.regexp(r"^[1-3]$"))
async def poll_vote_msg(m: Message):
    idx = int(m.text) - 1
    choice = POLL_CHOICES[idx]
    poll_vote(m.from_user.id, choice)
    await m.answer(f"Спасибо! Твой ответ: <b>{choice}</b>.")

@dp.message(Command("poll_stats"))
async def poll_stats_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Команда только для админов.")
    y, w, counts, total = poll_results()
    if total == 0:
        return await m.answer(f"🗳 Опрос {w}/{y}: пока нет голосов.")
    lines = [f"🗳 Опрос {w}/{y}: всего {total}"]
    for c in POLL_CHOICES:
        lines.append(f"{c}: {counts[c]}")
    await m.answer("\n".join(lines))

# ---------- Админ-панель ----------
@dp.message(Command("admin"))
async def admin_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Команда только для админов.")
    await m.answer(
        "🛠 <b>Админ-панель</b>\n"
        "• /broadcast <текст> — рассылка всем\n"
        "• /poll_stats — статистика опроса недели\n"
        "• /reload — перечитать CSV\n"
        "• /debug_notify — посмотреть свои задачи (для проверки)"
    )

@dp.message(Command("broadcast"))
async def broadcast_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Команда только для админов.")
    parts = (m.text or "").split(maxsplit=1)
    if len(parts)==1:
        return await m.answer("Использование: /broadcast <текст>")
    text = parts[1].strip()
    with closing(db()) as conn, conn:
        rows = conn.execute("SELECT tg_id FROM users").fetchall()
    sent, fail = 0, 0
    for (tg_id,) in rows:
        try:
            await bot.send_message(tg_id, f"📢 <b>Объявление</b>\n{text}")
            sent += 1
        except Exception:
            fail += 1
    await m.answer(f"Готово. Отправлено: {sent}, ошибок: {fail}.")

# ---- Кнопки основного меню ----
@dp.message(F.text.in_(BUTTON_SET))
async def buttons_router(m: Message):
    t = m.text
    if t == BTN_TODAY:    return await today_cmd(m)
    if t == BTN_TOMORROW: return await tomorrow_cmd(m)
    if t == BTN_WEEK:     return await week_cmd(m)
    if t == BTN_PROFILE:  return await whoami(m)
    if t == BTN_ON:       return await notify_on(m)
    if t == BTN_OFF:      return await notify_off(m)
    if t == BTN_LOGOUT:   return await logout_cmd(m)
    if t == BTN_GAMES:    return await games_cmd(m)

# ---- Регистрация ФИО (умная) ----
@dp.message(
    F.text,
    ~Command(commands={"start","logout","today","tomorrow","week","whoami","set_timezone",
                       "notify_on","notify_off","reload","games","surprise","poll","poll_stats",
                       "admin","broadcast","test_notify","debug_notify"}),
    ~F.text.in_(BUTTON_SET)
)
async def register_name(m: Message):
    ensure_user(m.from_user.id)
    raw = (m.text or "").strip()
    if not raw:
        return await m.answer("Пришли, пожалуйста, свои имя и фамилию.")
    want = normalize_name(raw)
    exact = next((n for n in ALL_NAMES if normalize_name(n) == want), None)
    if exact:
        set_full_name(m.from_user.id, exact)
        # построим уведомления для этого пользователя сразу
        u = get_user(m.from_user.id)
        schedule_user_daily(u)
        schedule_user_today_reminders(u)
        return await m.answer(
            f"Нашёл! 👋 Привет, <b>{exact}</b>.\nКоманды: /today /tomorrow /week /games /poll /surprise\n"
            "Совет: набери /test_notify — проверим доставку за 1 минуту.",
            reply_markup=main_kb()
        )
    tips = difflib.get_close_matches(raw, ALL_NAMES, n=5, cutoff=0.5)
    if tips:
        lines = "\n".join(f"• {t}" for t in tips)
        return await m.answer("Не нашёл такое ФИО 🙈\nВозможно, ты имел в виду:\n" + lines + "\n\nСкопируй и пришли ещё раз.")
    await m.answer("Не нашёл такое ФИО 🙈 Проверь написание и пришли ещё раз (как в списке).")

# ---------- WEBHOOK (aiohttp) ----------
async def on_startup():
    init_db()
    load_personal_csv()
    # 1) старт планировщика
    scheduler.start()
    # 2) общая пересборка ежедневных задач + подстраховка
    schedule_all_users()
    # 3) построить напоминания «сегодня» сразу после запуска
    schedule_all_today_reminders()

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
