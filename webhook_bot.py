import asyncio
import logging
import csv
import random
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import os

import aiohttp

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ------------------------------
#   НАСТРОЙКИ БОТА
# ------------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "СЮДА_ТОКЕН_БОТА")

# ключ Gemini — вставь свой новый ключ сюда
GEMINI_API_KEY = "AIzaSyAB6tV1AWG5sVHjWzUvrep9NzoKieagjSg"

# Лимит вопросов к ИИ в день на одного пользователя
AI_DAILY_LIMIT = 10

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

DEFAULT_TZ = "Europe/Moscow"
SCHEDULE_CSV = "personal_schedule_all.csv"
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
os.makedirs(DATA_DIR, exist_ok=True)

ADMINS = {7454117594, 5729574721}
SUMMER_START = date(2026, 5, 25)

MAINTENANCE_MODE = False
MAINTENANCE_TEXT = "🔧 Ведутся технические работы. Бот временно недоступен. Попробуй позже!"

# ------------------------------
#   ЛОГИ
# ------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# ------------------------------
#   FSM STATES
# ------------------------------

class BroadcastState(StatesGroup):
    waiting_message = State()

class QuestionState(StatesGroup):
    waiting_question = State()

class AnswerState(StatesGroup):
    waiting_answer = State()

class BanState(StatesGroup):
    waiting_name = State()

class UnbanState(StatesGroup):
    waiting_name = State()

class AiState(StatesGroup):
    waiting_question = State()

class SetGeminiKeyState(StatesGroup):
    waiting_key = State()

# ------------------------------
#   БАЗА ДАННЫХ
# ------------------------------

DB_PATH = os.path.join(DATA_DIR, "users.db")


def db():
    return sqlite3.connect(DB_PATH)


def create_db():
    with closing(db()) as conn, conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users(
                tg_id INTEGER PRIMARY KEY,
                full_name TEXT DEFAULT '',
                timezone TEXT DEFAULT 'Europe/Moscow',
                notify_enabled INTEGER DEFAULT 1,
                banned INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS questions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_tg_id INTEGER,
                from_name TEXT,
                question TEXT,
                answered INTEGER DEFAULT 0,
                created_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_usage(
                tg_id INTEGER PRIMARY KEY,
                count INTEGER DEFAULT 0,
                last_date TEXT DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT DEFAULT ''
            )
        """)


def get_user(tg_id: int):
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT tg_id, full_name, timezone, notify_enabled, banned FROM users WHERE tg_id=?",
            (tg_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "tg_id": row[0], "full_name": row[1], "timezone": row[2],
        "notify_enabled": bool(row[3]), "banned": bool(row[4]),
    }


def ensure_user(tg_id: int):
    if get_user(tg_id):
        return
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT INTO users(tg_id, full_name, timezone, notify_enabled, banned) VALUES(?,?,?,?,?)",
            (tg_id, "", DEFAULT_TZ, 1, 0),
        )


def set_full_name(tg_id: int, name: str):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET full_name=? WHERE tg_id=?", (name, tg_id))


def set_banned(tg_id: int, value: bool):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET banned=? WHERE tg_id=?", (1 if value else 0, tg_id))


def set_notify(tg_id: int, value: bool):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET notify_enabled=? WHERE tg_id=?", (1 if value else 0, tg_id))


def set_timezone(tg_id: int, tz: str):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET timezone=? WHERE tg_id=?", (tz, tg_id))


def get_all_users():
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT tg_id, full_name, timezone, notify_enabled, banned FROM users"
        ).fetchall()
    return [
        {"tg_id": r[0], "full_name": r[1], "timezone": r[2],
         "notify_enabled": bool(r[3]), "banned": bool(r[4])}
        for r in rows
    ]


def save_question(from_tg_id: int, from_name: str, question: str) -> int:
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO questions(from_tg_id, from_name, question, answered, created_at) VALUES(?,?,?,0,?)",
            (from_tg_id, from_name, question, datetime.now().strftime("%d.%m.%Y %H:%M")),
        )
        return cur.lastrowid


def get_unanswered_questions():
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT id, from_tg_id, from_name, question, created_at FROM questions WHERE answered=0 ORDER BY id"
        ).fetchall()
    return [{"id": r[0], "from_tg_id": r[1], "from_name": r[2], "question": r[3], "created_at": r[4]} for r in rows]


def mark_answered(question_id: int):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE questions SET answered=1 WHERE id=?", (question_id,))


# --- AI лимиты ---

def get_ai_usage(tg_id: int) -> int:
    today = date.today().isoformat()
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT count, last_date FROM ai_usage WHERE tg_id=?", (tg_id,)
        ).fetchone()
    if not row:
        return 0
    count, last_date = row
    if last_date != today:
        return 0  # новый день — счётчик обнулился
    return count


def increment_ai_usage(tg_id: int):
    today = date.today().isoformat()
    with closing(db()) as conn, conn:
        row = conn.execute(
            "SELECT count, last_date FROM ai_usage WHERE tg_id=?", (tg_id,)
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT INTO ai_usage(tg_id, count, last_date) VALUES(?,1,?)", (tg_id, today)
            )
        elif row[1] != today:
            conn.execute(
                "UPDATE ai_usage SET count=1, last_date=? WHERE tg_id=?", (today, tg_id)
            )
        else:
            conn.execute(
                "UPDATE ai_usage SET count=count+1 WHERE tg_id=?", (tg_id,)
            )


# --- Настройки (Gemini ключ) ---

def get_setting(key: str) -> str:
    with closing(db()) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else ""


def set_setting(key: str, value: str):
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?,?)", (key, value)
        )


def get_gemini_key() -> str:
    # Сначала из базы, потом из кода
    db_key = get_setting("gemini_key")
    if db_key:
        return db_key
    return GEMINI_API_KEY


# ------------------------------
#   GEMINI API
# ------------------------------

async def ask_gemini(question: str) -> str:
    api_key = get_gemini_key()
    if not api_key or api_key == "СЮДА_СВОЙ_НОВЫЙ_КЛЮЧ":
        return "❌ ИИ-помощник не настроен. Обратись к администратору."

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"

    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "text": (
                            "Ты умный школьный помощник. Отвечай на русском языке. "
                            "Давай чёткие, понятные ответы для школьников.\n\n"
                            f"Вопрос: {question}"
                        )
                    }
                ]
            }
        ]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error("Gemini error %s: %s", resp.status, error_text)
                    return "❌ Ошибка при обращении к ИИ. Попробуй позже."
                data = await resp.json()
                answer = data["candidates"][0]["content"]["parts"][0]["text"]
                return answer
    except asyncio.TimeoutError:
        return "⏱ ИИ не ответил вовремя. Попробуй ещё раз."
    except Exception as e:
        logger.error("Gemini exception: %s", e)
        return "❌ Произошла ошибка. Попробуй позже."


# ------------------------------
#   ЗАГРУЗКА РАСПИСАНИЯ CSV
# ------------------------------

PERSONAL = []
ALL_NAMES = []
WEEKDAY_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def load_personal():
    global PERSONAL, ALL_NAMES
    PERSONAL = []
    ALL_NAMES = []
    try:
        with open(SCHEDULE_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                try:
                    r["урок"] = int(r["урок"])
                except Exception:
                    continue
                r["ФИО"] = r["ФИО"].strip()
                r["день"] = r["день"].strip()
                r["класс-столбец"] = r["класс-столбец"].strip()
                r["предмет"] = r["предмет"].strip()
                PERSONAL.append(r)
        ALL_NAMES = sorted({r["ФИО"] for r in PERSONAL})
        logger.info("Loaded %s rows, %s unique names", len(PERSONAL), len(ALL_NAMES))
    except FileNotFoundError:
        logger.error("Schedule CSV %s not found", SCHEDULE_CSV)


def get_lessons(full_name: str, d: date):
    wd = d.weekday()
    day_code = WEEKDAY_RU[wd]
    return sorted(
        [r for r in PERSONAL if r["ФИО"].lower() == full_name.lower() and r["день"] == day_code],
        key=lambda x: x["урок"],
    )


# ------------------------------
#   СЧЁТЧИК ДО КАНИКУЛ
# ------------------------------

def days_to_summer() -> str:
    today = date.today()
    if today >= SUMMER_START:
        return "🎉 Каникулы уже начались! Отдыхай!"
    delta = (SUMMER_START - today).days
    if delta == 1:
        ending = "день"
    elif 2 <= delta <= 4:
        ending = "дня"
    else:
        ending = "дней"
    return f"☀️ До летних каникул осталось <b>{delta} {ending}</b>!\n({SUMMER_START.strftime('%d.%m.%Y')})"


# ------------------------------
#   МЕНЮ КЛАВИАТУРЫ
# ------------------------------

def main_menu(is_admin_user: bool = False) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="📅 Сегодня"), KeyboardButton(text="📆 Неделя")],
        [KeyboardButton(text="🎮 Игры"), KeyboardButton(text="🎁 Сюрприз дня")],
        [KeyboardButton(text="☀️ До каникул"), KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="❓ Вопрос куратору"), KeyboardButton(text="🤖 ИИ-помощник")],
    ]
    if is_admin_user:
        kb.append([KeyboardButton(text="🛠 Админ-меню")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def games_menu() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🎲 Кубик"), KeyboardButton(text="🎯 Дартс")],
        [KeyboardButton(text="⚽ Футбол"), KeyboardButton(text="🏀 Баскетбол")],
        [KeyboardButton(text="🎳 Боулинг"), KeyboardButton(text="🎰 Казино")],
        [KeyboardButton(text="❓ Угадай число"), KeyboardButton(text="✂️ Камень-ножницы-бумага")],
        [KeyboardButton(text="⬅️ В главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def profile_menu(notify_enabled: bool) -> ReplyKeyboardMarkup:
    notify_btn = "🔕 Выкл. уведомления" if notify_enabled else "🔔 Вкл. уведомления"
    kb = [
        [KeyboardButton(text=notify_btn)],
        [KeyboardButton(text="🌍 Сменить часовой пояс")],
        [KeyboardButton(text="⬅️ В главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def timezone_menu() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🕐 Москва (UTC+3)"), KeyboardButton(text="🕒 Екатеринбург (UTC+5)")],
        [KeyboardButton(text="🕔 Новосибирск (UTC+7)"), KeyboardButton(text="🕕 Красноярск (UTC+7)")],
        [KeyboardButton(text="🕖 Иркутск (UTC+8)"), KeyboardButton(text="🕗 Якутск (UTC+9)")],
        [KeyboardButton(text="🕘 Владивосток (UTC+10)"), KeyboardButton(text="🕙 Магадан (UTC+11)")],
        [KeyboardButton(text="🕚 Камчатка (UTC+12)")],
        [KeyboardButton(text="⬅️ Назад в профиль")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def admin_menu() -> ReplyKeyboardMarkup:
    maintenance_btn = "✅ Выкл. тех. работы" if MAINTENANCE_MODE else "🔧 Вкл. тех. работы"
    kb = [
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📢 Рассылка")],
        [KeyboardButton(text="👥 Список пользователей")],
        [KeyboardButton(text="🚫 Заблокировать"), KeyboardButton(text="✅ Разблокировать")],
        [KeyboardButton(text="💬 Вопросы учеников")],
        [KeyboardButton(text="🔑 Установить AI ключ")],
        [KeyboardButton(text=maintenance_btn)],
        [KeyboardButton(text="⬅️ В главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def cancel_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
    )


def ai_menu() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="⬅️ В главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


TIMEZONE_MAP = {
    "🕐 Москва (UTC+3)": "Europe/Moscow",
    "🕒 Екатеринбург (UTC+5)": "Asia/Yekaterinburg",
    "🕔 Новосибирск (UTC+7)": "Asia/Novosibirsk",
    "🕕 Красноярск (UTC+7)": "Asia/Krasnoyarsk",
    "🕖 Иркутск (UTC+8)": "Asia/Irkutsk",
    "🕗 Якутск (UTC+9)": "Asia/Yakutsk",
    "🕘 Владивосток (UTC+10)": "Asia/Vladivostok",
    "🕙 Магадан (UTC+11)": "Asia/Magadan",
    "🕚 Камчатка (UTC+12)": "Asia/Kamchatka",
}

# ------------------------------
#   БОТ И ДИСПЕТЧЕР
# ------------------------------

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler()

# ------------------------------
#   ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ------------------------------

def is_admin(user_id: int) -> bool:
    return user_id in ADMINS


def tz_for(u) -> ZoneInfo:
    return ZoneInfo(u["timezone"] or DEFAULT_TZ)


def guard_or_msg(u):
    if not u:
        return "Ты ещё не зарегистрирован. Напиши свои имя и фамилию, как в списке."
    if u["banned"]:
        return "🚫 Ты заблокирован."
    if not u["full_name"].strip():
        return "👋 Напиши свои имя и фамилию, как в списке (например: Иванов Иван)."
    return None


def format_day(full_name: str, d: date) -> str:
    lessons = get_lessons(full_name, d)
    wd = d.weekday()
    day_ru = WEEKDAY_RU[wd]
    header = f"<b>{day_ru}, {d.strftime('%d.%m.%Y')}</b>"
    if not lessons:
        return header + "\nНет уроков 🎉"
    lines = [header]
    for r in lessons:
        lines.append(
            f"• {r['урок']}. {r['начало']}-{r['конец']} — <b>{r['предмет']}</b> ({r['класс-столбец']})"
        )
    return "\n".join(lines)


def format_week(full_name: str, base: date) -> str:
    monday = base - timedelta(days=base.weekday())
    days = [monday + timedelta(days=i) for i in range(5)]
    header = f"<b>Неделя {days[0].strftime('%d.%m')}–{days[-1].strftime('%d.%m')}</b>"
    lines = [header]
    for d in days:
        wd = d.weekday()
        day_ru = WEEKDAY_RU[wd]
        lines.append(f"\n<b>{day_ru}</b>")
        lessons = get_lessons(full_name, d)
        if not lessons:
            lines.append("• Нет уроков 🎉")
            continue
        for r in lessons:
            lines.append(
                f"• {r['урок']}. {r['начало']}-{r['конец']} — <b>{r['предмет']}</b> ({r['класс-столбец']})"
            )
    return "\n".join(lines)


# ------------------------------
#   УВЕДОМЛЕНИЯ (УТРОМ)
# ------------------------------

async def morning_job(chat_id: int, full_name: str, tz_name: str):
    tz = ZoneInfo(tz_name or DEFAULT_TZ)
    today = datetime.now(tz).date()
    txt = "🌞 Доброе утро!\n" + format_day(full_name, today)
    try:
        await bot.send_message(chat_id, txt)
    except Exception as e:
        logger.warning("Failed to send morning message to %s: %s", chat_id, e)


def schedule_morning_for_user(u):
    if u["banned"] or not u["notify_enabled"] or not u["full_name"].strip():
        return
    jid = f"morning_{u['tg_id']}"
    tz = tz_for(u)
    scheduler.add_job(
        morning_job,
        trigger=CronTrigger(hour=8, minute=0, timezone=tz),
        id=jid,
        replace_existing=True,
        args=(u["tg_id"], u["full_name"], u["timezone"]),
        misfire_grace_time=3600,
        max_instances=1,
        coalesce=True,
    )


def schedule_all_morning():
    scheduler.remove_all_jobs()
    for u in get_all_users():
        schedule_morning_for_user(u)
    logger.info("Rebuilt all morning jobs")


# ------------------------------
#   СЮРПРИЗ ДНЯ
# ------------------------------

SURPRISES = [
    "✨ Факт: у улиток до 14 000 зубов.",
    "🧩 Задача: сколько будет 15% от 240? (Ответ проверь сам 😉)",
    "💭 Цитата: «Не ошибается тот, кто ничего не делает».",
    "🧠 Лайфхак: 25–5–5 — 25 мин учёбы, 5 отдых, 5 повторение.",
    "📚 Вопрос: столица Австралии? (Канберра)",
    "🎯 Совет: делай сложное по маленьким шагам.",
    "🔬 Факт: бананы слегка радиоактивны из-за калия-40.",
    "🗺 Факт: в России 11 часовых поясов!",
]


def surprise_for_today() -> str:
    today_ord = date.today().toordinal()
    idx = today_ord % len(SURPRISES)
    return "🎁 <b>Сюрприз дня</b>\n" + SURPRISES[idx]


# ==============================
#   MIDDLEWARE — ТЕХ. РАБОТЫ
# ==============================

@dp.message.outer_middleware()
async def maintenance_middleware(handler, event: Message, data: dict):
    global MAINTENANCE_MODE
    if is_admin(event.from_user.id):
        return await handler(event, data)
    if event.text and event.text.startswith("/start"):
        if MAINTENANCE_MODE:
            return await event.answer(MAINTENANCE_TEXT)
        return await handler(event, data)
    if MAINTENANCE_MODE:
        return await event.answer(MAINTENANCE_TEXT)
    return await handler(event, data)


# ==============================
#   ХЕНДЛЕРЫ
# ==============================

@dp.message(Command("start"))
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)
    isadm = is_admin(m.from_user.id)
    if u and u["banned"]:
        return await m.answer("🚫 Ты заблокирован.")
    if u and u["full_name"].strip():
        return await m.answer(
            f"С возвращением, <b>{u['full_name']}</b>!",
            reply_markup=main_menu(isadm),
        )
    sample = ""
    if ALL_NAMES:
        sample = "\n\nПримеры:\n" + "\n".join(f"• {n}" for n in ALL_NAMES[:6])
    await m.answer(
        "Привет! Напиши свои имя и фамилию как в списке (например: Иванов Иван)." + sample,
        reply_markup=main_menu(isadm),
    )


@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой ID: <code>{m.from_user.id}</code>")


# ------------------------------
#   ГЛАВНОЕ МЕНЮ
# ------------------------------

@dp.message(F.text == "📅 Сегодня")
async def btn_today(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg:
        return await m.answer(msg, reply_markup=main_menu(is_admin(m.from_user.id)))
    tz = tz_for(u)
    today = datetime.now(tz).date()
    await m.answer(format_day(u["full_name"], today))


@dp.message(F.text == "📆 Неделя")
async def btn_week(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg:
        return await m.answer(msg, reply_markup=main_menu(is_admin(m.from_user.id)))
    tz = tz_for(u)
    today = datetime.now(tz).date()
    await m.answer(format_week(u["full_name"], today))


@dp.message(F.text == "☀️ До каникул")
async def btn_summer(m: Message):
    await m.answer(days_to_summer())


@dp.message(F.text == "🎁 Сюрприз дня")
async def btn_surprise(m: Message):
    await m.answer(surprise_for_today())


@dp.message(F.text == "⬅️ В главное меню")
async def btn_back_to_main(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Главное меню 👇", reply_markup=main_menu(is_admin(m.from_user.id)))


@dp.message(F.text == "❌ Отмена")
async def btn_cancel(m: Message, state: FSMContext):
    await state.clear()
    if is_admin(m.from_user.id):
        await m.answer("Отменено.", reply_markup=admin_menu())
    else:
        await m.answer("Отменено.", reply_markup=main_menu(is_admin(m.from_user.id)))


# ------------------------------
#   ПРОФИЛЬ
# ------------------------------

@dp.message(F.text == "👤 Профиль")
async def btn_profile(m: Message):
    u = get_user(m.from_user.id)
    if not u:
        return await m.answer("Ты ещё не зарегистрирован.", reply_markup=main_menu(is_admin(m.from_user.id)))
    txt = (
        f"👤 <b>Профиль</b>\n"
        f"ID: <code>{u['tg_id']}</code>\n"
        f"ФИО: {u['full_name'] or '—'}\n"
        f"Часовой пояс: {u['timezone']}\n"
        f"Уведомления: {'🔔 включены' if u['notify_enabled'] else '🔕 выключены'}\n"
        f"Статус: {'🚫 заблокирован' if u['banned'] else '✅ активен'}"
    )
    await m.answer(txt, reply_markup=profile_menu(u["notify_enabled"]))


@dp.message(F.text == "⬅️ Назад в профиль")
async def btn_back_to_profile(m: Message):
    await btn_profile(m)


@dp.message(F.text.in_({"🔔 Вкл. уведомления", "🔕 Выкл. уведомления"}))
async def btn_toggle_notify(m: Message):
    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)
    if u["banned"]:
        return await m.answer("🚫 Ты заблокирован.")
    new_val = not u["notify_enabled"]
    set_notify(m.from_user.id, new_val)
    schedule_all_morning()
    u = get_user(m.from_user.id)
    status = "🔔 Уведомления включены!" if new_val else "🔕 Уведомления выключены!"
    await m.answer(status, reply_markup=profile_menu(u["notify_enabled"]))


@dp.message(F.text == "🌍 Сменить часовой пояс")
async def btn_change_tz(m: Message):
    await m.answer("Выбери свой часовой пояс:", reply_markup=timezone_menu())


@dp.message(F.text.in_(TIMEZONE_MAP.keys()))
async def btn_set_tz(m: Message):
    tz_name = TIMEZONE_MAP[m.text]
    ensure_user(m.from_user.id)
    set_timezone(m.from_user.id, tz_name)
    schedule_all_morning()
    u = get_user(m.from_user.id)
    await m.answer(
        f"✅ Часовой пояс изменён на <b>{m.text}</b>",
        reply_markup=profile_menu(u["notify_enabled"]),
    )


# ------------------------------
#   🤖 ИИ-ПОМОЩНИК
# ------------------------------

@dp.message(F.text == "🤖 ИИ-помощник")
async def btn_ai(m: Message, state: FSMContext):
    u = get_user(m.from_user.id)
    if not u or u["banned"]:
        return await m.answer("🚫 Ты заблокирован.")
    if not u["full_name"].strip():
        return await m.answer("Сначала зарегистрируйся.")

    used = get_ai_usage(m.from_user.id)
    remaining = AI_DAILY_LIMIT - used

    if remaining <= 0:
        return await m.answer(
            f"😔 Ты исчерпал дневной лимит <b>{AI_DAILY_LIMIT} вопросов</b>.\n"
            f"Приходи завтра — лимит обновится в полночь! 🌙",
            reply_markup=main_menu(is_admin(m.from_user.id)),
        )

    await state.set_state(AiState.waiting_question)
    await m.answer(
        f"🤖 <b>ИИ-помощник</b>\n\n"
        f"Задай любой вопрос — я постараюсь помочь!\n"
        f"📊 Осталось вопросов сегодня: <b>{remaining}/{AI_DAILY_LIMIT}</b>\n\n"
        f"Напиши свой вопрос 👇",
        reply_markup=cancel_menu(),
    )


@dp.message(AiState.waiting_question)
async def handle_ai_question(m: Message, state: FSMContext):
    await state.clear()
    u = get_user(m.from_user.id)
    if not u or u["banned"]:
        return

    used = get_ai_usage(m.from_user.id)
    if used >= AI_DAILY_LIMIT:
        return await m.answer(
            f"😔 Лимит исчерпан. Приходи завтра!",
            reply_markup=main_menu(is_admin(m.from_user.id)),
        )

    # Показываем что думаем
    thinking_msg = await m.answer("🤖 Думаю над ответом...")

    # Запрос к Gemini
    answer = await ask_gemini(m.text)

    # Увеличиваем счётчик
    increment_ai_usage(m.from_user.id)
    used_now = get_ai_usage(m.from_user.id)
    remaining = AI_DAILY_LIMIT - used_now

    # Удаляем сообщение "думаю"
    try:
        await thinking_msg.delete()
    except Exception:
        pass

    await m.answer(
        f"🤖 <b>Ответ ИИ:</b>\n\n{answer}\n\n"
        f"📊 Осталось вопросов сегодня: <b>{remaining}/{AI_DAILY_LIMIT}</b>",
        reply_markup=main_menu(is_admin(m.from_user.id)),
    )


# ------------------------------
#   ВОПРОС КУРАТОРУ
# ------------------------------

@dp.message(F.text == "❓ Вопрос куратору")
async def btn_ask_question(m: Message, state: FSMContext):
    u = get_user(m.from_user.id)
    if not u or u["banned"]:
        return await m.answer("🚫 Ты заблокирован.")
    if not u["full_name"].strip():
        return await m.answer("Сначала зарегистрируйся.")
    await state.set_state(QuestionState.waiting_question)
    await m.answer(
        "✏️ Напиши свой вопрос куратору.\nОн получит уведомление и ответит тебе прямо в боте.",
        reply_markup=cancel_menu(),
    )


@dp.message(QuestionState.waiting_question)
async def receive_question(m: Message, state: FSMContext):
    await state.clear()
    u = get_user(m.from_user.id)
    if not u:
        return
    question_id = save_question(m.from_user.id, u["full_name"], m.text)
    for admin_id in ADMINS:
        try:
            inline_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="💬 Ответить",
                    callback_data=f"answer_{question_id}_{m.from_user.id}"
                )]
            ])
            await bot.send_message(
                admin_id,
                f"❓ <b>Новый вопрос куратору!</b>\n\n"
                f"👤 От: <b>{u['full_name']}</b> (<code>{m.from_user.id}</code>)\n"
                f"🕐 Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                f"💬 <b>Вопрос:</b>\n{m.text}",
                reply_markup=inline_kb,
            )
        except Exception as e:
            logger.warning("Failed to notify admin %s: %s", admin_id, e)
    await m.answer(
        "✅ Вопрос отправлен куратору! Ожидай ответа.",
        reply_markup=main_menu(is_admin(m.from_user.id)),
    )


@dp.callback_query(F.data.startswith("answer_"))
async def callback_answer(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return await call.answer("❌ Нет прав.", show_alert=True)
    parts = call.data.split("_")
    question_id = int(parts[1])
    student_id = int(parts[2])
    await state.set_state(AnswerState.waiting_answer)
    await state.update_data(question_id=question_id, student_id=student_id)
    await call.message.answer(
        f"✏️ Напиши ответ ученику (id: <code>{student_id}</code>).",
        reply_markup=cancel_menu(),
    )
    await call.answer()


@dp.message(AnswerState.waiting_answer)
async def send_answer(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    question_id = data.get("question_id")
    student_id = data.get("student_id")
    await state.clear()
    try:
        await bot.send_message(
            student_id,
            f"📩 <b>Ответ куратора на твой вопрос:</b>\n\n{m.text}",
        )
        mark_answered(question_id)
        await m.answer("✅ Ответ отправлен ученику!", reply_markup=admin_menu())
    except Exception as e:
        await m.answer(f"❌ Не удалось отправить ответ. Ошибка: {e}", reply_markup=admin_menu())


# ------------------------------
#   ИГРЫ
# ------------------------------

GUESS_GAME = {}


@dp.message(F.text == "🎮 Игры")
async def btn_games(m: Message):
    await m.answer("🎮 Выбирай игру 👇", reply_markup=games_menu())


@dp.message(F.text == "🎲 Кубик")
async def game_dice(m: Message):
    await bot.send_dice(m.chat.id, emoji="🎲")


@dp.message(F.text == "🎯 Дартс")
async def game_darts(m: Message):
    await bot.send_dice(m.chat.id, emoji="🎯")


@dp.message(F.text == "⚽ Футбол")
async def game_football(m: Message):
    await bot.send_dice(m.chat.id, emoji="⚽")


@dp.message(F.text == "🏀 Баскетбол")
async def game_basketball(m: Message):
    await bot.send_dice(m.chat.id, emoji="🏀")


@dp.message(F.text == "🎳 Боулинг")
async def game_bowling(m: Message):
    await bot.send_dice(m.chat.id, emoji="🎳")


@dp.message(F.text == "🎰 Казино")
async def game_casino(m: Message):
    await bot.send_dice(m.chat.id, emoji="🎰")


@dp.message(F.text == "❓ Угадай число")
async def game_guess_start(m: Message):
    GUESS_GAME[m.from_user.id] = {"number": random.randint(1, 20), "tries": 0}
    await m.answer(
        "Я загадал число от 1 до 20. Пиши числа, а я скажу больше/меньше.\n"
        "Напиши «стоп», чтобы выйти."
    )


@dp.message(F.text.func(lambda s: s is not None and s.lower() == "стоп"))
async def game_guess_stop(m: Message):
    if m.from_user.id in GUESS_GAME:
        GUESS_GAME.pop(m.from_user.id, None)
        await m.answer("Ок, игру остановили 🙂")


@dp.message(F.text.regexp(r"^\d+$"))
async def game_guess_number(m: Message):
    if m.from_user.id not in GUESS_GAME:
        return
    num = int(m.text)
    g = GUESS_GAME[m.from_user.id]
    g["tries"] += 1
    target = g["number"]
    if num == target:
        tries = g["tries"]
        GUESS_GAME.pop(m.from_user.id, None)
        await m.answer(f"🎉 Верно! Это было число <b>{target}</b>. Попыток: {tries}")
    elif num < target:
        await m.answer("Моё число больше ↑")
    else:
        await m.answer("Моё число меньше ↓")


@dp.message(F.text == "✂️ Камень-ножницы-бумага")
async def game_rps_help(m: Message):
    await m.answer("Напиши: «камень», «ножницы» или «бумага» 🙂")


@dp.message(F.text.func(lambda s: s is not None and s.lower() in {"камень", "ножницы", "бумага"}))
async def game_rps(m: Message):
    user_choice = m.text.lower()
    options = ["камень", "ножницы", "бумага"]
    bot_choice = random.choice(options)
    if user_choice == bot_choice:
        result = "🤝 Ничья!"
    elif (
        (user_choice == "камень" and bot_choice == "ножницы")
        or (user_choice == "ножницы" and bot_choice == "бумага")
        or (user_choice == "бумага" and bot_choice == "камень")
    ):
        result = "🎉 Ты выиграл!"
    else:
        result = "Я выиграл 😈"
    await m.answer(f"Ты: {user_choice}\nБот: {bot_choice}\n\n{result}")


# ------------------------------
#   АДМИН-МЕНЮ
# ------------------------------

@dp.message(F.text == "🛠 Админ-меню")
async def btn_admin_menu(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    status = "🔧 ТЕХ. РАБОТЫ ВКЛЮЧЕНЫ" if MAINTENANCE_MODE else "✅ Бот работает в штатном режиме"
    await m.answer(f"🛠 Админ-панель\n{status}", reply_markup=admin_menu())


# --- Установить AI ключ ---
@dp.message(F.text == "🔑 Установить AI ключ")
async def btn_set_ai_key(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    current = get_gemini_key()
    masked = current[:8] + "..." if current and current != "СЮДА_СВОЙ_НОВЫЙ_КЛЮЧ" else "не установлен"
    await state.set_state(SetGeminiKeyState.waiting_key)
    await m.answer(
        f"🔑 <b>Установка Gemini API ключа</b>\n\n"
        f"Текущий ключ: <code>{masked}</code>\n\n"
        f"Введи новый ключ (начинается с AIza...):",
        reply_markup=cancel_menu(),
    )


@dp.message(SetGeminiKeyState.waiting_key)
async def receive_gemini_key(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    key = m.text.strip()
    if not key.startswith("AIza"):
        return await m.answer("❌ Ключ должен начинаться с 'AIza...'. Попробуй ещё раз.")
    set_setting("gemini_key", key)
    await state.clear()
    await m.answer(
        f"✅ Gemini API ключ успешно обновлён!\n<code>{key[:8]}...</code>",
        reply_markup=admin_menu(),
    )


# --- Тех. работы ---
@dp.message(F.text.in_({"🔧 Вкл. тех. работы", "✅ Выкл. тех. работы"}))
async def btn_toggle_maintenance(m: Message):
    global MAINTENANCE_MODE
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    MAINTENANCE_MODE = not MAINTENANCE_MODE
    users = get_all_users()
    if MAINTENANCE_MODE:
        for u in users:
            if u["banned"] or u["tg_id"] in ADMINS:
                continue
            try:
                await bot.send_message(u["tg_id"], MAINTENANCE_TEXT)
            except Exception:
                pass
            await asyncio.sleep(0.05)
        await m.answer(
            "🔧 <b>Технические работы включены!</b>\nВсе пользователи уведомлены.",
            reply_markup=admin_menu(),
        )
    else:
        for u in users:
            if u["banned"] or u["tg_id"] in ADMINS:
                continue
            try:
                await bot.send_message(
                    u["tg_id"],
                    "✅ <b>Технические работы завершены!</b>\nБот снова работает!",
                    reply_markup=main_menu(False),
                )
            except Exception:
                pass
            await asyncio.sleep(0.05)
        await m.answer(
            "✅ <b>Технические работы выключены!</b>",
            reply_markup=admin_menu(),
        )


# --- Статистика ---
@dp.message(F.text == "📊 Статистика")
async def btn_stats(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    users = get_all_users()
    total = len(users)
    active = sum(1 for u in users if not u["banned"] and u["full_name"].strip())
    banned = sum(1 for u in users if u["banned"])
    no_name = sum(1 for u in users if not u["full_name"].strip())
    notify_on = sum(1 for u in users if u["notify_enabled"] and not u["banned"])
    unanswered = len(get_unanswered_questions())
    maintenance = "🔧 Включены" if MAINTENANCE_MODE else "✅ Выключены"
    ai_key = get_gemini_key()
    ai_status = "✅ Установлен" if ai_key and ai_key != "СЮДА_СВОЙ_НОВЫЙ_КЛЮЧ" else "❌ Не установлен"
    txt = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: <b>{total}</b>\n"
        f"✅ Активных: <b>{active}</b>\n"
        f"🚫 Заблокированных: <b>{banned}</b>\n"
        f"❓ Без ФИО: <b>{no_name}</b>\n"
        f"🔔 С уведомлениями: <b>{notify_on}</b>\n"
        f"💬 Неотвеченных вопросов: <b>{unanswered}</b>\n"
        f"🔧 Тех. работы: <b>{maintenance}</b>\n"
        f"🤖 AI ключ: <b>{ai_status}</b>"
    )
    await m.answer(txt, reply_markup=admin_menu())


# --- Список пользователей ---
@dp.message(F.text == "👥 Список пользователей")
async def btn_users_list(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    users = get_all_users()
    if not users:
        return await m.answer("Нет пользователей.", reply_markup=admin_menu())
    lines = [f"<b>Пользователи ({len(users)}):</b>"]
    for u in users:
        status = "🚫" if u["banned"] else "✅"
        notify = "🔔" if u["notify_enabled"] else "🔕"
        lines.append(f"{status}{notify} {u['full_name'] or '—'} (<code>{u['tg_id']}</code>)")
    await m.answer("\n".join(lines), reply_markup=admin_menu())


# --- Вопросы учеников ---
@dp.message(F.text == "💬 Вопросы учеников")
async def btn_questions_list(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    questions = get_unanswered_questions()
    if not questions:
        return await m.answer("✅ Нет неотвеченных вопросов!", reply_markup=admin_menu())
    await m.answer(f"💬 <b>Неотвеченных: {len(questions)}</b>", reply_markup=admin_menu())
    for q in questions:
        inline_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="💬 Ответить",
                callback_data=f"answer_{q['id']}_{q['from_tg_id']}"
            )]
        ])
        await m.answer(
            f"👤 <b>{q['from_name']}</b> (<code>{q['from_tg_id']}</code>)\n"
            f"🕐 {q['created_at']}\n\n💬 {q['question']}",
            reply_markup=inline_kb,
        )


# --- Заблокировать ---
@dp.message(F.text == "🚫 Заблокировать")
async def btn_ban_start(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    users = get_all_users()
    active_users = [u for u in users if not u["banned"] and u["full_name"].strip() and u["tg_id"] not in ADMINS]
    if not active_users:
        return await m.answer("Нет активных пользователей.", reply_markup=admin_menu())
    await state.set_state(BanState.waiting_name)
    kb_buttons = [[KeyboardButton(text=u["full_name"])] for u in active_users]
    kb_buttons.append([KeyboardButton(text="❌ Отмена")])
    await m.answer("Выбери кого заблокировать:", reply_markup=ReplyKeyboardMarkup(keyboard=kb_buttons, resize_keyboard=True))


@dp.message(BanState.waiting_name)
async def btn_ban_confirm(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    name = m.text.strip()
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT tg_id, full_name FROM users WHERE lower(full_name)=lower(?)", (name,)
        ).fetchall()
    if not rows:
        await state.clear()
        return await m.answer(f"Пользователь «{name}» не найден.", reply_markup=admin_menu())
    tg_id, full_name = rows[0]
    set_banned(tg_id, True)
    schedule_all_morning()
    await state.clear()
    await m.answer(f"🚫 <b>{full_name}</b> заблокирован.", reply_markup=admin_menu())


# --- Разблокировать ---
@dp.message(F.text == "✅ Разблокировать")
async def btn_unban_start(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    users = get_all_users()
    banned_users = [u for u in users if u["banned"]]
    if not banned_users:
        return await m.answer("Нет заблокированных пользователей.", reply_markup=admin_menu())
    await state.set_state(UnbanState.waiting_name)
    kb_buttons = [[KeyboardButton(text=u["full_name"])] for u in banned_users]
    kb_buttons.append([KeyboardButton(text="❌ Отмена")])
    await m.answer("Выбери кого разблокировать:", reply_markup=ReplyKeyboardMarkup(keyboard=kb_buttons, resize_keyboard=True))


@dp.message(UnbanState.waiting_name)
async def btn_unban_confirm(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    name = m.text.strip()
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT tg_id, full_name FROM users WHERE lower(full_name)=lower(?)", (name,)
        ).fetchall()
    if not rows:
        await state.clear()
        return await m.answer(f"Пользователь «{name}» не найден.", reply_markup=admin_menu())
    tg_id, full_name = rows[0]
    set_banned(tg_id, False)
    schedule_all_morning()
    await state.clear()
    await m.answer(f"✅ <b>{full_name}</b> разблокирован.", reply_markup=admin_menu())


# --- Рассылка ---
@dp.message(F.text == "📢 Рассылка")
async def btn_broadcast_start(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ Нет прав.")
    await state.set_state(BroadcastState.waiting_message)
    await m.answer("✏️ Напиши сообщение для рассылки.", reply_markup=cancel_menu())


@dp.message(BroadcastState.waiting_message)
async def btn_broadcast_send(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await state.clear()
        return
    await state.clear()
    users = get_all_users()
    text = f"📢 <b>Сообщение от администратора:</b>\n\n{m.text}"
    sent = 0
    failed = 0
    for u in users:
        if u["banned"]:
            continue
        try:
            await bot.send_message(u["tg_id"], text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await m.answer(
        f"✅ Рассылка завершена!\nОтправлено: <b>{sent}</b>\nОшибок: <b>{failed}</b>",
        reply_markup=admin_menu(),
    )


# ------------------------------
#   РЕГИСТРАЦИЯ (ввод ФИО)
# ------------------------------

@dp.message(F.text)
async def handle_text(m: Message):
    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)
    if u["banned"]:
        return await m.answer("🚫 Ты заблокирован.")
    if not u["full_name"].strip():
        name = m.text.strip()
        if ALL_NAMES and name.lower() not in [n.lower() for n in ALL_NAMES]:
            sample = "\n".join(f"• {n}" for n in ALL_NAMES[:6])
            return await m.answer(f"❌ «{name}» не найден в расписании.\n\nПримеры:\n{sample}")
        set_full_name(m.from_user.id, name)
        schedule_all_morning()
        return await m.answer(
            f"✅ Привет, <b>{name}</b>! Ты зарегистрирован.",
            reply_markup=main_menu(is_admin(m.from_user.id)),
        )
    await m.answer("Используй кнопки меню 👇", reply_markup=main_menu(is_admin(m.from_user.id)))


# ------------------------------
#   ЗАПУСК
# ------------------------------

async def main():
    create_db()
    load_personal()
    scheduler.start()
    schedule_all_morning()
    logger.info("Bot started (polling mode)")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
