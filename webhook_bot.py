import asyncio
import logging
import csv
import random
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import os

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ------------------------------
#   НАСТРОЙКИ БОТА
# ------------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
BASE_URL = os.getenv("BASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")
if not WEBHOOK_SECRET:
    raise RuntimeError("WEBHOOK_SECRET is not set")
if not BASE_URL:
    raise RuntimeError("BASE_URL is not set")

WEBHOOK_URL = BASE_URL.rstrip("/") + "/webhook/" + WEBHOOK_SECRET

# часовой пояс Москва
DEFAULT_TZ = "Europe/Moscow"

# Файл расписания
SCHEDULE_CSV = "personal_schedule_all.csv"

# Админы (Александра Антоновна и Алексей Михайлович)
ADMINS = {7454117594, 5729574721}

# ------------------------------
#   ЛОГИ
# ------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# ------------------------------
#   БАЗА ДАННЫХ
# ------------------------------

DB_PATH = "users.db"


def db():
    return sqlite3.connect(DB_PATH)


def create_db():
    with closing(db()) as conn, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users(
                tg_id INTEGER PRIMARY KEY,
                full_name TEXT DEFAULT '',
                timezone TEXT DEFAULT 'Europe/Moscow',
                notify_enabled INTEGER DEFAULT 1,
                banned INTEGER DEFAULT 0
            )
            """
        )


def get_user(tg_id: int):
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT tg_id, full_name, timezone, notify_enabled, banned FROM users WHERE tg_id=?",
            (tg_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "tg_id": row[0],
        "full_name": row[1],
        "timezone": row[2],
        "notify_enabled": bool(row[3]),
        "banned": bool(row[4]),
    }


def ensure_user(tg_id: int):
    u = get_user(tg_id)
    if u:
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
        conn.execute(
            "UPDATE users SET notify_enabled=? WHERE tg_id=?",
            (1 if value else 0, tg_id),
        )


def set_timezone(tg_id: int, tz: str):
    with closing(db()) as conn, conn:
        conn.execute("UPDATE users SET timezone=? WHERE tg_id=?", (tz, tg_id))


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
        PERSONAL = []
        ALL_NAMES = []


def get_lessons(full_name: str, d: date):
    wd = d.weekday()
    day_code = WEEKDAY_RU[wd]
    return sorted(
        [r for r in PERSONAL if r["ФИО"].lower() == full_name.lower() and r["день"] == day_code],
        key=lambda x: x["урок"],
    )


# ------------------------------
#   МЕНЮ КЛАВИАТУРЫ
# ------------------------------

def main_menu(is_admin: bool = False) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="📅 Сегодня"), KeyboardButton(text="📆 Неделя")],
        [KeyboardButton(text="🎮 Игры"), KeyboardButton(text="🎁 Сюрприз дня")],
        [KeyboardButton(text="👤 Профиль")],
    ]
    if is_admin:
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


def admin_menu_kb() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📚 Перечитать расписание")],
        [KeyboardButton(text="🚫 Заблокировать ученика"), KeyboardButton(text="✅ Разблокировать ученика")],
        [KeyboardButton(text="📢 Рассылка"), KeyboardButton(text="⬅️ В главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# ------------------------------
#   БОТ И ДИСПЕТЧЕР
# ------------------------------

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
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
    if u["banned"]:
        return
    if not u["notify_enabled"]:
        return
    if not u["full_name"].strip():
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
    logger.info("Scheduled morning for %s (%s) at 8:00 %s", u["tg_id"], u["full_name"], u["timezone"])


def schedule_all_morning():
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT tg_id, full_name, timezone, notify_enabled, banned FROM users"
        ).fetchall()
    scheduler.remove_all_jobs()
    for tg_id, full_name, timezone, notify_enabled, banned in rows:
        u = {
            "tg_id": tg_id,
            "full_name": full_name,
            "timezone": timezone,
            "notify_enabled": bool(notify_enabled),
            "banned": bool(banned),
        }
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


# ------------------------------
#   КОМАНДЫ
# ------------------------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)
    isadm = is_admin(m.from_user.id)
    if u and u["banned"]:
        return await m.answer("🚫 Ты заблокирован.", reply_markup=main_menu(isadm))
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


@dp.message(Command("today"))
async def cmd_today(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg:
        return await m.answer(msg, reply_markup=main_menu(is_admin(m.from_user.id)))
    tz = tz_for(u)
    today = datetime.now(tz).date()
    await m.answer(format_day(u["full_name"], today))


@dp.message(Command("tomorrow"))
async def cmd_tomorrow(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg:
        return await m.answer(msg, reply_markup=main_menu(is_admin(m.from_user.id)))
    tz = tz_for(u)
    tomorrow = (datetime.now(tz) + timedelta(days=1)).date()
    await m.answer(format_day(u["full_name"], tomorrow))


@dp.message(Command("week"))
async def cmd_week(m: Message):
    u = get_user(m.from_user.id)
    msg = guard_or_msg(u)
    if msg:
        return await m.answer(msg, reply_markup=main_menu(is_admin(m.from_user.id)))
    tz = tz_for(u)
    today = datetime.now(tz).date()
    await m.answer(format_week(u["full_name"], today))


@dp.message(Command("whoami"))
async def cmd_whoami(m: Message):
    u = get_user(m.from_user.id)
    isadm = is_admin(m.from_user.id)
    if not u:
        return await m.answer(
            "Ты ещё не зарегистрирован. Напиши свои имя и фамилию.",
            reply_markup=main_menu(isadm),
        )
    txt = (
        f"👤 <b>Профиль</b>\n"
        f"ID: <code>{u['tg_id']}</code>\n"
        f"ФИО: {u['full_name'] or '—'}\n"
        f"Часовой пояс: {u['timezone']}\n"
        f"Уведомления: {'включены' if u['notify_enabled'] else 'выключены'}\n"
        f"Статус: {'🚫 заблокирован' if u['banned'] else '✅ активен'}"
    )
    await m.answer(txt, reply_markup=main_menu(isadm))


@dp.message(Command("notify_on"))
async def cmd_notify_on(m: Message):
    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)
    if u["banned"]:
        return await m.answer(
            "🚫 Ты заблокирован.", reply_markup=main_menu(is_admin(m.from_user.id))
        )
    set_notify(m.from_user.id, True)
    schedule_all_morning()
    await m.answer(
        "🔔 Уведомления включены.", reply_markup=main_menu(is_admin(m.from_user.id))
    )


@dp.message(Command("notify_off"))
async def cmd_notify_off(m: Message):
    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)
    if u["banned"]:
        return await m.answer(
            "🚫 Ты заблокирован.", reply_markup=main_menu(is_admin(m.from_user.id))
        )
    set_notify(m.from_user.id, False)
    schedule_all_morning()
    await m.answer(
        "🔕 Уведомления выключены.", reply_markup=main_menu(is_admin(m.from_user.id))
    )


# ------------------------------
#   ИГРЫ
# ------------------------------

GUESS_GAME = {}
ADMIN_STATE = {}  # {tg_id: {"mode": "ban" | "unban" | "broadcast"}}


@dp.message(Command("games"))
async def cmd_games(m: Message):
    await m.answer(
        "🎮 Выбирай игру из меню ниже 👇",
        reply_markup=games_menu(),
    )


@dp.message(F.text == "🎮 Игры")
async def btn_games(m: Message):
    await cmd_games(m)


@dp.message(F.text == "⬅️ В главное меню")
async def btn_back_to_main(m: Message):
    await m.answer(
        "Возвращаемся в главное меню.",
        reply_markup=main_menu(is_admin(m.from_user.id)),
    )


# Кнопочные игры с эмодзи (кубик, дартс и т.д.)

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


# Угадай число (1–20)

@dp.message(F.text == "❓ Угадай число")
async def game_guess_start(m: Message):
    GUESS_GAME[m.from_user.id] = {"number": random.randint(1, 20), "tries": 0}
    await m.answer(
        "Я загадал число от 1 до 20. Пиши числа, а я скажу больше/меньше.\n"
        "Напиши «стоп», чтобы выйти из игры."
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


# Камень-ножницы-бумага

@dp.message(F.text == "✂️ Камень-ножницы-бумага")
async def game_rps_help(m: Message):
    await m.answer(
        "Напиши слово: «камень», «ножницы» или «бумага», и я сыграю с тобой 🙂"
    )


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
#   СЮРПРИЗ ДНЯ
# ------------------------------

@dp.message(Command("surprise"))
async def cmd_surprise(m: Message):
    await m.answer(
        surprise_for_today(), reply_markup=main_menu(is_admin(m.from_user.id))
    )


@dp.message(F.text == "🎁 Сюрприз дня")
async def btn_surprise(m: Message):
    await cmd_surprise(m)


# ------------------------------
#   АДМИН-КОМАНДЫ
# ------------------------------

@dp.message(Command("ban"))
async def cmd_ban(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ У тебя нет прав администратора.")
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Использование: /ban Фамилия Имя")
    name = parts[1].strip()

    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT tg_id, full_name, banned FROM users WHERE lower(full_name)=lower(?)",
            (name,),
        ).fetchall()

    if not rows:
        return await m.answer(f"Пользователь с ФИО «{name}» не найден.")
    if len(rows) > 1:
        text = "Найдено несколько пользователей с таким ФИО:\n"
        text += "\n".join(f"• {r[1]} (id={r[0]}, banned={r[2]})" for r in rows)
        return await m.answer(text)

    tg_id, full_name, banned = rows[0]
    set_banned(tg_id, True)
    schedule_all_morning()
    await m.answer(f"🚫 Пользователь <b>{full_name}</b> заблокирован.")


@dp.message(Command("unban"))
async def cmd_unban(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ У тебя нет прав администратора.")
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Использование: /unban Фамилия Имя")
    name = parts[1].strip()

    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT tg_id, full_name, banned FROM users WHERE lower(full_name)=lower(?)",
            (name,),
        ).fetchall()

    if not rows:
        return await m.answer(f"Пользователь с ФИО «{name}» не найден.")
    if len(rows) > 1:
        text = "Найдено несколько пользователей с таким ФИО:\n"
        text += "\n".join(f"• {r[1]} (id={r[0]}, banned={r[2]})" for r in rows)
        return await m.answer(text)

    tg_id, full_name, banned = rows[0]
    set_banned(tg_id, False)
    schedule_all_morning()
    await m.answer(f"✅ Пользователь <b>{full_name}</b> разбанен.")


@dp.message(Command("broadcast"))
async def cmd_broadcast(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ У тебя нет прав администратора.")
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Использование: /broadcast [текст]")
    text = parts[1]
    with closing(db()) as conn:
        rows = conn.execute("SELECT tg_id, banned FROM users").fetchall()
    sent = 0
    for tg_id, banned in rows:
        if banned:
            continue
        try:
            await bot.send_message(tg_id, f"📢 <b>Объявление</b>\n{text}")
            sent += 1
        except Exception as e:
            logger.warning("Failed broadcast to %s: %s", tg_id, e)
    await m.answer(f"Готово. Отправлено {sent} пользователям.")


@dp.message(Command("admin"))
async def cmd_admin(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ У тебя нет прав администратора.")
    await m.answer(
        "🛠 <b>Админ-панель</b>\n"
        "Выбирай действие кнопками ниже 👇",
        reply_markup=admin_menu_kb(),
    )


@dp.message(F.text == "🛠 Админ-меню")
async def btn_admin_menu(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ У тебя нет прав администратора.")
    await cmd_admin(m)


@dp.message(Command("reload_schedule"))
async def cmd_reload_schedule(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ У тебя нет прав администратора.")
    load_personal()
    await m.answer("📚 Расписание перечитано из CSV.", reply_markup=admin_menu_kb())


@dp.message(Command("stats"))
async def cmd_stats(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("❌ У тебя нет прав администратора.")
    with closing(db()) as conn:
        total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        banned = conn.execute("SELECT COUNT(*) FROM users WHERE banned=1").fetchone()[0]
        with_name = conn.execute("SELECT COUNT(*) FROM users WHERE full_name!=''").fetchone()[0]
    await m.answer(
        "📊 <b>Статистика</b>\n"
        f"Всего пользователей: {total}\n"
        f"С указ. ФИО: {with_name}\n"
        f"Заблокировано: {banned}",
        reply_markup=admin_menu_kb(),
    )


# --------- КНОПКИ АДМИН-ПАНЕЛИ ---------

@dp.message(F.text == "📊 Статистика")
async def btn_admin_stats(m: Message):
    if not is_admin(m.from_user.id):
        return
    await cmd_stats(m)


@dp.message(F.text == "📚 Перечитать расписание")
async def btn_admin_reload_schedule(m: Message):
    if not is_admin(m.from_user.id):
        return
    await cmd_reload_schedule(m)


@dp.message(F.text == "🚫 Заблокировать ученика")
async def btn_admin_ban_mode(m: Message):
    if not is_admin(m.from_user.id):
        return
    ADMIN_STATE[m.from_user.id] = {"mode": "ban"}
    await m.answer(
        "Введи ФИО ученика, которого нужно заблокировать (точно как в списке):\n"
        "<i>Например: Иванов Иван</i>",
        reply_markup=admin_menu_kb(),
    )


@dp.message(F.text == "✅ Разблокировать ученика")
async def btn_admin_unban_mode(m: Message):
    if not is_admin(m.from_user.id):
        return
    ADMIN_STATE[m.from_user.id] = {"mode": "unban"}
    await m.answer(
        "Введи ФИО ученика, которого нужно разблокировать (точно как в списке):\n"
        "<i>Например: Иванов Иван</i>",
        reply_markup=admin_menu_kb(),
    )


@dp.message(F.text == "📢 Рассылка")
async def btn_admin_broadcast_mode(m: Message):
    if not is_admin(m.from_user.id):
        return
    ADMIN_STATE[m.from_user.id] = {"mode": "broadcast"}
    await m.answer(
        "Отправь текст рассылки одним сообщением.\n"
        "Я отправлю его всем незаблокированным пользователям.",
        reply_markup=admin_menu_kb(),
    )


# ------------------------------
#   КНОПКИ МЕНЮ (ТЕКСТОВЫЕ)
# ------------------------------

@dp.message(F.text == "📅 Сегодня")
async def btn_today(m: Message):
    await cmd_today(m)


@dp.message(F.text == "📆 Неделя")
async def btn_week(m: Message):
    await cmd_week(m)


@dp.message(F.text == "👤 Профиль")
async def btn_profile(m: Message):
    await cmd_whoami(m)


# ------------------------------
#   РЕГИСТРАЦИЯ ФИО / АДМИН-РЕЖИМЫ
# ------------------------------

@dp.message(F.text, ~F.text.startswith("/"))
async def register_name(m: Message):
    # Сначала проверяем: может, админ сейчас в режиме ban/unban/broadcast
    if is_admin(m.from_user.id):
        state = ADMIN_STATE.get(m.from_user.id)
        if state:
            mode = state.get("mode")
            text = (m.text or "").strip()
            ADMIN_STATE.pop(m.from_user.id, None)

            # ----- BAN -----
            if mode == "ban":
                name = text
                with closing(db()) as conn:
                    rows = conn.execute(
                        "SELECT tg_id, full_name, banned FROM users WHERE lower(full_name)=lower(?)",
                        (name,),
                    ).fetchall()

                if not rows:
                    return await m.answer(
                        f"Пользователь с ФИО «{name}» не найден.",
                        reply_markup=admin_menu_kb(),
                    )
                if len(rows) > 1:
                    txt = "Найдено несколько пользователей с таким ФИО:\n"
                    txt += "\n".join(f"• {r[1]} (id={r[0]}, banned={r[2]})" for r in rows)
                    return await m.answer(txt, reply_markup=admin_menu_kb())

                tg_id, full_name, banned = rows[0]
                set_banned(tg_id, True)
                schedule_all_morning()
                return await m.answer(
                    f"🚫 Пользователь <b>{full_name}</b> заблокирован.",
                    reply_markup=admin_menu_kb(),
                )

            # ----- UNBAN -----
            if mode == "unban":
                name = text
                with closing(db()) as conn:
                    rows = conn.execute(
                        "SELECT tg_id, full_name, banned FROM users WHERE lower(full_name)=lower(?)",
                        (name,),
                    ).fetchall()

                if not rows:
                    return await m.answer(
                        f"Пользователь с ФИО «{name}» не найден.",
                        reply_markup=admin_menu_kb(),
                    )
                if len(rows) > 1:
                    txt = "Найдено несколько пользователей с таким ФИО:\n"
                    txt += "\n".join(f"• {r[1]} (id={r[0]}, banned={r[2]})" for r in rows)
                    return await m.answer(txt, reply_markup=admin_menu_kb())

                tg_id, full_name, banned = rows[0]
                set_banned(tg_id, False)
                schedule_all_morning()
                return await m.answer(
                    f"✅ Пользователь <b>{full_name}</b> разбанен.",
                    reply_markup=admin_menu_kb(),
                )

            # ----- BROADCAST -----
            if mode == "broadcast":
                broadcast_text = text
                sent = 0
                with closing(db()) as conn:
                    rows = conn.execute("SELECT tg_id, banned FROM users").fetchall()
                for tg_id, banned in rows:
                    if banned:
                        continue
                    try:
                        await bot.send_message(
                            tg_id,
                            f"📢 <b>Объявление</b>\n{broadcast_text}",
                        )
                        sent += 1
                    except Exception as e:
                        logger.warning("Failed broadcast to %s: %s", tg_id, e)
                return await m.answer(
                    f"Готово. Отправлено {sent} пользователям.",
                    reply_markup=admin_menu_kb(),
                )

    # --- Обычная логика регистрации ФИО ---

    ensure_user(m.from_user.id)
    u = get_user(m.from_user.id)

    if u["banned"]:
        return await m.answer(
            "🚫 Ты заблокирован.", reply_markup=main_menu(is_admin(m.from_user.id))
        )

    if u["full_name"].strip():
        return await m.answer(
            "У тебя уже записано ФИО.\n"
            "Если хочешь изменить — напиши новое ФИО после /start.",
            reply_markup=main_menu(is_admin(m.from_user.id)),
        )

    raw = (m.text or "").strip()
    if not raw:
        return await m.answer("Напиши свои имя и фамилию (например: Иванов Иван).")

    exact = next((n for n in ALL_NAMES if n.lower() == raw.lower()), None)
    if exact:
        set_full_name(m.from_user.id, exact)
        schedule_all_morning()
        return await m.answer(
            f"Отлично! Нашёл тебя как <b>{exact}</b> ✅\n"
            "Теперь можешь пользоваться командами /today, /week, /games и кнопками меню.",
            reply_markup=main_menu(is_admin(m.from_user.id)),
        )

    import difflib

    suggestions = difflib.get_close_matches(raw, ALL_NAMES, n=5, cutoff=0.5)
    if suggestions:
        text = "Я не нашёл точного совпадения 🙈\nВозможно, ты из этого списка:\n"
        text += "\n".join(f"• {s}" for s in suggestions)
        text += "\n\nСкопируй правильный вариант и пришли ещё раз."
        return await m.answer(
            text,
            reply_markup=main_menu(is_admin(m.from_user.id)),
        )
    else:
        return await m.answer(
            "Я не нашёл такое ФИО в списке 🙈\n"
            "Проверь написание фамилии и имени и пришли ещё раз.",
            reply_markup=main_menu(is_admin(m.from_user.id)),
        )


# ------------------------------
#   WEBHOOK ДЛЯ RENDER (AIOHTTP)
# ------------------------------

async def on_startup(app: web.Application):
    logger.info("Bot starting...")
    create_db()
    load_personal()
    scheduler.start()
    schedule_all_morning()

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(url=WEBHOOK_URL, secret_token=WEBHOOK_SECRET)
    logger.info("Webhook set to %s", WEBHOOK_URL)


async def on_shutdown(app: web.Application):
    logger.info("Bot shutting down...")
    scheduler.shutdown(wait=False)
    await bot.session.close()


def create_app():
    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(
        app, path=f"/webhook/{WEBHOOK_SECRET}"
    )
    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
