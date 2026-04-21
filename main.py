import asyncio
import os
import json
import re
import sys
import signal
import random
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.types import (
    ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message
)
from pyrogram.errors import (
    PeerIdInvalid, Forbidden, SessionRevoked,
    AuthKeyUnregistered, Unauthorized, FloodWait,
    ApiIdInvalid, AccessTokenInvalid
)
from pyrogram.handlers import DisconnectHandler
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %name)s - %levelname)s - %message)s'
)
logger = logging.getLogger(__name__)

# --- КОНФИГ ---
API_ID = 30032542
API_HASH = "ce646da1307fb452305d49f9bb8751ca"
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8659319275:AAEaMn1u9a-iCxmGQQEpL2qOz3W7BKB0mnw)

# === РАБОЧАЯ ДИРЕКТОРИЯ ===
IS_RAILWAY = os.path.exists('/app') or 'RAILWAY_SERVICE_NAME' in os.environ

if IS_RAILWAY:
    WORK_DIR = '/app/data'
    if not os.path.exists(WORK_DIR):
        logger.error(f"❌ Volume не смонтирован в {WORK_DIR}")
        os.makedirs(WORK_DIR, exist_ok=True)
else:
    WORK_DIR = os.path.dirname(os.path.abspath(__file__))

os.makedirs(WORK_DIR, exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, 'sessions'), exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, 'user_settings'), exist_ok=True)
bot_session_dir = os.path.join(WORK_DIR, 'bot_session')
os.makedirs(bot_session_dir, exist_ok=True)

logger.info(f"📁 Рабочая директория: {WORK_DIR}")
logger.info(f"📁 На Railway: {IS_RAILWAY}")

# === НАСТРОЙКА КЛЮЧЕЙ (новые случайные форматы) ===
KEYS_FILE = os.path.join(WORK_DIR, 'activation_keys.json')

# Функция генерации случайного ключа (для автоматической выдачи)
def generate_random_key(prefix="Msdf"):
    import random
    import string
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    return f"{prefix}_{suffix}"

def load_keys():
    default_keys = {
        # Предустановленные ключи (можно менять через админку)
        "Msdf_7d9f3k_sdfs_92jd": ("Неделя", 7, False),
        "Msdf_3k9d0f_sdfs_4hrt": ("Месяц", 30, False),
        "Msdf_8g4h1t_sdfs_6jsk": ("Год", 365, False),
        "Msdf_0f2a5e_sdfs_8djs": ("Навсегда", 3650, False),
        "ADMIN_MASTER_KEY": ("Администратор", 3650, True)
    }
    try:
        if os.path.exists(KEYS_FILE):
            with open(KEYS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            with open(KEYS_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_keys, f, ensure_ascii=False, indent=2)
            return default_keys
    except Exception as e:
        logger.error(f"Ошибка загрузки ключей: {e}")
        return default_keys

def save_keys(keys):
    try:
        with open(KEYS_FILE, 'w', encoding='utf-8') as f:
            json.dump(keys, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения ключей: {e}")
        return False

ONE_TIME_KEYS = load_keys()
MAX_ACCOUNTS_PER_USER = 3
WELCOME_PHOTO_FILE = os.path.join(WORK_DIR, 'welcome_photo_id.txt')

# --- ДАННЫЕ ПОЛЬЗОВАТЕЛЕЙ ---
users_data = {}
temp_auth = {}
users_file = os.path.join(WORK_DIR, "bot_users.json")
reconnect_tasks = {}
keep_alive_tasks = {}

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def parse_key_with_username(key_text):
    pattern = r'^(.*?)-@([a-zA-Z0-9_]+)$'
    match = re.match(pattern, key_text.strip())
    if match:
        return match.group(1), match.group(2)
    else:
        return key_text.strip(), None

def get_welcome_photo_id():
    try:
        if os.path.exists(WELCOME_PHOTO_FILE):
            with open(WELCOME_PHOTO_FILE, 'r') as f:
                return f.read().strip()
    except:
        pass
    return None

def set_welcome_photo_id(file_id):
    with open(WELCOME_PHOTO_FILE, 'w') as f:
        f.write(file_id)

def is_admin(user_id):
    return users_data.get(user_id, {}).get("is_admin", False)

def has_active_subscription(user_id):
    if user_id not in users_data:
        return False
    expires = users_data[user_id]["expires"]
    if isinstance(expires, str):
        expires = datetime.fromisoformat(expires)
    return expires > datetime.now()

def save_users():
    try:
        users_to_save = {}
        for uid, data in users_data.items():
            accounts = {}
            for phone, acc in data["accounts"].items():
                clean_phone = phone.replace('+', '').replace(' ', '')
                session_path = os.path.join(WORK_DIR, 'sessions', f"{clean_phone}_{uid}")
                accounts[phone] = {
                    "text": acc["text"],
                    "interval": acc["interval"],
                    "running": False,
                    "added_date": acc["added_date"].isoformat() if isinstance(acc["added_date"], datetime) else acc["added_date"],
                    "session_name": session_path,
                    "safe_mode": acc.get("safe_mode", False),
                    "texts_list": acc.get("texts_list", []),
                    "base_interval": acc.get("base_interval", 3600)
                }
            users_to_save[str(uid)] = {
                "expires": data["expires"].isoformat() if isinstance(data["expires"], datetime) else data["expires"],
                "key_used": data["key_used"],
                "is_admin": data["is_admin"],
                "username": data.get("username", ""),
                "bound_username": data.get("bound_username", ""),
                "accounts": accounts
            }
        with open(users_file, 'w', encoding='utf-8') as f:
            json.dump(users_to_save, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения: {e}")
        return False

def load_users():
    global users_data
    try:
        if os.path.exists(users_file):
            with open(users_file, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
            for uid, data in loaded_data.items():
                uid = int(uid)
                expires = data["expires"]
                if isinstance(expires, str):
                    expires = datetime.fromisoformat(expires)
                if expires > datetime.now():
                    accounts = {}
                    for phone, acc_data in data.get("accounts", {}).items():
                        accounts[phone] = {
                            "text": acc_data["text"],
                            "interval": acc_data["interval"],
                            "running": False,
                            "added_date": datetime.fromisoformat(acc_data["added_date"]) if isinstance(acc_data.get("added_date"), str) else datetime.now(),
                            "session_name": acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{uid}")),
                            "safe_mode": acc_data.get("safe_mode", False),
                            "texts_list": acc_data.get("texts_list", []),
                            "base_interval": acc_data.get("base_interval", 3600)
                        }
                    users_data[uid] = {
                        "expires": expires,
                        "key_used": data["key_used"],
                        "is_admin": data["is_admin"],
                        "username": data.get("username", ""),
                        "bound_username": data.get("bound_username", ""),
                        "accounts": accounts
                    }
            logger.info(f"Загружено {len(users_data)} пользователей")
    except Exception as e:
        logger.error(f"Ошибка загрузки: {e}")

async def load_user_sessions():
    sessions_dir = os.path.join(WORK_DIR, 'sessions')
    if not os.path.exists(sessions_dir):
        os.makedirs(sessions_dir)
    loaded_count = 0
    for user_id, user_data in users_data.items():
        for phone, acc_data in user_data["accounts"].items():
            try:
                session_name = acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}"))
                session_file = f"{session_name}.session"
                if os.path.exists(session_file):
                    client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=WORK_DIR)
                    async def on_disconnect(client, uid=user_id, ph=phone):
                        await schedule_reconnect(uid, ph)
                    client.add_handler(DisconnectHandler(on_disconnect))
                    await client.start()
                    acc_data["client"] = client
                    task_key = f"{user_id}_{phone}"
                    if task_key in keep_alive_tasks:
                        keep_alive_tasks[task_key].cancel()
                    keep_alive_tasks[task_key] = asyncio.create_task(keep_alive(user_id, phone, client))
                    loaded_count += 1
                    logger.info(f"Сессия {phone} загружена")
            except Exception as e:
                logger.error(f"Ошибка загрузки сессии {phone}: {e}")
    return loaded_count

# --- ФУНКЦИИ ДЛЯ ПЕРЕПОДКЛЮЧЕНИЯ И KEEP-ALIVE ---
async def keep_alive(user_id, phone, client):
    key = f"{user_id}_{phone}"
    while True:
        try:
            if key not in keep_alive_tasks:
                break
            await asyncio.wait_for(client.get_me(), timeout=10)
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Keep-alive ошибка {phone}: {e}")
            if key in keep_alive_tasks:
                await schedule_reconnect(user_id, phone)
            break

async def schedule_reconnect(user_id, phone):
    key = f"{user_id}_{phone}"
    if key in reconnect_tasks:
        reconnect_tasks[key].cancel()
    if key in keep_alive_tasks:
        keep_alive_tasks[key].cancel()
    async def reconnect_with_delay():
        await asyncio.sleep(30)
        await reconnect_account(user_id, phone)
    reconnect_tasks[key] = asyncio.create_task(reconnect_with_delay())

async def reconnect_account(user_id, phone):
    if user_id not in users_data or phone not in users_data[user_id]["accounts"]:
        return
    acc_data = users_data[user_id]["accounts"][phone]
    session_name = acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}"))
    try:
        client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=WORK_DIR)
        async def on_disconnect(client, uid=user_id, ph=phone):
            await schedule_reconnect(uid, ph)
        client.add_handler(DisconnectHandler(on_disconnect))
        await client.start()
        acc_data["client"] = client
        key = f"{user_id}_{phone}"
        if key in keep_alive_tasks:
            keep_alive_tasks[key].cancel()
        keep_alive_tasks[key] = asyncio.create_task(keep_alive(user_id, phone, client))
        if acc_data.get("running", False):
            if acc_data.get("safe_mode", False):
                asyncio.create_task(safe_spam_cycle(user_id, phone, acc_data, None))
            else:
                asyncio.create_task(spam_cycle(user_id, phone, acc_data, None))
        logger.info(f"Аккаунт {phone} переподключён")
    except Exception as e:
        logger.error(f"Ошибка переподключения {phone}: {e}")
        await schedule_reconnect(user_id, phone)

# --- РАССЫЛКА (ОБЫЧНАЯ И БЕЗОПАСНАЯ) ---
async def spam_cycle(user_id, phone, data, message):
    status_msg = None
    if message:
        status_msg = await message.reply(f"🚀 Запуск рассылки для {phone}...")
    sent_chats = []
    error_count = 0
    cycle_count = 0

    while data.get("running", False):
        try:
            if "client" not in data:
                error_count += 1
                if error_count > 3:
                    break
                await asyncio.sleep(60)
                continue

            try:
                me = await data["client"].get_me()
                if not me:
                    raise Exception("Не удалось получить информацию")
            except Exception as e:
                logger.warning(f"Клиент {phone} не отвечает: {e}")
                await reconnect_account(user_id, phone)
                await asyncio.sleep(30)
                continue

            dialogs = []
            async for dialog in data["client"].get_dialogs():
                if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    dialogs.append(dialog)

            for dialog in dialogs:
                if not data.get("running", False):
                    break
                try:
                    await data["client"].send_message(dialog.chat.id, data["text"])
                    sent_chats.append(dialog.chat.title)
                    if len(sent_chats) % 5 == 0 and status_msg:
                        new_text = f"🚀 Рассылка {phone}\nЦикл #{cycle_count+1}\nОтправлено в {len(sent_chats)} чатов\nПоследние: " + ", ".join(sent_chats[-5:])
                        try:
                            await status_msg.edit_text(new_text)
                        except:
                            pass
                    await asyncio.sleep(0.5)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                except (PeerIdInvalid, Forbidden):
                    continue
                except Exception as e:
                    logger.error(f"Ошибка отправки: {e}")
                    continue

            cycle_count += 1
            error_count = 0
            wait_time = data["interval"]
            for _ in range(wait_time):
                if not data.get("running", False):
                    break
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Ошибка в цикле {phone}: {e}")
            error_count += 1
            if error_count > 5:
                data["running"] = False
                break
            await asyncio.sleep(60)

    if status_msg:
        try:
            await status_msg.edit_text(f"✅ Рассылка {phone} завершена. Циклов: {cycle_count}, чатов: {len(sent_chats)}")
        except:
            pass
    logger.info(f"Рассылка {phone} остановлена")

async def safe_spam_cycle(user_id, phone, data, message):
    status_msg = None
    if message:
        status_msg = await message.reply(f"🛡 Запуск безопасной рассылки для {phone}...")
    texts = data.get("texts_list", [])
    if not texts:
        texts = [data["text"]]
    base_interval = data.get("base_interval", 3600)
    sent_chats = []
    error_count = 0
    cycle_count = 0

    while data.get("running", False):
        try:
            if "client" not in data:
                error_count += 1
                if error_count > 3:
                    break
                await asyncio.sleep(60)
                continue

            try:
                me = await data["client"].get_me()
                if not me:
                    raise Exception("Не удалось получить информацию")
            except Exception as e:
                logger.warning(f"Клиент {phone} не отвечает: {e}")
                await reconnect_account(user_id, phone)
                await asyncio.sleep(30)
                continue

            dialogs = []
            async for dialog in data["client"].get_dialogs():
                if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    dialogs.append(dialog)

            chosen_text = random.choice(texts)
            for dialog in dialogs:
                if not data.get("running", False):
                    break
                try:
                    await data["client"].send_message(dialog.chat.id, chosen_text)
                    sent_chats.append(dialog.chat.title)
                    if len(sent_chats) % 5 == 0 and status_msg:
                        new_text = f"🛡 Безопасная рассылка {phone}\nЦикл #{cycle_count+1}\nОтправлено в {len(sent_chats)} чатов\nПоследние: " + ", ".join(sent_chats[-5:])
                        try:
                            await status_msg.edit_text(new_text)
                        except:
                            pass
                    await asyncio.sleep(0.5)
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                except (PeerIdInvalid, Forbidden):
                    continue
                except Exception as e:
                    logger.error(f"Ошибка отправки: {e}")
                    continue

            cycle_count += 1
            error_count = 0
            delay = random.randint(3300, 4200)
            logger.info(f"🛡 Цикл {cycle_count} для {phone} завершён. Следующий через {delay//60} мин")
            for _ in range(delay):
                if not data.get("running", False):
                    break
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Ошибка в безопасном цикле {phone}: {e}")
            error_count += 1
            if error_count > 5:
                data["running"] = False
                break
            await asyncio.sleep(60)

    if status_msg:
        try:
            await status_msg.edit_text(f"✅ Безопасная рассылка {phone} завершена. Циклов: {cycle_count}, чатов: {len(sent_chats)}")
        except:
            pass
    logger.info(f"Безопасная рассылка {phone} остановлена")

# ========== ИНИЦИАЛИЗАЦИЯ БОТА ==========
bot = Client(
    "manager_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir=bot_session_dir
)

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(user_id):
    # Базовые кнопки (всегда)
    kb = [
        [InlineKeyboardButton("🚀 Запустить", callback_data="start_ras"),
         InlineKeyboardButton("🛍 Магазин", callback_data="shop")],
        [InlineKeyboardButton("👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton("ℹ️ Информация о боте", callback_data="info")]
    ]
    # Кнопка "Стоп рассылки" появляется только если хотя бы один аккаунт запущен
    if user_id in users_data:
        has_running = any(acc.get("running", False) for acc in users_data[user_id]["accounts"].values())
        if has_running:
            kb.append([InlineKeyboardButton("🛑 Стоп рассылки", callback_data="stop_all")])
    # Админ панель для админа
    if is_admin(user_id):
        kb.append([InlineKeyboardButton("🛠 Админ панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

def get_start_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Сменить интервал", callback_data="change_interval")],
        [InlineKeyboardButton("✏️ Сменить текст", callback_data="change_text")],
        [InlineKeyboardButton("🚀 Запустить (обычный)", callback_data="start_normal")],
        [InlineKeyboardButton("🛡 Безопасный режим", callback_data="start_safe")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ])

def get_shop_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Неделя - $7.99", callback_data="sub_week")],
        [InlineKeyboardButton("📅 Месяц - $19.99", callback_data="sub_month")],
        [InlineKeyboardButton("📅 Год - $149.99", callback_data="sub_year")],
        [InlineKeyboardButton("🌟 Навсегда - $249.99", callback_data="sub_forever")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ])

def get_admin_panel_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Все пользователи", callback_data="list_users")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton("🔑 Управление ключами", callback_data="manage_keys")],
        [InlineKeyboardButton("🖼 Сменить приветственное фото", callback_data="change_welcome_photo")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ])

def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]])

# ========== ФУНКЦИЯ АВТОВЫДАЧИ КЛЮЧА ПОСЛЕ ОПЛАТЫ ==========
async def issue_key_to_user(user_id, subscription_type, days):
    """Автоматически генерирует и выдаёт ключ пользователю, добавляет подписку."""
    # Генерируем новый уникальный ключ
    new_key = generate_random_key()
    # Сохраняем ключ в общий список ключей
    current_keys = load_keys()
    # Определяем описание ключа
    if subscription_type == "week":
        desc = f"Неделя (автовыдача)"
    elif subscription_type == "month":
        desc = f"Месяц (автовыдача)"
    elif subscription_type == "year":
        desc = f"Год (автовыдача)"
    else:
        desc = f"Навсегда (автовыдача)"
    current_keys[new_key] = (desc, days, False)  # не админский
    save_keys(current_keys)

    # Проверяем, есть ли уже пользователь
    if user_id in users_data:
        # Обновляем подписку (продлеваем)
        old_expires = datetime.fromisoformat(users_data[user_id]["expires"])
        new_expires = max(old_expires, datetime.now()) + timedelta(days=days)
        users_data[user_id]["expires"] = new_expires.isoformat()
        users_data[user_id]["key_used"] = new_key
        save_users()
        return new_key, new_expires
    else:
        # Создаём нового пользователя
        expires = datetime.now() + timedelta(days=days)
        users_data[user_id] = {
            "expires": expires.isoformat(),
            "key_used": new_key,
            "is_admin": False,
            "username": "",  # заполнится при первом /start
            "bound_username": "",
            "accounts": {}
        }
        save_users()
        return new_key, expires

# ========== ОБРАБОТЧИКИ СООБЩЕНИЙ И CALLBACK'ОВ ==========
@bot.on_message(filters.command("start"))
async def start_cmd(c: Client, m: Message):
    user_id = m.from_user.id
    username = m.from_user.username or m.from_user.first_name

    # Если пользователь не зарегистрирован, показываем только магазин и профиль? Лучше предложить купить подписку.
    if user_id not in users_data:
        # Предлагаем купить подписку через магазин
        await m.reply(
            f"✨ *Добро пожаловать в NeverkaBOT, {username}!* ✨\n\n"
            f"🤖 У вас пока нет активной подписки.\n"
            f"🛍 Нажмите кнопку «Магазин», чтобы приобрести доступ.\n\n"
            f"👇 Используйте кнопки ниже.",
            reply_markup=get_main_keyboard(user_id),
            parse_mode=enums.ParseMode.MARKDOWN
        )
        return

    # Проверяем, идёт ли рассылка – если да, не показываем фото
    has_running = any(acc.get("running", False) for acc in users_data[user_id]["accounts"].values())
    photo_id = None if has_running else get_welcome_photo_id()

    text = (
        f"✨ *Добро пожаловать в NeverkaBOT, {username}!* ✨\n\n"
        f"🤖 Я помогу вам автоматизировать рассылку сообщений в группы.\n"
        f"📱 Добавляйте аккаунты, настраивайте текст и интервал.\n"
        f"⚡️ *Доступ к рассылке* — только по активной подписке.\n"
        f"🛡 *Безопасный режим* — рандомный текст и интервал 55-70 мин.\n\n"
        f"👇 Используйте кнопки ниже для управления."
    )
    if photo_id:
        await m.reply_photo(photo_id, caption=text, reply_markup=get_main_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
    else:
        await m.reply(text, reply_markup=get_main_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)

# Обработчик текстовых сообщений (ключ, добавление аккаунта, настройки) – остаётся как раньше
@bot.on_message(filters.text & filters.private)
async def handle_text(c: Client, m: Message):
    user_id = m.from_user.id
    text = m.text

    # Если пользователь не зарегистрирован, но вводит ключ вручную (старый метод)
    if user_id not in users_data:
        # Возможно, он хочет ввести ключ? У нас теперь только магазин, но на всякий случай оставим обработку ключа
        # Но лучше перенаправить в магазин
        await m.reply("🔐 У вас нет подписки. Пожалуйста, приобретите её в магазине.", reply_markup=get_main_keyboard(user_id))
        return

    # Остальная логика (добавление аккаунта, смена текста/интервала) – без изменений
    if user_id in temp_auth and temp_auth[user_id].get("step") == "phone":
        await process_phone_input(c, m)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "code":
        await process_code_input(c, m)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "password":
        await process_password_input(c, m)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "change_text":
        new_text = text.strip()
        for acc in users_data[user_id]["accounts"].values():
            acc["text"] = new_text
        save_users()
        await m.reply("✅ Текст рассылки обновлён для всех аккаунтов.", reply_markup=get_main_keyboard(user_id))
        temp_auth.pop(user_id)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "change_interval":
        try:
            interval = int(text)
            if interval < 10:
                await m.reply("⚠️ Интервал меньше 10 секунд может привести к бану. Введите число >= 10.")
                return
            for acc in users_data[user_id]["accounts"].values():
                acc["interval"] = interval
            save_users()
            await m.reply(f"✅ Интервал установлен: {interval} сек.", reply_markup=get_main_keyboard(user_id))
            temp_auth.pop(user_id)
        except ValueError:
            await m.reply("❌ Введите целое число секунд.")
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "safe_texts":
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if 3 <= len(lines) <= 5:
            temp_auth[user_id]["texts_list"] = lines
            temp_auth[user_id]["step"] = "safe_interval"
            await m.reply(
                f"✅ Принято {len(lines)} текстов.\n\n"
                "Теперь введите *базовый интервал* в секундах (рекомендуется 3600 = 1 час):\n"
                "Бот будет отправлять сообщения с задержкой от 55 до 70 минут.",
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await m.reply("❌ Нужно ввести от 3 до 5 текстов (каждый с новой строки). Попробуйте ещё раз.")
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "safe_interval":
        try:
            base_int = int(text)
            if base_int < 60:
                await m.reply("❌ Интервал должен быть не менее 60 секунд.")
                return
            temp_auth[user_id]["base_interval"] = base_int
            accounts = users_data[user_id]["accounts"]
            if not accounts:
                await m.reply("❌ Нет добавленных аккаунтов.")
                temp_auth.pop(user_id)
                return
            for phone, acc in accounts.items():
                acc["safe_mode"] = True
                acc["texts_list"] = temp_auth[user_id]["texts_list"]
                acc["base_interval"] = base_int
                acc["text"] = temp_auth[user_id]["texts_list"][0]
                if acc.get("running", False):
                    acc["running"] = False
                if "client" not in acc:
                    await reconnect_account(user_id, phone)
                    await asyncio.sleep(2)
                if "client" in acc:
                    acc["running"] = True
                    asyncio.create_task(safe_spam_cycle(user_id, phone, acc, m))
            save_users()
            await m.reply(f"🛡 Безопасная рассылка запущена для {len(accounts)} аккаунтов.", reply_markup=get_main_keyboard(user_id))
            temp_auth.pop(user_id)
        except ValueError:
            await m.reply("❌ Введите число.")
        return

    await m.reply("Используйте кнопки меню.", reply_markup=get_main_keyboard(user_id))

async def process_phone_input(c, m):
    user_id = m.from_user.id
    phone = m.text.strip()
    if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
        await m.reply(f"❌ Лимит аккаунтов ({MAX_ACCOUNTS_PER_USER})")
        temp_auth.pop(user_id)
        return
    session_name = os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}")
    try:
        client = Client(session_name, api_id=API_ID, api_hash=API_HASH, phone_number=phone, workdir=WORK_DIR)
        await client.connect()
        sent = await client.send_code(phone)
        temp_auth[user_id] = {
            "step": "code",
            "client": client,
            "phone": phone,
            "code_hash": sent.phone_code_hash,
            "user_id": user_id
        }
        await m.reply("🔢 Введите код из СМС:")
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        temp_auth.pop(user_id, None)

async def process_code_input(c, m):
    user_id = m.from_user.id
    data = temp_auth[user_id]
    try:
        await data["client"].sign_in(data["phone"], data["code_hash"], m.text)
        await finalize_account(user_id, data, m)
    except Exception as e:
        if "SESSION_PASSWORD_NEEDED" in str(e):
            data["step"] = "password"
            await m.reply("🔐 Введите облачный пароль (2FA):")
        else:
            await m.reply(f"❌ Ошибка: {e}")
            temp_auth.pop(user_id, None)

async def process_password_input(c, m):
    user_id = m.from_user.id
    data = temp_auth[user_id]
    try:
        await data["client"].check_password(m.text)
        await finalize_account(user_id, data, m)
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        temp_auth.pop(user_id, None)

async def finalize_account(uid, data, m):
    user_id = data["user_id"]
    phone = data["phone"]
    client = data["client"]
    session_name = os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}")

    async def on_disconnect(client, uid=user_id, ph=phone):
        await schedule_reconnect(uid, ph)
    client.add_handler(DisconnectHandler(on_disconnect))

    key = f"{user_id}_{phone}"
    if key in keep_alive_tasks:
        keep_alive_tasks[key].cancel()
    keep_alive_tasks[key] = asyncio.create_task(keep_alive(user_id, phone, client))

    users_data[user_id]["accounts"][phone] = {
        "client": client,
        "text": "Привет! Это рассылка.",
        "interval": 3600,
        "running": False,
        "added_date": datetime.now().isoformat(),
        "session_name": session_name,
        "safe_mode": False,
        "texts_list": [],
        "base_interval": 3600
    }
    save_users()
    await m.reply(f"✅ Аккаунт {phone} успешно добавлен!", reply_markup=get_main_keyboard(user_id))
    temp_auth.pop(uid, None)

@bot.on_callback_query()
async def handle_callback(c: Client, query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data

    # Если пользователь не зарегистрирован, но нажал на кнопку из магазина – разрешаем
    if user_id not in users_data and data not in ["shop", "back_to_main", "sub_week", "sub_month", "sub_year", "sub_forever", "payment_crypto", "payment_card", "cancel_payment"]:
        await query.answer("❌ У вас нет подписки. Приобретите её в магазине.", show_alert=True)
        return

    if data == "shop":
        await query.message.edit_text(
            "🛍 *Магазин подписок*\n\nВыберите срок подписки:",
            reply_markup=get_shop_keyboard(),
            parse_mode=enums.ParseMode.MARKDOWN
        )
    elif data == "sub_week":
        await show_subscription(query, "week", 7, 7.99)
    elif data == "sub_month":
        await show_subscription(query, "month", 30, 19.99)
    elif data == "sub_year":
        await show_subscription(query, "year", 365, 149.99)
    elif data == "sub_forever":
        await show_subscription(query, "forever", 3650, 249.99)
    elif data == "payment_crypto":
        await process_crypto_payment(query)
    elif data == "payment_card":
        await process_card_payment(query)
    elif data == "cancel_payment":
        await query.message.edit_text("❌ Платёж отменён.", reply_markup=get_shop_keyboard())
    elif data == "profile":
        await show_profile(query)
    elif data == "info":
        await show_info(query)
    elif data == "start_ras":
        await query.message.edit_text("Выберите режим рассылки:", reply_markup=get_start_menu_keyboard())
    elif data == "change_interval":
        temp_auth[user_id] = {"step": "change_interval"}
        await query.message.reply("⏱ Введите новый интервал между циклами (в секундах, минимум 10):")
        await query.answer()
    elif data == "change_text":
        temp_auth[user_id] = {"step": "change_text"}
        await query.message.reply("✏️ Введите новый текст для рассылки (будет применён ко всем аккаунтам):")
        await query.answer()
    elif data == "start_normal":
        await start_normal_ras(query)
    elif data == "start_safe":
        await start_safe_mode(query)
    elif data == "stop_all":
        await stop_all_ras(query)
    elif data == "admin_panel" and is_admin(user_id):
        await query.message.edit_text("🛠 *Админ панель*", reply_markup=get_admin_panel_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "list_users" and is_admin(user_id):
        await list_all_users(query)
    elif data == "stats" and is_admin(user_id):
        await show_stats(query)
    elif data == "manage_keys" and is_admin(user_id):
        await manage_keys(query)
    elif data == "change_welcome_photo" and is_admin(user_id):
        temp_auth[user_id] = {"step": "wait_photo"}
        await query.message.reply("📸 Отправьте новое приветственное фото (как обычное изображение).")
        await query.answer()
    elif data == "back_to_main":
        # Возвращаемся в главное меню, обновляя клавиатуру (чтобы скрыть/показать кнопку стоп)
        has_running = user_id in users_data and any(acc.get("running", False) for acc in users_data[user_id]["accounts"].values())
        photo_id = None if has_running else get_welcome_photo_id()
        text = "✨ *Главное меню*" if user_id in users_data else "✨ *Добро пожаловать!* Приобретите подписку в магазине."
        if photo_id and user_id in users_data:
            await query.message.delete()
            await query.message.reply_photo(photo_id, caption=text, reply_markup=get_main_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
        else:
            await query.message.edit_text(text, reply_markup=get_main_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "add_account":
        if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
            await query.answer(f"❌ Лимит {MAX_ACCOUNTS_PER_USER} аккаунтов", show_alert=True)
            return
        temp_auth[user_id] = {"step": "phone", "user_id": user_id}
        await query.message.reply("📱 Введите номер телефона в международном формате (например, +380123456789):")
        await query.answer()
    else:
        await query.answer("⛔ Недоступно", show_alert=True)

async def show_subscription(query, sub_type, days, price):
    text = f"💎 *Подписка {sub_type.capitalize()}*\n\n"
    text += f"💰 Стоимость: ${price}\n"
    text += f"📅 Срок: {days} дней\n\n"
    text += "Нажмите «Оплатить», чтобы продолжить."
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Оплатить", callback_data=f"pay_{sub_type}")],
        [InlineKeyboardButton("❌ Отменить", callback_data="cancel_payment")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)
    # Сохраняем временно данные о выбранной подписке
    temp_auth[query.from_user.id] = {"subscription": sub_type, "days": days, "price": price}

@bot.on_callback_query(filters.regex(r"^pay_(week|month|year|forever)$"))
async def pay_subscription(c, query):
    sub_type = query.data.split("_")[1]
    # Восстанавливаем данные из temp_auth
    if query.from_user.id not in temp_auth:
        await query.answer("Ошибка, попробуйте снова", show_alert=True)
        return
    sub_data = temp_auth[query.from_user.id]
    text = f"💳 *Выберите способ оплаты*\n\nПодписка: {sub_type.capitalize()}\nСумма: ${sub_data['price']}\n\n"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("₿ Криптовалюта (USDT)", callback_data="payment_crypto")],
        [InlineKeyboardButton("🇺🇦 Украинская карта", callback_data="payment_card")],
        [InlineKeyboardButton("◀️ Назад", callback_data="shop")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)

async def process_crypto_payment(query):
    user_id = query.from_user.id
    sub_data = temp_auth.get(user_id, {})
    sub_type = sub_data.get("subscription", "month")
    days = sub_data.get("days", 30)
    price = sub_data.get("price", 19.99)

    # Генерируем ключ и выдаём пользователю
    new_key, expires = await issue_key_to_user(user_id, sub_type, days)

    # Отправляем сообщение с ключом
    text = (
        f"✅ *Оплата успешно проведена!*\n\n"
        f"🎉 Ваш ключ доступа: `{new_key}`\n"
        f"📅 Подписка активна до: {expires.strftime('%d.%m.%Y')}\n\n"
        f"🔑 Вы также можете использовать этот ключ для входа на других устройствах.\n"
        f"Для начала работы нажмите /start."
    )
    await query.message.edit_text(text, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=get_main_keyboard(user_id))
    # Очищаем временные данные
    temp_auth.pop(user_id, None)

async def process_card_payment(query):
    user_id = query.from_user.id
    text = (
        "💳 *Оплата украинской картой*\n\n"
        "Для оплаты этим методом обратитесь к администратору:\n"
        "👤 @its_neverka\n\n"
        "После подтверждения оплаты вам будет выдан ключ доступа."
    )
    await query.message.edit_text(text, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=get_back_keyboard())

async def show_profile(query: CallbackQuery):
    user_id = query.from_user.id
    if user_id not in users_data:
        await query.answer("❌ Сначала приобретите подписку в магазине", show_alert=True)
        return
    data = users_data[user_id]
    accounts = data["accounts"]
    total = len(accounts)
    running = sum(1 for a in accounts.values() if a.get("running", False))

    text = f"👤 *Мой профиль*\n\n"
    text += f"🆔 ID: `{user_id}`\n"
    text += f"👤 Имя: {data.get('username', 'Не указано')}\n"
    if data.get('bound_username'):
        text += f"🔗 Привязан к: @{data['bound_username']}\n"
    text += f"📱 Аккаунтов: {total}/{MAX_ACCOUNTS_PER_USER}\n"
    text += f"🟢 Активных рассылок: {running}\n"
    text += f"📅 Подписка до: {datetime.fromisoformat(data['expires']).strftime('%d.%m.%Y')}\n\n"

    if accounts:
        text += "📋 *Список аккаунтов*:\n"
        for i, (phone, acc) in enumerate(accounts.items(), 1):
            status = "🟢 Активен" if acc.get("running", False) else "🔴 Остановлен"
            client_ok = "✅" if "client" in acc else "❌"
            safe_mark = "🛡" if acc.get("safe_mode", False) else ""
            text += f"{i}. {phone} {client_ok} {status} {safe_mark}\n"
            text += f"   Текст: {acc['text'][:40]}...\n"
            text += f"   Интервал: {acc['interval']} сек.\n"
    else:
        text += "📭 Нет добавленных аккаунтов.\n"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)

async def show_info(query: CallbackQuery):
    text = (
        "ℹ️ *О боте*\n\n"
        "🤖 **NeverkaBOT** — мощный инструмент для автоматической рассылки сообщений в Telegram-группы.\n\n"
        "⚙️ **Функции:**\n"
        "• Добавление нескольких аккаунтов\n"
        "• Настройка текста и интервала рассылки\n"
        "• Безопасный режим с рандомными текстами и интервалом 55-70 мин\n"
        "• Управление подпиской через магазин\n\n"
        "📞 **Поддержка:** @its_neverka\n\n"
        "© 2026 NeverkaBOT"
    )
    await query.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)

async def start_normal_ras(query: CallbackQuery):
    user_id = query.from_user.id
    if not has_active_subscription(user_id):
        await query.answer("❌ Ваша подписка истекла! Продлите в магазине.", show_alert=True)
        return
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        await query.answer("❌ Нет добавленных аккаунтов", show_alert=True)
        return
    started = 0
    for phone, acc in accounts.items():
        if not acc.get("running", False):
            if "client" not in acc:
                await reconnect_account(user_id, phone)
                await asyncio.sleep(2)
            if "client" in acc:
                acc["running"] = True
                acc["safe_mode"] = False
                asyncio.create_task(spam_cycle(user_id, phone, acc, query.message))
                started += 1
    save_users()
    await query.message.reply(f"🚀 Запущено обычных рассылок: {started}", reply_markup=get_main_keyboard(user_id))
    await query.answer()

async def start_safe_mode(query: CallbackQuery):
    user_id = query.from_user.id
    if not has_active_subscription(user_id):
        await query.answer("❌ Подписка истекла!", show_alert=True)
        return
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        await query.answer("❌ Нет аккаунтов", show_alert=True)
        return
    temp_auth[user_id] = {"step": "safe_texts"}
    await query.message.reply(
        "🛡 *Безопасный режим*\n\n"
        "Отправьте от 3 до 5 текстов (каждый с новой строки).\n"
        "Бот будет случайным образом выбирать один из них.\n\n"
        "Пример:\n"
        "Привет, друг!\n"
        "Как дела?\n"
        "Отличный день!\n"
        "Будь здоров!\n"
        "Хорошего настроения!",
        parse_mode=enums.ParseMode.MARKDOWN
    )
    await query.answer()

async def stop_all_ras(query: CallbackQuery):
    user_id = query.from_user.id
    accounts = users_data[user_id]["accounts"]
    stopped = 0
    for acc in accounts.values():
        if acc.get("running", False):
            acc["running"] = False
            stopped += 1
    save_users()
    await query.message.reply(f"🛑 Остановлено рассылок: {stopped}", reply_markup=get_main_keyboard(user_id))
    await query.answer()

async def list_all_users(query: CallbackQuery):
    if not users_data:
        await query.message.reply("📭 Нет активных пользователей")
        return
    text = "👥 *Все пользователи*\n\n"
    for uid, data in users_data.items():
        expires = datetime.fromisoformat(data["expires"])
        acc_count = len(data["accounts"])
        bound = f" (привязан @{data['bound_username']})" if data.get('bound_username') else ""
        text += f"🆔 `{uid}` {bound}\n"
        text += f"👤 {data.get('username', 'нет юзернейма')}\n"
        text += f"📱 Акков: {acc_count} | Доступ до: {expires.strftime('%d.%m.%Y')}\n"
        text += f"🔑 Ключ: `{data['key_used']}`\n\n"
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await query.message.reply(text[i:i+4000])
    else:
        await query.message.edit_text(text, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=get_back_keyboard())
    await query.answer()

async def show_stats(query: CallbackQuery):
    total_users = len(users_data)
    total_accounts = sum(len(d["accounts"]) for d in users_data.values())
    total_running = sum(1 for d in users_data.values() for a in d["accounts"].values() if a.get("running"))
    keys = load_keys()
    total_keys = len(keys)
    used_keys = sum(1 for d in users_data.values() if d["key_used"] in keys)
    text = (
        f"📊 *Статистика бота*\n\n"
        f"👥 Пользователей: {total_users}\n"
        f"📱 Всего аккаунтов: {total_accounts}\n"
        f"🟢 Активных рассылок: {total_running}\n"
        f"🔑 Всего ключей: {total_keys}\n"
        f"✅ Использовано: {used_keys}\n"
        f"📦 Свободно: {total_keys - used_keys}"
    )
    await query.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)
    await query.answer()

async def manage_keys(query: CallbackQuery):
    keys = load_keys()
    text = "🔑 *Управление ключами*\n\n"
    for key, info in keys.items():
        if isinstance(info, tuple):
            if len(info) == 2:
                desc, days = info
                is_admin = False
            else:
                desc, days, is_admin = info
            validity = f"{days} дн." if days < 1000 else "Навсегда"
            role = "👑 Админ" if is_admin else "👤 Пользователь"
        else:
            desc = info
            validity = "30 дн."
            role = "Пользователь"
        used = any(d["key_used"] == key for d in users_data.values())
        status = "❌ использован" if used else "✅ свободен"
        text += f"• `{key}` — {desc} ({validity}) {role} — {status}\n"
    text += "\n*Сгенерированные ключи:*\n"
    text += "Неделя: `Msdf_7d9f3k_sdfs_92jd`\n"
    text += "Месяц: `Msdf_3k9d0f_sdfs_4hrt`\n"
    text += "Год: `Msdf_8g4h1t_sdfs_6jsk`\n"
    text += "Навсегда: `Msdf_0f2a5e_sdfs_8djs`\n"
    text += "Админ: `ADMIN_MASTER_KEY`"
    await query.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)
    await query.answer()

@bot.on_message(filters.photo & filters.private)
async def handle_photo(c, m):
    user_id = m.from_user.id
    if user_id in temp_auth and temp_auth[user_id].get("step") == "wait_photo" and is_admin(user_id):
        file_id = m.photo.file_id
        set_welcome_photo_id(file_id)
        await m.reply("✅ Приветственное фото обновлено!", reply_markup=get_main_keyboard(user_id))
        temp_auth.pop(user_id)
    else:
        await m.reply("Используйте кнопки меню.")

# ========== ГРАЦИОЗНОЕ ЗАВЕРШЕНИЕ ==========
async def shutdown(sig=None):
    logger.info("🛑 Останавливаю бота...")
    for task in keep_alive_tasks.values():
        task.cancel()
    for task in reconnect_tasks.values():
        task.cancel()
    save_users()
    for user_data in users_data.values():
        for acc in user_data["accounts"].values():
            if "client" in acc:
                try:
                    await acc["client"].stop()
                except:
                    pass
    await bot.stop()
    sys.exit(0)

# ========== ЗАПУСК ==========
if __name__ == "__main__":
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig)))

    load_users()

    async def startup():
        logger.info("🚀 Запуск бота...")
        if IS_RAILWAY:
            test_file = os.path.join(WORK_DIR, 'test_write.txt')
            try:
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                logger.info("✅ Volume доступен для записи")
            except Exception as e:
                logger.error(f"❌ Volume НЕ доступен: {e}")
        await load_user_sessions()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(startup())
    logger.info("🤖 Бот запущен")
    try:
        bot.run()
    except (KeyboardInterrupt, SystemExit):
        loop.run_until_complete(shutdown())
