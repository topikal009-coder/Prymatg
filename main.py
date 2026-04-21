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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- КОНФИГ ---
API_ID = 30032542
API_HASH = "ce646da1307fb452305d49f9bb8751ca"
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8711240311:AAHy5FzxQ7P0MpSm3Bv7xfoYDa9kVlwAb5w')

# --- РАБОЧАЯ ДИРЕКТОРИЯ ---
IS_RAILWAY = os.path.exists('/app') or 'RAILWAY_SERVICE_NAME' in os.environ
WORK_DIR = '/app/data' if IS_RAILWAY else os.path.dirname(os.path.abspath(__file__))
os.makedirs(WORK_DIR, exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, 'sessions'), exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, 'user_settings'), exist_ok=True)
bot_session_dir = os.path.join(WORK_DIR, 'bot_session')
os.makedirs(bot_session_dir, exist_ok=True)

# --- ФАЙЛЫ ---
KEYS_FILE = os.path.join(WORK_DIR, 'activation_keys.json')
USERS_FILE = os.path.join(WORK_DIR, "bot_users.json")
WELCOME_PHOTO_FILE = os.path.join(WORK_DIR, 'welcome_photo_id.txt')

# --- КЛЮЧИ ПО УМОЛЧАНИЮ ---
DEFAULT_KEYS = {
    "Msdf_7d9f3k_sdfs_92jd": ("Недельная подписка", 7, False),
    "Msdf_3k9d0f_sdfs_4hrt": ("Месячная подписка", 30, False),
    "Msdf_8g4h1t_sdfs_6jsk": ("Годовая подписка", 365, False),
    "Msdf_0a9b2c_sdfs_7xyz": ("Подписка навсегда", 3650, False),
    "Msdf_admin_9x8y7z": ("Администратор", 3650, True)
}

def load_keys():
    try:
        if os.path.exists(KEYS_FILE):
            with open(KEYS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            with open(KEYS_FILE, 'w', encoding='utf-8') as f:
                json.dump(DEFAULT_KEYS, f, ensure_ascii=False, indent=2)
            return DEFAULT_KEYS
    except Exception:
        return DEFAULT_KEYS

def save_keys(keys):
    try:
        with open(KEYS_FILE, 'w', encoding='utf-8') as f:
            json.dump(keys, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

# --- ДАННЫЕ ПОЛЬЗОВАТЕЛЕЙ ---
users_data = {}
temp_auth = {}
reconnect_tasks = {}
keep_alive_tasks = {}
MAX_ACCOUNTS_PER_USER = 3

def save_users():
    try:
        to_save = {}
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
            to_save[str(uid)] = {
                "expires": data["expires"].isoformat() if isinstance(data["expires"], datetime) else data["expires"],
                "key_used": data["key_used"],
                "is_admin": data["is_admin"],
                "username": data.get("username", ""),
                "bound_username": data.get("bound_username", ""),
                "accounts": accounts
            }
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(to_save, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения: {e}")
        return False

def load_users():
    global users_data
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            for uid, data in loaded.items():
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

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def is_admin(user_id):
    return users_data.get(user_id, {}).get("is_admin", False)

def has_active_subscription(user_id):
    if user_id not in users_data:
        return False
    expires = users_data[user_id]["expires"]
    if isinstance(expires, str):
        expires = datetime.fromisoformat(expires)
    return expires > datetime.now()

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

def parse_key_with_username(key_text):
    pattern = r'^(.*?)-@([a-zA-Z0-9_]+)$'
    match = re.match(pattern, key_text.strip())
    if match:
        return match.group(1), match.group(2)
    else:
        return key_text.strip(), None

# --- ДИНАМИЧЕСКАЯ ГЛАВНАЯ КЛАВИАТУРА ---
def get_main_keyboard(user_id):
    has_running = False
    if user_id in users_data:
        for acc in users_data[user_id]["accounts"].values():
            if acc.get("running", False):
                has_running = True
                break
    if has_running:
        row1 = [InlineKeyboardButton("🛑 Стоп рассылки", callback_data="stop_all")]
    else:
        row1 = [InlineKeyboardButton("🚀 Запустить", callback_data="start_menu")]
    row1.append(InlineKeyboardButton("🛍 Магазин", callback_data="shop"))
    row2 = [InlineKeyboardButton("👤 Профиль", callback_data="profile")]
    row3 = [InlineKeyboardButton("ℹ️ Информация о боте", callback_data="info")]
    kb = [row1, row2, row3]
    if is_admin(user_id):
        kb.append([InlineKeyboardButton("🛠 Админ панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

def get_start_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Обычный режим", callback_data="start_normal")],
        [InlineKeyboardButton("🛡 Безопасный режим", callback_data="start_safe")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ])

def get_shop_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Неделя – $7.99", callback_data="sub_week")],
        [InlineKeyboardButton("📆 Месяц – $19.99", callback_data="sub_month")],
        [InlineKeyboardButton("🗓 Год – $149.99", callback_data="sub_year")],
        [InlineKeyboardButton("♾ Навсегда – $249.99", callback_data="sub_forever")],
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

# --- ФУНКЦИИ РАССЫЛКИ (обычный и безопасный режимы) ---
async def spam_cycle(user_id, phone, data, message):
    status_msg = None
    if message:
        status_msg = await message.reply(f"🚀 Обычная рассылка для {phone} запущена...")
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
                await data["client"].get_me()
            except Exception:
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
            for _ in range(data["interval"]):
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
            await status_msg.edit_text(f"✅ Обычная рассылка {phone} завершена. Циклов: {cycle_count}, чатов: {len(sent_chats)}")
        except:
            pass
    logger.info(f"Рассылка {phone} остановлена")

async def safe_spam_cycle(user_id, phone, data, message):
    status_msg = None
    if message:
        status_msg = await message.reply(f"🛡 Безопасная рассылка для {phone} запущена...")
    texts = data.get("texts_list", [])
    if not texts:
        texts = [data["text"]]
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
                await data["client"].get_me()
            except Exception:
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
            delay = random.randint(3300, 4200)  # 55-70 мин
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

# --- ПЕРЕПОДКЛЮЧЕНИЕ И KEEP-ALIVE ---
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
        except Exception:
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

async def load_user_sessions():
    loaded = 0
    for uid, user_data in users_data.items():
        for phone, acc in user_data["accounts"].items():
            try:
                session_name = acc.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{uid}"))
                if os.path.exists(f"{session_name}.session"):
                    client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=WORK_DIR)
                    async def on_disconnect(client, uid=uid, ph=phone):
                        await schedule_reconnect(uid, ph)
                    client.add_handler(DisconnectHandler(on_disconnect))
                    await client.start()
                    acc["client"] = client
                    key = f"{uid}_{phone}"
                    keep_alive_tasks[key] = asyncio.create_task(keep_alive(uid, phone, client))
                    loaded += 1
            except Exception:
                pass
    return loaded

# --- ОБРАБОТЧИКИ ---
bot = Client("manager_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir=bot_session_dir)

@bot.on_message(filters.command("start"))
async def start_cmd(c: Client, m: Message):
    user_id = m.from_user.id
    username = m.from_user.username or m.from_user.first_name
    text = (
        f"✨ *Добро пожаловать в NeverkaBOT, {username}!* ✨\n\n"
        f"🤖 Я помогу вам автоматизировать рассылку сообщений в группы.\n"
        f"📱 Добавляйте аккаунты, настраивайте текст и интервал.\n"
        f"⚡️ *Доступ к рассылке* — только по активной подписке.\n"
        f"🛡 *Безопасный режим* — рандомный текст и интервал 55-70 мин.\n\n"
        f"👇 Используйте кнопки ниже для управления."
    )
    photo_id = get_welcome_photo_id()
    if photo_id:
        await m.reply_photo(photo_id, caption=text, reply_markup=get_main_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
    else:
        await m.reply(text, reply_markup=get_main_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)

@bot.on_message(filters.private & filters.text)
async def handle_private_text(c: Client, m: Message):
    user_id = m.from_user.id
    # Ввод ключа
    if user_id in temp_auth and temp_auth[user_id].get("step") == "enter_key":
        await process_key_input(c, m)
        return
    # Добавление аккаунта
    if user_id in temp_auth and temp_auth[user_id].get("step") == "phone":
        await process_phone_input(c, m)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "code":
        await process_code_input(c, m)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "password":
        await process_password_input(c, m)
        return
    # Смена текста
    if user_id in temp_auth and temp_auth[user_id].get("step") == "change_text":
        new_text = m.text.strip()
        for acc in users_data[user_id]["accounts"].values():
            acc["text"] = new_text
        save_users()
        await m.reply("✅ Текст рассылки обновлён.", reply_markup=get_main_keyboard(user_id))
        temp_auth.pop(user_id)
        return
    # Смена интервала
    if user_id in temp_auth and temp_auth[user_id].get("step") == "change_interval":
        try:
            interval = int(m.text)
            if interval < 10:
                await m.reply("⚠️ Минимум 10 секунд.")
                return
            for acc in users_data[user_id]["accounts"].values():
                acc["interval"] = interval
            save_users()
            await m.reply(f"✅ Интервал установлен: {interval} сек.", reply_markup=get_main_keyboard(user_id))
            temp_auth.pop(user_id)
        except ValueError:
            await m.reply("❌ Введите число.")
        return
    # Безопасный режим – ввод текстов
    if user_id in temp_auth and temp_auth[user_id].get("step") == "safe_texts":
        lines = [line.strip() for line in m.text.split('\n') if line.strip()]
        if 3 <= len(lines) <= 5:
            temp_auth[user_id]["texts_list"] = lines
            temp_auth[user_id]["step"] = "safe_interval"
            await m.reply(
                f"✅ Принято {len(lines)} текстов.\n\nТеперь введите *базовый интервал* в секундах (рекомендуется 3600):",
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await m.reply("❌ Нужно от 3 до 5 текстов (каждый с новой строки).")
        return
    # Безопасный режим – ввод интервала
    if user_id in temp_auth and temp_auth[user_id].get("step") == "safe_interval":
        try:
            base_int = int(m.text)
            if base_int < 60:
                await m.reply("❌ Интервал не менее 60 секунд.")
                return
            temp_auth[user_id]["base_interval"] = base_int
            accounts = users_data[user_id]["accounts"]
            if not accounts:
                await m.reply("❌ Нет аккаунтов.")
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
    # Если ничего – главное меню
    await m.reply("Используйте кнопки меню.", reply_markup=get_main_keyboard(user_id))

async def process_key_input(c, m):
    user_id = m.from_user.id
    raw_key = m.text.strip()
    username = m.from_user.username or ""
    key, bound_username = parse_key_with_username(raw_key)
    current_keys = load_keys()
    if key not in current_keys:
        await m.reply("❌ Неверный ключ!")
        return
    key_info = current_keys[key]
    if isinstance(key_info, tuple):
        if len(key_info) == 2:
            desc, days = key_info
            is_admin_key = False
        else:
            desc, days, is_admin_key = key_info
    else:
        desc = key_info
        days = 30
        is_admin_key = False
    if bound_username and bound_username.lower() != username.lower():
        await m.reply(f"❌ Ключ привязан к @{bound_username}")
        return
    for uid, udata in users_data.items():
        if udata["key_used"] == key:
            await m.reply("❌ Ключ уже использован!")
            return
    expires = datetime.now() + timedelta(days=days)
    # Сохраняем существующие аккаунты, если пользователь уже добавлял их (без подписки)
    old_accounts = users_data.get(user_id, {}).get("accounts", {})
    users_data[user_id] = {
        "expires": expires.isoformat(),
        "key_used": key,
        "is_admin": is_admin_key,
        "username": username or m.from_user.first_name,
        "bound_username": bound_username or "",
        "accounts": old_accounts
    }
    save_users()
    role = "👑 Администратор" if is_admin_key else "👤 Пользователь"
    await m.reply(
        f"✅ *Подписка активирована!*\n\n{role}\nСрок до: {expires.strftime('%d.%m.%Y')}\n\nТеперь вам доступен запуск рассылки.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard(user_id)
    )
    temp_auth.pop(user_id, None)

async def process_phone_input(c, m):
    user_id = m.from_user.id
    phone = m.text.strip()
    if user_id not in users_data:
        users_data[user_id] = {
            "expires": datetime.now().isoformat(),
            "key_used": "",
            "is_admin": False,
            "username": m.from_user.username or m.from_user.first_name,
            "bound_username": "",
            "accounts": {}
        }
    if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
        await m.reply(f"❌ Лимит {MAX_ACCOUNTS_PER_USER} аккаунтов")
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
    await m.reply(f"✅ Аккаунт {phone} добавлен!", reply_markup=get_main_keyboard(user_id))
    temp_auth.pop(uid, None)

# --- CALLBACK'И ---
@bot.on_callback_query()
async def handle_callback(c: Client, query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data
    if data == "back_to_main":
        await query.message.edit_reply_markup(reply_markup=get_main_keyboard(user_id))
        await query.answer()
        return
    if data == "profile":
        await show_profile(query)
    elif data == "start_menu":
        if not has_active_subscription(user_id):
            # Удаляем предыдущее приветственное сообщение и отправляем новое о подписке
            await query.message.delete()
            text = "🔐 *У вас нет активной подписки!*\n\nПожалуйста, приобретите её в магазине."
            await query.message.reply(text, reply_markup=get_shop_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)
            await query.answer()
            return
        await query.message.edit_reply_markup(reply_markup=get_start_menu_keyboard())
    elif data == "shop":
        await query.message.edit_reply_markup(reply_markup=get_shop_keyboard())
    elif data == "info":
        info_text = (
            "ℹ️ *Информация о боте*\n\n"
            "🤖 **NeverkaBOT** – автоматическая рассылка в Telegram-группы.\n\n"
            "🔹 *Возможности:*\n"
            "• Множество аккаунтов\n"
            "• Настройка текста и интервала\n"
            "• Обычный и безопасный режимы\n"
            "• Защита от блокировок\n\n"
            "📌 *Разработчик:* @its_neverka"
        )
        await query.message.edit_text(info_text, reply_markup=get_back_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "admin_panel" and is_admin(user_id):
        await query.message.edit_reply_markup(reply_markup=get_admin_panel_keyboard())
    elif data == "list_users" and is_admin(user_id):
        await list_all_users(query)
    elif data == "stats" and is_admin(user_id):
        await show_stats(query)
    elif data == "manage_keys" and is_admin(user_id):
        await manage_keys(query)
    elif data == "change_welcome_photo" and is_admin(user_id):
        temp_auth[user_id] = {"step": "wait_photo"}
        await query.message.reply("📸 Отправьте новое приветственное фото.")
        await query.answer()
    elif data.startswith("sub_"):
        await handle_subscription(query, data)
    elif data == "start_normal":
        await start_normal_ras(query)
    elif data == "start_safe":
        await start_safe_mode(query)
    elif data == "stop_all":
        await stop_all_ras(query)
    elif data == "add_account":
        if user_id not in users_data:
            users_data[user_id] = {
                "expires": datetime.now().isoformat(),
                "key_used": "",
                "is_admin": False,
                "username": query.from_user.username or query.from_user.first_name,
                "bound_username": "",
                "accounts": {}
            }
        if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
            await query.answer(f"❌ Лимит {MAX_ACCOUNTS_PER_USER} аккаунтов", show_alert=True)
            return
        temp_auth[user_id] = {"step": "phone", "user_id": user_id}
        await query.message.reply("📱 Введите номер телефона (например, +380123456789):")
        await query.answer()
    elif data == "activate_key":
        temp_auth[user_id] = {"step": "enter_key"}
        await query.message.reply("🔑 Введите активационный ключ:")
        await query.answer()
    else:
        await query.answer("⛔ Недоступно", show_alert=True)

async def handle_subscription(query: CallbackQuery, sub_type):
    subs = {
        "sub_week": ("Неделя", 7, 7.99),
        "sub_month": ("Месяц", 30, 19.99),
        "sub_year": ("Год", 365, 149.99),
        "sub_forever": ("Навсегда", 3650, 249.99)
    }
    name, days, price = subs[sub_type]
    text = f"🛍 *Подписка «{name}»*\n\n💰 Цена: ${price}\n📅 Срок: {days} дней\n\nВыберите способ оплаты:"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("₿ Криптовалюта (USDT)", callback_data=f"pay_crypto_{sub_type}")],
        [InlineKeyboardButton("🇺🇦 Украинская карта", callback_data=f"pay_card_{sub_type}")],
        [InlineKeyboardButton("◀️ Назад", callback_data="shop")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)

@bot.on_callback_query(filters.regex(r"pay_crypto_(.*)"))
async def pay_crypto(c: Client, query: CallbackQuery):
    sub_type = query.matches[0].group(1)
    subs = {"sub_week": ("Неделя", 7.99), "sub_month": ("Месяц", 19.99), "sub_year": ("Год", 149.99), "sub_forever": ("Навсегда", 249.99)}
    name, price = subs[sub_type]
    crypto_link = f"https://t.me/cryptobot?start=payment_USDT_{int(price*100)}"
    text = f"💸 *Оплата подписки «{name}»*\n\nСумма: ${price} USDT\n\nПерейдите в криптобот по ссылке ниже."
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💎 Перейти к оплате", url=crypto_link)],
        [InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"check_payment_{sub_type}")],
        [InlineKeyboardButton("◀️ Назад", callback_data="shop")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)
    await query.answer()

@bot.on_callback_query(filters.regex(r"pay_card_(.*)"))
async def pay_card(c: Client, query: CallbackQuery):
    text = "💳 *Оплата украинской картой*\n\nДля оплаты свяжитесь с администратором: @its_neverka"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Написать", url="https://t.me/its_neverka")],
        [InlineKeyboardButton("◀️ Назад", callback_data="shop")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)
    await query.answer()

@bot.on_callback_query(filters.regex(r"check_payment_(.*)"))
async def check_payment(c: Client, query: CallbackQuery):
    await query.answer("⏳ Оплата ещё не подтверждена. Подождите 1-2 минуты.", show_alert=True)

async def show_profile(query: CallbackQuery):
    user_id = query.from_user.id
    if user_id not in users_data:
        users_data[user_id] = {
            "expires": datetime.now().isoformat(),
            "key_used": "",
            "is_admin": False,
            "username": query.from_user.username or query.from_user.first_name,
            "bound_username": "",
            "accounts": {}
        }
        save_users()
    data = users_data[user_id]
    accounts = data["accounts"]
    total = len(accounts)
    running = sum(1 for a in accounts.values() if a.get("running", False))
    has_sub = has_active_subscription(user_id)
    expiry_str = datetime.fromisoformat(data["expires"]).strftime('%d.%m.%Y') if has_sub else "Нет подписки"
    text = f"👤 *Мой профиль*\n\n🆔 ID: `{user_id}`\n👤 Имя: {data.get('username', 'Не указано')}\n📱 Аккаунтов: {total}/{MAX_ACCOUNTS_PER_USER}\n🟢 Активных рассылок: {running}\n📅 Подписка до: {expiry_str}\n\n"
    if accounts:
        text += "📋 *Список аккаунтов*:\n"
        for i, (phone, acc) in enumerate(accounts.items(), 1):
            status = "Активен" if acc.get("running", False) else "Остановлен"
            client_ok = "✅" if "client" in acc else "❌"
            safe_mark = "🛡" if acc.get("safe_mode", False) else ""
            text += f"{i}. {phone} {client_ok} {status} {safe_mark}\n   Текст: {acc['text'][:40]}...\n   Интервал: {acc['interval']} сек.\n"
    else:
        text += "📭 Нет добавленных аккаунтов.\n"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("🔑 Активировать ключ", callback_data="activate_key")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)

async def start_normal_ras(query: CallbackQuery):
    user_id = query.from_user.id
    if not has_active_subscription(user_id):
        await query.answer("❌ Нет активной подписки!", show_alert=True)
        return
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        await query.answer("❌ Нет аккаунтов", show_alert=True)
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
    await query.message.reply(f"🚀 Запущено обычных рассылок: {started}")
    await query.message.edit_reply_markup(reply_markup=get_main_keyboard(user_id))
    await query.answer()

async def start_safe_mode(query: CallbackQuery):
    user_id = query.from_user.id
    if not has_active_subscription(user_id):
        await query.answer("❌ Нет активной подписки!", show_alert=True)
        return
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        await query.answer("❌ Нет аккаунтов", show_alert=True)
        return
    temp_auth[user_id] = {"step": "safe_texts"}
    await query.message.reply(
        "🛡 *Безопасный режим*\n\nОтправьте от 3 до 5 текстов (каждый с новой строки).\nПример:\nПривет!\nКак дела?\nОтличный день!",
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
    await query.message.reply(f"🛑 Остановлено рассылок: {stopped}")
    await query.message.edit_reply_markup(reply_markup=get_main_keyboard(user_id))
    await query.answer()

async def list_all_users(query: CallbackQuery):
    if not users_data:
        await query.message.reply("📭 Нет пользователей", reply_markup=get_back_keyboard())
        return
    text = "👥 *Все пользователи*\n\n"
    for uid, data in users_data.items():
        expires = datetime.fromisoformat(data["expires"])
        acc_count = len(data["accounts"])
        bound = f" (привязан @{data['bound_username']})" if data.get('bound_username') else ""
        text += f"🆔 `{uid}` {bound}\n👤 {data.get('username', 'нет юзернейма')}\n📱 Акков: {acc_count} | Доступ до: {expires.strftime('%d.%m.%Y')}\n🔑 Ключ: `{data['key_used']}`\n\n"
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await query.message.reply(text[i:i+4000])
    else:
        await query.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)
    await query.answer()

async def show_stats(query: CallbackQuery):
    total_users = len(users_data)
    total_accounts = sum(len(d["accounts"]) for d in users_data.values())
    total_running = sum(1 for d in users_data.values() for a in d["accounts"].values() if a.get("running"))
    keys = load_keys()
    total_keys = len(keys)
    used_keys = sum(1 for d in users_data.values() if d["key_used"] in keys)
    text = f"📊 *Статистика*\n\n👥 Пользователей: {total_users}\n📱 Аккаунтов: {total_accounts}\n🟢 Активных рассылок: {total_running}\n🔑 Всего ключей: {total_keys}\n✅ Использовано: {used_keys}\n📦 Свободно: {total_keys - used_keys}"
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
    text += "\n*Доступные ключи:*\nНеделя: `Msdf_7d9f3k_sdfs_92jd`\nМесяц: `Msdf_3k9d0f_sdfs_4hrt`\nГод: `Msdf_8g4h1t_sdfs_6jsk`\nНавсегда: `Msdf_0a9b2c_sdfs_7xyz`\nАдмин: `Msdf_admin_9x8y7z`"
    await query.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)
    await query.answer()

@bot.on_message(filters.photo & filters.private)
async def handle_photo(c, m):
    user_id = m.from_user.id
    if user_id in temp_auth and temp_auth[user_id].get("step") == "wait_photo" and is_admin(user_id):
        set_welcome_photo_id(m.photo.file_id)
        await m.reply("✅ Приветственное фото обновлено!", reply_markup=get_main_keyboard(user_id))
        temp_auth.pop(user_id)
    else:
        await m.reply("Используйте кнопки меню.")

# --- ЗАВЕРШЕНИЕ ---
async def shutdown(sig=None):
    logger.info("Остановка бота...")
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

if __name__ == "__main__":
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig)))
    load_users()
    async def startup():
        logger.info("Запуск...")
        if IS_RAILWAY:
            try:
                os.makedirs(WORK_DIR, exist_ok=True)
                with open(os.path.join(WORK_DIR, 'test.txt'), 'w') as f:
                    f.write('test')
                os.remove(os.path.join(WORK_DIR, 'test.txt'))
                logger.info("✅ Volume работает")
            except Exception as e:
                logger.error(f"❌ Volume ошибка: {e}")
        await load_user_sessions()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(startup())
    logger.info("Бот запущен")
    try:
        bot.run()
    except (KeyboardInterrupt, SystemExit):
        loop.run_until_complete(shutdown())
