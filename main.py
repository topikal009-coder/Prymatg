import asyncio
import os
import json
import re
import sys
import signal
import random
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message
)
from pyrogram.errors import (
    PeerIdInvalid, Forbidden, FloodWait
)
from pyrogram.handlers import DisconnectHandler
import logging
import aiohttp  # для CryptoPay

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- КОНФИГ ---
API_ID = 30032542
API_HASH = "ce646da1307fb452305d49f9bb8751ca"
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8659319275:AAEaMn1u9a-iCxmGQQEpL2qOz3W7BKB0mnw')

# === ID АДМИНИСТРАТОРОВ (укажите свои ID) ===
ADMIN_IDS = [964442694]  # ← ЗАМЕНИТЕ НА ВАШ ТЕЛЕГРАМ ID

# === РЕКВИЗИТЫ ДЛЯ ОПЛАТЫ (карта/ручной перевод) ===
USDT_WALLET = "563269:AA2NzVEuw3OYahJXkJHCBXMbgFnO9xw4wan"  # ← замените на свой USDT (TRC20) кошелёк

# === НАСТРОЙКИ CRYPTOPAY ===
CRYPTO_PAY_TOKEN = os.environ.get('CRYPTO_PAY_TOKEN', '')
CRYPTO_PAY_TESTNET = os.environ.get('CRYPTO_PAY_TESTNET', 'False').lower() == 'true'

# === РАБОЧАЯ ДИРЕКТОРИЯ ===
IS_RAILWAY = os.path.exists('/app') or 'RAILWAY_SERVICE_NAME' in os.environ

if IS_RAILWAY:
    WORK_DIR = '/app/data'
    if not os.path.exists(WORK_DIR):
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

# === КЛАСС ДЛЯ РАБОТЫ С CRYPTOPAY ===
class CryptoPayClient:
    def __init__(self, api_token: str, testnet=False):
        self.token = api_token
        self.url = "https://testnet-pay.crypt.bot/api" if testnet else "https://pay.crypt.bot/api"
    
    async def _req(self, method: str, params=None):
        async with aiohttp.ClientSession() as sess:
            async with sess.post(f"{self.url}/{method}", 
                                 headers={"Crypto-Pay-API-Token": self.token, "Content-Type": "application/json"},
                                 json=params or {}) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    raise Exception(data.get("error", "Unknown error"))
                return data["result"]
    
    async def create_invoice(self, asset: str, amount: str, desc=None, payload=None, expires=1800):
        p = {"asset": asset, "amount": str(amount), "expires_in": expires}
        if desc: p["description"] = desc
        if payload: p["payload"] = payload
        return await self._req("createInvoice", p)
    
    async def get_invoices(self, ids: list):
        if not ids: return {"items": []}
        return await self._req("getInvoices", {"invoice_ids": ",".join(map(str, ids))})

# Инициализация CryptoPay (если токен задан)
crypto = CryptoPayClient(CRYPTO_PAY_TOKEN, testnet=CRYPTO_PAY_TESTNET) if CRYPTO_PAY_TOKEN else None
if not crypto:
    logger.warning("⚠️ CryptoPay не настроен. Автоматическая оплата через USDT недоступна.")

# === НАСТРОЙКА КЛЮЧЕЙ ===
KEYS_FILE = os.path.join(WORK_DIR, 'activation_keys.json')

def generate_random_key(prefix="Msdf"):
    import string
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    return f"{prefix}_{suffix}"

def load_keys():
    default_keys = {
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
    if user_id in ADMIN_IDS:
        return True
    return users_data.get(user_id, {}).get("is_admin", False)

def has_active_subscription(user_id):
    if user_id not in users_data:
        return False
    expires = users_data[user_id]["expires"]
    if isinstance(expires, str):
        expires = datetime.fromisoformat(expires)
    return expires > datetime.now()

def ensure_user_exists(user_id, username=""):
    if user_id not in users_data:
        users_data[user_id] = {
            "expires": (datetime.now() - timedelta(days=1)).isoformat(),
            "key_used": None,
            "is_admin": False,
            "username": username,
            "bound_username": "",
            "accounts": {}
        }
        save_users()
        logger.info(f"Создан новый пользователь {user_id} (без подписки)")

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
    kb = [
        [InlineKeyboardButton("🚀 Запустить", callback_data="start_ras"),
         InlineKeyboardButton("🛍 Магазин", callback_data="shop")],
        [InlineKeyboardButton("👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton("ℹ️ Информация о боте", callback_data="info")]
    ]
    if user_id in users_data:
        has_running = any(acc.get("running", False) for acc in users_data[user_id]["accounts"].values())
        if has_running:
            kb.append([InlineKeyboardButton("🛑 Стоп рассылки", callback_data="stop_all")])
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
        [InlineKeyboardButton("✨ Создать обычный ключ", callback_data="create_normal_key")],
        [InlineKeyboardButton("👑 Создать админ-ключ", callback_data="create_admin_key")],
        [InlineKeyboardButton("🖼 Сменить приветственное фото", callback_data="change_welcome_photo")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ])

def get_back_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]])

# ========== ФУНКЦИЯ ОТПРАВКИ ГЛАВНОГО МЕНЮ ==========
async def send_main_menu(target, user_id, text=None):
    """Отправляет главное меню с фото или без в зависимости от наличия активных рассылок"""
    if text is None:
        if user_id in users_data:
            text = "✨ *Главное меню*"
        else:
            text = "✨ *Добро пожаловать!* Приобретите подписку в магазине или активируйте ключ."
    has_running = user_id in users_data and any(acc.get("running", False) for acc in users_data[user_id]["accounts"].values())
    photo_id = None if has_running else get_welcome_photo_id()
    
    if photo_id and user_id in users_data:
        await target.reply_photo(photo_id, caption=text, reply_markup=get_main_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
    else:
        await target.reply(text, reply_markup=get_main_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)

# ========== ФУНКЦИЯ АВТОВЫДАЧИ КЛЮЧА ПОСЛЕ ОПЛАТЫ (если нужно) ==========
async def issue_key_to_user(user_id, subscription_type, days):
    new_key = generate_random_key()
    current_keys = load_keys()
    if subscription_type == "week":
        desc = f"Неделя (автовыдача)"
    elif subscription_type == "month":
        desc = f"Месяц (автовыдача)"
    elif subscription_type == "year":
        desc = f"Год (автовыдача)"
    else:
        desc = f"Навсегда (автовыдача)"
    current_keys[new_key] = (desc, days, False)
    save_keys(current_keys)

    ensure_user_exists(user_id)
    old_expires = datetime.fromisoformat(users_data[user_id]["expires"])
    new_expires = max(old_expires, datetime.now()) + timedelta(days=days)
    users_data[user_id]["expires"] = new_expires.isoformat()
    users_data[user_id]["key_used"] = new_key
    save_users()
    return new_key, new_expires

# ========== ФУНКЦИЯ АКТИВАЦИИ КЛЮЧА ==========
async def activate_key(user_id, key_text):
    keys = load_keys()
    if key_text not in keys:
        return False, "❌ Неверный ключ активации."

    # Проверяем, не использован ли ключ ранее
    for uid, data in users_data.items():
        if data.get("key_used") == key_text:
            return False, "❌ Этот ключ уже был использован."

    key_info = keys[key_text]
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

    ensure_user_exists(user_id)
    old_expires = datetime.fromisoformat(users_data[user_id]["expires"])
    new_expires = max(old_expires, datetime.now()) + timedelta(days=days)
    users_data[user_id]["expires"] = new_expires.isoformat()
    users_data[user_id]["key_used"] = key_text
    if is_admin_key:
        users_data[user_id]["is_admin"] = True
    save_users()

    if is_admin_key:
        return True, f"✅ Ключ активирован! Вы получили права администратора.\n📅 Подписка продлена до {new_expires.strftime('%d.%m.%Y')}."
    else:
        return True, f"✅ Ключ «{desc}» активирован!\n📅 Подписка активна до {new_expires.strftime('%d.%m.%Y')}."

# ========== ОБРАБОТЧИКИ ==========
@bot.on_message(filters.command("start"))
async def start_cmd(c: Client, m: Message):
    user_id = m.from_user.id
    username = m.from_user.username or m.from_user.first_name
    ensure_user_exists(user_id, username)
    await send_main_menu(m, user_id)

@bot.on_message(filters.command("sendkey") & filters.private)
async def send_key_command(c: Client, m: Message):
    """Администратор: /sendkey user_id описание дни (опционально)"""
    if not is_admin(m.from_user.id):
        await m.reply("⛔ Нет прав.")
        return
    args = m.text.split(maxsplit=3)
    if len(args) < 3:
        await m.reply("❌ Использование: `/sendkey USER_ID ОПИСАНИЕ ДНИ`\nПример: `/sendkey 123456789 Пробный 7`", parse_mode=enums.ParseMode.MARKDOWN)
        return
    try:
        target_id = int(args[1])
        desc = args[2]
        days = int(args[3]) if len(args) > 3 else 30
    except:
        await m.reply("❌ Неверный формат. ID и дни должны быть числами.")
        return
    new_key = generate_random_key()
    keys = load_keys()
    keys[new_key] = (desc, days, False)
    save_keys(keys)
    try:
        await bot.send_message(target_id, f"🔑 Администратор отправил вам ключ:\n`{new_key}`\n\nИспользуйте кнопку «Активировать ключ» в профиле.")
        await m.reply(f"✅ Ключ `{new_key}` отправлен пользователю {target_id}.\nОписание: {desc}\nДней: {days}")
    except Exception as e:
        await m.reply(f"❌ Не удалось отправить пользователю: {e}")

@bot.on_message(filters.text & filters.private)
async def handle_text(c: Client, m: Message):
    user_id = m.from_user.id
    text = m.text

    ensure_user_exists(user_id, m.from_user.username or m.from_user.first_name)

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
        await send_main_menu(m, user_id, "✅ Текст рассылки обновлён для всех аккаунтов.")
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
            await send_main_menu(m, user_id, f"✅ Интервал установлен: {interval} сек.")
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
                    if has_active_subscription(user_id):
                        asyncio.create_task(safe_spam_cycle(user_id, phone, acc, m))
                    else:
                        await m.reply("❌ Для запуска рассылки необходима активная подписка! Приобретите её в магазине или активируйте ключ.")
                        acc["running"] = False
            save_users()
            if has_active_subscription(user_id):
                await send_main_menu(m, user_id, f"🛡 Безопасная рассылка запущена для {len(accounts)} аккаунтов.")
            else:
                await send_main_menu(m, user_id, "❌ Не удалось запустить рассылку: нет активной подписки.")
            temp_auth.pop(user_id)
        except ValueError:
            await m.reply("❌ Введите число.")
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "wait_key":
        success, message = await activate_key(user_id, text.strip())
        await send_main_menu(m, user_id, message)
        temp_auth.pop(user_id)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "create_key_days":
        try:
            days = int(text.strip())
            if days <= 0:
                await m.reply("❌ Количество дней должно быть положительным числом.")
                return
            temp_auth[user_id]["days"] = days
            temp_auth[user_id]["step"] = "create_key_desc"
            await m.reply("📝 Введите описание для ключа (например, «Пробный на 7 дней»):")
        except ValueError:
            await m.reply("❌ Введите целое число (количество дней).")
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "create_key_desc":
        desc = text.strip()
        days = temp_auth[user_id]["days"]
        is_admin = temp_auth[user_id].get("is_admin", False)
        new_key = generate_random_key()
        keys = load_keys()
        keys[new_key] = (desc, days, is_admin)
        save_keys(keys)
        await m.reply(
            f"✅ Ключ успешно создан!\n\n"
            f"🔑 `{new_key}`\n"
            f"📝 Описание: {desc}\n"
            f"📅 Срок: {days} дней\n"
            f"👑 Админский: {'Да' if is_admin else 'Нет'}",
            parse_mode=enums.ParseMode.MARKDOWN,
            reply_markup=get_admin_panel_keyboard()
        )
        temp_auth.pop(user_id)
        return

    # Если ничего не подошло – показываем главное меню
    await send_main_menu(m, user_id)

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
    await send_main_menu(m, user_id, f"✅ Аккаунт {phone} успешно добавлен!")
    temp_auth.pop(uid, None)

@bot.on_callback_query()
async def handle_callback(c: Client, query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data

    if user_id not in users_data and data not in ["sub_week", "sub_month", "sub_year", "sub_forever", "payment_crypto", "payment_card", "shop"]:
        ensure_user_exists(user_id, query.from_user.username or query.from_user.first_name)

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
        await send_main_menu(query.message, user_id, "❌ Платёж отменён.")
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
    elif data == "create_normal_key" and is_admin(user_id):
        temp_auth[user_id] = {"step": "create_key_days", "is_admin": False}
        await query.message.reply("🔢 Введите количество дней действия ключа (целое число):")
        await query.answer()
    elif data == "create_admin_key" and is_admin(user_id):
        temp_auth[user_id] = {"step": "create_key_days", "is_admin": True}
        await query.message.reply("🔢 Введите количество дней действия админ-ключа (целое число):")
        await query.answer()
    elif data == "change_welcome_photo" and is_admin(user_id):
        temp_auth[user_id] = {"step": "wait_photo"}
        await query.message.reply("📸 Отправьте новое приветственное фото (как обычное изображение).")
        await query.answer()
    elif data == "back_to_main":
        await send_main_menu(query.message, user_id)
    elif data == "add_account":
        if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
            await query.answer(f"❌ Лимит {MAX_ACCOUNTS_PER_USER} аккаунтов", show_alert=True)
            return
        temp_auth[user_id] = {"step": "phone", "user_id": user_id}
        await query.message.reply("📱 Введите номер телефона в международном формате (например, +380123456789):")
        await query.answer()
    elif data == "activate_key":
        temp_auth[user_id] = {"step": "wait_key"}
        await query.message.reply("🔑 Введите активационный ключ:")
        await query.answer()
    elif data.startswith("check_payment_"):
        await check_payment(query)
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
    temp_auth[query.from_user.id] = {"subscription": sub_type, "days": days, "price": price}

@bot.on_callback_query(filters.regex(r"^pay_(week|month|year|forever)$"))
async def pay_subscription(c, query):
    sub_type = query.data.split("_")[1]
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

# ========== НОВАЯ ОБРАБОТКА КРИПТОПЛАТЕЖЕЙ С CRYPTOPAY ==========
async def process_crypto_payment(query: CallbackQuery):
    user_id = query.from_user.id
    sub_data = temp_auth.get(user_id, {})
    if not sub_data:
        await query.answer("Ошибка, попробуйте снова", show_alert=True)
        return

    price = sub_data.get("price", 0)
    days = sub_data.get("days", 30)
    sub_type = sub_data.get("subscription", "month")

    if not crypto:
        await query.message.edit_text("❌ Оплата криптовалютой временно недоступна. Обратитесь к администратору.")
        return

    try:
        inv = await crypto.create_invoice(
            "USDT", f"{price:.2f}",
            desc=f"Подписка {sub_type} на {days} дней",
            payload=f"sub_{sub_type}_{user_id}_{int(datetime.now().timestamp())}",
            expires=1800
        )
        invoice_id = inv["invoice_id"]
        url = inv.get("bot_invoice_url")
        if not url:
            raise ValueError("CryptoPay не вернул ссылку")

        temp_auth[user_id]["invoice_id"] = invoice_id
        temp_auth[user_id]["payment_step"] = "awaiting_payment"

        text = (
            f"💸 *Оплата через CryptoPay (USDT)*\n\n"
            f"💰 Сумма: ${price:.2f}\n"
            f"📅 Подписка: {sub_type.capitalize()} ({days} дней)\n\n"
            f"👉 [Оплатить через бота CryptoPay]({url})\n\n"
            f"После оплаты нажмите кнопку «✅ Проверить оплату»."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Перейти к оплате", url=url)],
            [InlineKeyboardButton("✅ Проверить оплату", callback_data=f"check_payment_{invoice_id}")],
            [InlineKeyboardButton("◀️ Назад", callback_data="shop")]
        ])
        await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)

    except Exception as e:
        logger.error(f"CryptoPay error: {e}")
        await query.message.edit_text(f"❌ Ошибка создания счёта: {e}\nПопробуйте позже или выберите другой способ оплаты.")

async def check_payment(query: CallbackQuery):
    user_id = query.from_user.id
    invoice_id = int(query.data.split("_")[2])
    sub_data = temp_auth.get(user_id, {})
    
    if sub_data.get("invoice_id") != invoice_id:
        await query.answer("❌ Сессия оплаты не найдена или устарела", show_alert=True)
        return

    if not crypto:
        await query.answer("❌ CryptoPay не настроен", show_alert=True)
        return

    try:
        res = await crypto.get_invoices([invoice_id])
        if res and res.get('items'):
            inv = res['items'][0]
            if inv.get('status') == 'paid':
                # Оплата подтверждена – активируем подписку
                sub_type = sub_data.get("subscription")
                days = sub_data.get("days")
                price = sub_data.get("price")

                ensure_user_exists(user_id)
                old_expires = datetime.fromisoformat(users_data[user_id]["expires"])
                new_expires = max(old_expires, datetime.now()) + timedelta(days=days)
                users_data[user_id]["expires"] = new_expires.isoformat()
                # Сохраняем информацию о платеже
                if "payments" not in users_data[user_id]:
                    users_data[user_id]["payments"] = []
                users_data[user_id]["payments"].append({
                    "date": datetime.now().isoformat(),
                    "amount": price,
                    "subscription": sub_type,
                    "invoice_id": invoice_id,
                    "method": "cryptopay"
                })
                save_users()

                # Очищаем временные данные
                temp_auth.pop(user_id, None)

                await query.message.edit_text(
                    f"✅ *Оплата подтверждена!*\n\n"
                    f"📅 Ваша подписка активна до {new_expires.strftime('%d.%m.%Y')}\n"
                    f"💰 Сумма: ${price:.2f}\n"
                    f"Теперь вы можете запускать рассылку.",
                    reply_markup=get_back_keyboard(),
                    parse_mode=enums.ParseMode.MARKDOWN
                )
                # Уведомляем администраторов
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(
                            admin_id,
                            f"✅ Пользователь {user_id} оплатил подписку {sub_type} (${price}) через CryptoPay. Действует до {new_expires.strftime('%d.%m.%Y')}"
                        )
                    except:
                        pass
            else:
                await query.answer("⏳ Платёж ещё не подтверждён. Подождите немного и нажмите «Проверить» снова.", show_alert=True)
        else:
            await query.answer("❌ Счёт не найден", show_alert=True)
    except Exception as e:
        logger.error(f"Ошибка проверки платежа: {e}")
        await query.answer("Ошибка при проверке, попробуйте позже", show_alert=True)

# ========== ОСТАЛЬНЫЕ ФУНКЦИИ (ПРОФИЛЬ, СТАТИСТИКА И Т.Д.) ==========
async def process_card_payment(query: CallbackQuery):
    user_id = query.from_user.id
    sub_data = temp_auth.get(user_id, {})
    price = sub_data.get("price", 0)
    days = sub_data.get("days", 30)
    sub_type = sub_data.get("subscription", "month")

    text = (
        f"💳 *Оплата украинской картой*\n\n"
        f"💰 Сумма: ${price}\n"
        f"📅 Подписка: {sub_type.capitalize()} ({days} дней)\n\n"
        f"Для оплаты этим методом обратитесь к администратору:\n"
        f"👤 @its_neverka\n\n"
        f"После оплаты нажмите кнопку ниже, чтобы уведомить администратора."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📩 Я оплатил, уведомить", callback_data="notify_admin_card")],
        [InlineKeyboardButton("◀️ Назад", callback_data="shop")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)

async def show_profile(query: CallbackQuery):
    user_id = query.from_user.id
    ensure_user_exists(user_id, query.from_user.username or query.from_user.first_name)
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
    if has_active_subscription(user_id):
        text += f"📅 Подписка активна до: {datetime.fromisoformat(data['expires']).strftime('%d.%m.%Y')}\n"
    else:
        text += f"❌ *Подписка отсутствует* — для запуска рассылки необходимо её приобрести или активировать ключ.\n"

    if accounts:
        text += "\n📋 *Список аккаунтов*:\n"
        for i, (phone, acc) in enumerate(accounts.items(), 1):
            status = "🟢 Активен" if acc.get("running", False) else "🔴 Остановлен"
            client_ok = "✅" if "client" in acc else "❌"
            safe_mark = "🛡" if acc.get("safe_mode", False) else ""
            text += f"{i}. {phone} {client_ok} {status} {safe_mark}\n"
            text += f"   Текст: {acc['text'][:40]}...\n"
            text += f"   Интервал: {acc['interval']} сек.\n"
    else:
        text += "\n📭 Нет добавленных аккаунтов.\n"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("🔑 Активировать ключ", callback_data="activate_key")],
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
        "• Управление подпиской через магазин или активацию ключа\n\n"
        "💰 *Для запуска рассылки требуется активная подписка.*\n"
        "📞 **Поддержка:** @its_neverka\n\n"
        "© 2026 NeverkaBOT"
    )
    await query.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)

async def start_normal_ras(query: CallbackQuery):
    user_id = query.from_user.id
    if not has_active_subscription(user_id):
        await query.answer("❌ Для запуска рассылки необходима активная подписка! Приобретите её в магазине или активируйте ключ.", show_alert=True)
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
    await send_main_menu(query.message, user_id, f"🚀 Запущено обычных рассылок: {started}")
    await query.answer()

async def start_safe_mode(query: CallbackQuery):
    user_id = query.from_user.id
    if not has_active_subscription(user_id):
        await query.answer("❌ Для запуска рассылки необходима активная подписка! Приобретите её в магазине или активируйте ключ.", show_alert=True)
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
    await send_main_menu(query.message, user_id, f"🛑 Остановлено рассылок: {stopped}")
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
        text += f"📱 Акков: {acc_count} | Подписка до: {expires.strftime('%d.%m.%Y')}\n"
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
        await send_main_menu(m, user_id, "✅ Приветственное фото обновлено!")
        temp_auth.pop(user_id)
    else:
        await send_main_menu(m, user_id)

# ========== ГРАЦИОЗНОЕ ЗАВЕРШЕНИЕ ==========
async def shutdown():
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

# ========== ЗАПУСК ==========
async def main():
    load_users()
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
    await bot.start()
    logger.info("🤖 Бот запущен и готов к работе")
    await idle()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        loop.run_until_complete(shutdown())
    finally:
        loop.close()
