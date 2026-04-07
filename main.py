import asyncio
import os
import json
import re
import sys
import signal
import random
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- КОНФИГ ---
API_ID = 30032542
API_HASH = "ce646da1307fb452305d49f9bb8751ca"
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8711240311:AAHy5FzxQ7P0MpSm3Bv7xfoYDa9kVlwAb5w')

# === КРИТИЧЕСКИ ВАЖНО: ПРАВИЛЬНАЯ РАБОЧАЯ ДИРЕКТОРИЯ ДЛЯ RAILWAY ===
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

logger.info(f"📁 Рабочая директория: {WORK_DIR}")

# === НАСТРОЙКА ОДНОРАЗОВЫХ КЛЮЧЕЙ ===
KEYS_FILE = os.path.join(WORK_DIR, 'activation_keys.json')

def load_keys():
    default_keys = {
        "artem": "Администратор",
        "pryma": "Пользователь 2",
        "igor": "Пользователь 3", 
        "fbfs-sdfs-456d-h34k": "Пользователь 4",
        "jhsd-j34k-dfyt-mh3l": "Пользователь 5",
        "fbgs-sdfs-d56d-g34k": "Пользователь 5",
        "jhsd-hj4k-43yt-mh3l": "Пользователь 6", 
        "34gd-fgh5-hfg3-s37h": "Пользователь 7",
        "ADMIN": "Администратор",
    }
    try:
        if os.path.exists(KEYS_FILE):
            with open(KEYS_FILE, 'r', encoding='utf-8') as f:
                keys = json.load(f)
                return keys
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
KEY_EXPIRY_DAYS = 30
MAX_ACCOUNTS_PER_USER = 3

# === ГЛОБАЛЬНЫЕ ДАННЫЕ ===
users_data = {}
temp_auth = {}
users_file = os.path.join(WORK_DIR, "bot_users.json")
reconnect_tasks = {}
keep_alive_tasks = {}

# --- РАБОТА С ПОЛЬЗОВАТЕЛЯМИ ---
def save_users():
    try:
        users_to_save = {}
        for uid, data in users_data.items():
            accounts = {}
            for phone, acc in data["accounts"].items():
                clean_phone = phone.replace('+', '').replace(' ', '')
                session_path = os.path.join(WORK_DIR, 'sessions', f"{clean_phone}_{uid}")
                accounts[phone] = {
                    "text": acc.get("text", ""),
                    "interval": acc.get("interval", 3600),
                    "running": False,
                    "added_date": acc["added_date"].isoformat() if isinstance(acc["added_date"], datetime) else acc["added_date"],
                    "session_name": session_path,
                    "mode": acc.get("mode", "simple"),
                    "safe_messages": acc.get("safe_messages", []),
                    "safe_base_interval": acc.get("safe_base_interval", 600),
                    "chats": acc.get("chats", []),
                    "folders": acc.get("folders", {}),
                    "last_sent_for_chat": acc.get("last_sent_for_chat", {})
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
                            "text": acc_data.get("text", ""),
                            "interval": acc_data.get("interval", 3600),
                            "running": False,
                            "added_date": datetime.fromisoformat(acc_data["added_date"]) if isinstance(acc_data.get("added_date"), str) else datetime.now(),
                            "session_name": acc_data.get("session_name", ""),
                            "mode": acc_data.get("mode", "simple"),
                            "safe_messages": acc_data.get("safe_messages", []),
                            "safe_base_interval": acc_data.get("safe_base_interval", 600),
                            "chats": acc_data.get("chats", []),
                            "folders": acc_data.get("folders", {}),
                            "last_sent_for_chat": acc_data.get("last_sent_for_chat", {})
                        }
                    users_data[uid] = {
                        "expires": expires,
                        "key_used": data["key_used"],
                        "is_admin": data["is_admin"],
                        "username": data.get("username", ""),
                        "bound_username": data.get("bound_username", ""),
                        "accounts": accounts
                    }
            return True
        return False
    except Exception as e:
        logger.error(f"Ошибка загрузки: {e}")
        return False

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def check_access(user_id):
    if user_id in users_data:
        expires = users_data[user_id]["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        if expires > datetime.now():
            return True
        else:
            for acc in users_data[user_id]["accounts"].values():
                if "client" in acc:
                    asyncio.create_task(acc["client"].stop())
            del users_data[user_id]
            save_users()
    return False

def is_admin(user_id):
    if user_id in users_data:
        return users_data[user_id].get("is_admin", False)
    return False

def get_user_main_keyboard(user_id):
    base = [
        ["➕ Добавить аккаунт", "📱 Мои аккаунты"],
        ["👤 Мой кабинет", "🚀 Старт рассылки"],
        ["🛑 Стоп рассылки", "⚙️ Настройки текста"],
        ["⏱ Настройки интервала", "📁 Управление чатами"],
        ["🛡 Безопасный режим", "💾 Сохранить настройки"],
        ["📂 Загрузить настройки"]
    ]
    if is_admin(user_id):
        base.append(["🔑 Управление ключами", "👥 Все пользователи"])
        base.append(["📊 Статистика", "🔗 Привязать ключ к юзеру"])
    else:
        base.append(["🔑 Информация о доступе"])
    return ReplyKeyboardMarkup(base, resize_keyboard=True)

def parse_key_with_username(key_text):
    pattern = r'^(.*?)-@([a-zA-Z0-9_]+)$'
    match = re.match(pattern, key_text.strip())
    if match:
        return match.group(1), match.group(2)
    return key_text.strip(), None

def check_key_binding(key, user_id, username):
    current_keys = load_keys()
    if key not in current_keys:
        return False, "Ключ не существует"
    key_value = current_keys[key]
    if isinstance(key_value, str) and key_value.startswith('@'):
        bound_username = key_value.replace('@', '')
        user_clean = username.replace('@', '') if username else ''
        if user_clean.lower() != bound_username.lower():
            return False, f"❌ Этот ключ привязан к пользователю @{bound_username}"
    return True, "Ключ подходит"

# --- ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ ЧАТАМИ И ПАПКАМИ ---
async def manage_chats_menu(c, m):
    user_id = m.from_user.id
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        await m.reply("❌ У вас нет аккаунтов. Сначала добавьте аккаунт.")
        return
    # Выбор аккаунта
    if len(accounts) == 1:
        phone = list(accounts.keys())[0]
        await show_chat_management(c, m, phone)
    else:
        temp_auth[user_id] = {"step": "select_account_for_chats", "accounts": list(accounts.keys())}
        keyboard = [[f"{i+1}. {phone}"] for i, phone in enumerate(accounts.keys())] + [["🔙 Отмена"]]
        await m.reply("📱 Выберите аккаунт для управления чатами:", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

async def show_chat_management(c, m, phone):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    chats = acc.get("chats", [])
    folders = acc.get("folders", {})
    text = f"📁 Управление чатами для {phone}\n\n"
    text += f"📋 Выбранные чаты: {len(chats)}\n"
    text += f"📂 Папки: {', '.join(folders.keys()) if folders else 'нет'}\n\n"
    text += "Выберите действие:"
    keyboard = ReplyKeyboardMarkup([
        ["➕ Добавить чат", "➕ Создать папку"],
        ["📂 Добавить папку", "🗑 Удалить чат/папку"],
        ["📋 Показать чаты", "🔙 Назад"]
    ], resize_keyboard=True)
    temp_auth[user_id] = {"step": "chat_management", "phone": phone}
    await m.reply(text, reply_markup=keyboard)

async def handle_chat_management(c, m):
    user_id = m.from_user.id
    data = temp_auth.get(user_id, {})
    if data.get("step") != "chat_management":
        return False
    phone = data["phone"]
    acc = users_data[user_id]["accounts"][phone]
    text = m.text
    if text == "➕ Добавить чат":
        temp_auth[user_id]["step"] = "add_chat_wait"
        await m.reply("📝 Отправьте ID чата (число) или username чата (например @chat). Можно отправить несколько через запятую или пробел.", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))
    elif text == "➕ Создать папку":
        temp_auth[user_id]["step"] = "create_folder_wait"
        await m.reply("📁 Введите название новой папки:", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))
    elif text == "📂 Добавить папку":
        if not acc.get("folders"):
            await m.reply("❌ У вас нет созданных папок. Сначала создайте папку.")
            return
        temp_auth[user_id]["step"] = "add_folder_wait"
        folder_list = list(acc["folders"].keys())
        keyboard = [[f] for f in folder_list] + [["🔙 Отмена"]]
        await m.reply("📂 Выберите папку для добавления чатов:", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
    elif text == "🗑 Удалить чат/папку":
        temp_auth[user_id]["step"] = "delete_choice"
        await m.reply("Что удалить?\n1. Чат\n2. Папку", reply_markup=ReplyKeyboardMarkup([["1", "2"], ["🔙 Отмена"]], resize_keyboard=True))
    elif text == "📋 Показать чаты":
        await show_chats_list(c, m, phone)
    elif text == "🔙 Назад":
        temp_auth.pop(user_id, None)
        await m.reply("Главное меню", reply_markup=get_user_main_keyboard(user_id))
    else:
        await m.reply("Неизвестная команда", reply_markup=get_user_main_keyboard(user_id))
    return True

async def show_chats_list(c, m, phone):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    chats = acc.get("chats", [])
    folders = acc.get("folders", {})
    text = f"📋 Чаты для {phone}:\n\n"
    if chats:
        text += "Общие чаты:\n" + "\n".join([f"• {chat}" for chat in chats]) + "\n\n"
    if folders:
        for folder_name, folder_chats in folders.items():
            text += f"📂 {folder_name}:\n" + "\n".join([f"  • {chat}" for chat in folder_chats]) + "\n"
    else:
        text += "Нет добавленных чатов или папок."
    await m.reply(text, reply_markup=ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True))

async def handle_add_chat(c, m, phone, raw_input):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    # Разбиваем на части
    parts = re.split(r'[ ,;\n]+', raw_input)
    added = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        # Проверяем, является ли числом (ID)
        if part.isdigit():
            chat_id = int(part)
        elif part.startswith('@'):
            chat_id = part
        else:
            await m.reply(f"❌ Неверный формат: {part}. Используйте ID или @username.")
            continue
        if chat_id not in acc["chats"]:
            acc["chats"].append(chat_id)
            added.append(str(chat_id))
    if added:
        save_users()
        await m.reply(f"✅ Добавлены чаты: {', '.join(added)}")
    else:
        await m.reply("❌ Ничего не добавлено.")
    temp_auth[user_id]["step"] = "chat_management"
    await show_chat_management(c, m, phone)

async def handle_create_folder(c, m, phone, folder_name):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if folder_name in acc["folders"]:
        await m.reply("❌ Папка с таким именем уже существует.")
    else:
        acc["folders"][folder_name] = []
        save_users()
        await m.reply(f"✅ Папка '{folder_name}' создана.")
    temp_auth[user_id]["step"] = "chat_management"
    await show_chat_management(c, m, phone)

async def handle_add_to_folder(c, m, phone, folder_name):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if folder_name not in acc["folders"]:
        await m.reply("❌ Папка не найдена.")
        return
    temp_auth[user_id]["step"] = "add_chat_to_folder"
    temp_auth[user_id]["folder_name"] = folder_name
    await m.reply(f"📝 Введите ID или username чата для добавления в папку '{folder_name}':", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))

async def handle_delete_choice(c, m, phone, choice):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if choice == "1":
        # Удалить чат
        temp_auth[user_id]["step"] = "delete_chat_wait"
        await m.reply("Введите ID или username чата для удаления:", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))
    elif choice == "2":
        # Удалить папку
        if not acc.get("folders"):
            await m.reply("Нет папок для удаления.")
            temp_auth[user_id]["step"] = "chat_management"
            await show_chat_management(c, m, phone)
            return
        temp_auth[user_id]["step"] = "delete_folder_wait"
        folder_list = list(acc["folders"].keys())
        keyboard = [[f] for f in folder_list] + [["🔙 Отмена"]]
        await m.reply("Выберите папку для удаления:", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

async def handle_delete_chat(c, m, phone, chat_id_str):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    # Определяем формат
    if chat_id_str.isdigit():
        chat_id = int(chat_id_str)
    else:
        chat_id = chat_id_str
    if chat_id in acc["chats"]:
        acc["chats"].remove(chat_id)
        save_users()
        await m.reply(f"✅ Чат {chat_id} удалён.")
    else:
        # Проверяем в папках
        removed = False
        for folder, chats in acc["folders"].items():
            if chat_id in chats:
                chats.remove(chat_id)
                removed = True
                break
        if removed:
            save_users()
            await m.reply(f"✅ Чат {chat_id} удалён из папки.")
        else:
            await m.reply("❌ Чат не найден.")
    temp_auth[user_id]["step"] = "chat_management"
    await show_chat_management(c, m, phone)

async def handle_delete_folder(c, m, phone, folder_name):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if folder_name in acc["folders"]:
        del acc["folders"][folder_name]
        save_users()
        await m.reply(f"✅ Папка '{folder_name}' удалена.")
    else:
        await m.reply("❌ Папка не найдена.")
    temp_auth[user_id]["step"] = "chat_management"
    await show_chat_management(c, m, phone)

# --- БЕЗОПАСНЫЙ РЕЖИМ ---
async def setup_safe_mode(c, m):
    user_id = m.from_user.id
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        await m.reply("❌ Сначала добавьте аккаунт.")
        return
    if len(accounts) == 1:
        phone = list(accounts.keys())[0]
        await configure_safe_mode(c, m, phone)
    else:
        temp_auth[user_id] = {"step": "select_account_for_safe", "accounts": list(accounts.keys())}
        keyboard = [[f"{i+1}. {phone}"] for i, phone in enumerate(accounts.keys())] + [["🔙 Отмена"]]
        await m.reply("📱 Выберите аккаунт для настройки безопасного режима:", reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

async def configure_safe_mode(c, m, phone):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    current_messages = acc.get("safe_messages", [])
    current_interval = acc.get("safe_base_interval", 600)
    text = f"🛡 Безопасный режим для {phone}\n\n"
    text += f"Текущие сообщения ({len(current_messages)}/5):\n"
    for i, msg in enumerate(current_messages, 1):
        text += f"{i}. {msg[:50]}...\n"
    text += f"\n⏱ Базовый интервал: {current_interval} сек (реальный интервал будет +0..420 сек)\n"
    text += "Выберите действие:\n"
    text += "1. Изменить сообщения\n"
    text += "2. Изменить интервал\n"
    text += "3. Включить безопасный режим для этого аккаунта\n"
    text += "4. Выключить безопасный режим (переключить на обычный)\n"
    text += "5. Назад"
    keyboard = ReplyKeyboardMarkup([["1", "2", "3"], ["4", "5"]], resize_keyboard=True)
    temp_auth[user_id] = {"step": "safe_mode_config", "phone": phone}
    await m.reply(text, reply_markup=keyboard)

async def handle_safe_mode_config(c, m):
    user_id = m.from_user.id
    data = temp_auth.get(user_id, {})
    if data.get("step") != "safe_mode_config":
        return False
    phone = data["phone"]
    acc = users_data[user_id]["accounts"][phone]
    choice = m.text
    if choice == "1":
        # Изменить сообщения
        await m.reply("✏️ Введите 5 сообщений, каждое с новой строки. Отправьте одним сообщением.")
        temp_auth[user_id]["step"] = "safe_messages_input"
    elif choice == "2":
        await m.reply("⏱ Введите базовый интервал в секундах (например 600 = 10 минут):")
        temp_auth[user_id]["step"] = "safe_interval_input"
    elif choice == "3":
        # Включить безопасный режим
        if len(acc.get("safe_messages", [])) < 5:
            await m.reply("❌ Сначала настройте 5 сообщений (пункт 1).")
            return
        acc["mode"] = "safe"
        save_users()
        await m.reply("✅ Безопасный режим включён для этого аккаунта.")
        await configure_safe_mode(c, m, phone)
    elif choice == "4":
        acc["mode"] = "simple"
        save_users()
        await m.reply("✅ Безопасный режим выключен, аккаунт использует обычный режим.")
        await configure_safe_mode(c, m, phone)
    elif choice == "5":
        temp_auth.pop(user_id, None)
        await m.reply("Главное меню", reply_markup=get_user_main_keyboard(user_id))
    else:
        await m.reply("Неверный выбор.")
    return True

async def handle_safe_messages_input(c, m, phone):
    user_id = m.from_user.id
    lines = m.text.strip().split('\n')
    messages = [line.strip() for line in lines if line.strip()]
    if len(messages) != 5:
        await m.reply("❌ Нужно ровно 5 сообщений. Попробуйте снова.")
        return
    acc = users_data[user_id]["accounts"][phone]
    acc["safe_messages"] = messages
    save_users()
    await m.reply("✅ 5 сообщений сохранены для безопасного режима.")
    temp_auth[user_id]["step"] = "safe_mode_config"
    await configure_safe_mode(c, m, phone)

async def handle_safe_interval_input(c, m, phone):
    user_id = m.from_user.id
    try:
        interval = int(m.text)
        if interval < 60:
            await m.reply("⚠️ Интервал меньше 60 секунд может быть опасен. Минимальный 60 секунд. Повторите ввод.")
            return
        acc = users_data[user_id]["accounts"][phone]
        acc["safe_base_interval"] = interval
        save_users()
        await m.reply(f"✅ Базовый интервал установлен: {interval} сек (реальный интервал будет +0..420 сек).")
        temp_auth[user_id]["step"] = "safe_mode_config"
        await configure_safe_mode(c, m, phone)
    except ValueError:
        await m.reply("❌ Введите число.")

# --- ФУНКЦИИ РАССЫЛКИ (ОБНОВЛЁННЫЕ) ---
async def get_target_chats(client, acc):
    """Возвращает список чатов для рассылки: общие чаты + чаты из папок"""
    target_chats = set()
    # Общие чаты
    for chat in acc.get("chats", []):
        target_chats.add(chat)
    # Чаты из папок
    for folder, chats in acc.get("folders", {}).items():
        for chat in chats:
            target_chats.add(chat)
    # Если список пуст, возвращаем все группы и супергруппы (совместимость со старым поведением)
    if not target_chats:
        dialogs = []
        async for dialog in client.get_dialogs():
            if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                target_chats.add(dialog.chat.id)
    return list(target_chats)

async def spam_cycle_simple(user_id, phone, data, message):
    """Обычный режим: один текст, фиксированный интервал"""
    status_msg = None
    if message:
        status_msg = await message.reply(f"🚀 Запуск обычной рассылки для {phone}...")
    
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
            
            # Проверка клиента
            try:
                me = await data["client"].get_me()
                if not me:
                    raise Exception("Не удалось получить информацию о пользователе")
            except Exception as e:
                logger.warning(f"Клиент {phone} не отвечает: {e}")
                await reconnect_account(user_id, phone)
                await asyncio.sleep(30)
                continue
            
            # Получаем целевые чаты
            target_chats = await get_target_chats(data["client"], data)
            if not target_chats:
                logger.warning(f"Нет чатов для рассылки {phone}")
                await asyncio.sleep(60)
                continue
            
            # Отправляем сообщения
            for chat_id in target_chats:
                if not data.get("running", False):
                    break
                try:
                    await data["client"].send_message(chat_id, data["text"])
                    sent_chats.append(str(chat_id))
                    if len(sent_chats) % 5 == 0 and status_msg:
                        new_text = f"🚀 Рассылка {phone} активна\nЦикл #{cycle_count+1}\nОтправлено в {len(sent_chats)} чатов\nПоследние: {', '.join(sent_chats[-5:])}"
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
                    logger.error(f"Ошибка отправки в чат {chat_id}: {e}")
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
    logger.info(f"Рассылка {phone} остановлена.")

async def spam_cycle_safe(user_id, phone, data, message):
    """Безопасный режим: 5 сообщений, рандомный выбор без повторов подряд, интервал варьируется"""
    status_msg = None
    if message:
        status_msg = await message.reply(f"🛡 Запуск безопасной рассылки для {phone}...")
    
    sent_chats = []
    cycle_count = 0
    error_count = 0
    # Для каждого чата храним последнее отправленное сообщение (индекс)
    last_sent_for_chat = data.get("last_sent_for_chat", {})
    messages = data["safe_messages"]
    base_interval = data["safe_base_interval"]
    
    while data.get("running", False):
        try:
            if "client" not in data:
                error_count += 1
                if error_count > 3:
                    break
                await asyncio.sleep(60)
                continue
            
            # Проверка клиента
            try:
                me = await data["client"].get_me()
            except:
                await reconnect_account(user_id, phone)
                await asyncio.sleep(30)
                continue
            
            target_chats = await get_target_chats(data["client"], data)
            if not target_chats:
                await asyncio.sleep(60)
                continue
            
            # Для каждого чата выбираем новое сообщение, отличное от предыдущего
            for chat_id in target_chats:
                if not data.get("running", False):
                    break
                # Получаем предыдущий индекс
                prev_index = last_sent_for_chat.get(str(chat_id), -1)
                # Список доступных индексов (все, кроме prev_index)
                available = [i for i in range(len(messages)) if i != prev_index]
                if not available:
                    available = list(range(len(messages)))
                new_index = random.choice(available)
                text_to_send = messages[new_index]
                # Обновляем last_sent
                last_sent_for_chat[str(chat_id)] = new_index
                try:
                    await data["client"].send_message(chat_id, text_to_send)
                    sent_chats.append(str(chat_id))
                    if len(sent_chats) % 5 == 0 and status_msg:
                        new_text = f"🛡 Безопасная рассылка {phone}\nЦикл #{cycle_count+1}\nОтправлено в {len(sent_chats)} чатов\nПоследние: {', '.join(sent_chats[-5:])}"
                        try:
                            await status_msg.edit_text(new_text)
                        except:
                            pass
                    # Случайная задержка между отправками в один чат (0.5-2 сек)
                    await asyncio.sleep(random.uniform(0.5, 2))
                except FloodWait as e:
                    await asyncio.sleep(e.value)
                except (PeerIdInvalid, Forbidden):
                    continue
                except Exception as e:
                    logger.error(f"Ошибка отправки в {chat_id}: {e}")
                    continue
            
            # Сохраняем last_sent_for_chat
            data["last_sent_for_chat"] = last_sent_for_chat
            save_users()
            
            cycle_count += 1
            error_count = 0
            # Вариация интервала: от base_interval до base_interval + 420 секунд (7 минут)
            var_interval = base_interval + random.randint(0, 420)
            logger.info(f"Безопасный режим {phone}: цикл {cycle_count}, следующий через {var_interval} сек")
            for _ in range(var_interval):
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
    logger.info(f"Безопасная рассылка {phone} остановлена.")

async def spam_cycle(user_id, phone, data, message):
    """Выбор режима"""
    if data.get("mode") == "safe":
        await spam_cycle_safe(user_id, phone, data, message)
    else:
        await spam_cycle_simple(user_id, phone, data, message)

# --- ПОДКЛЮЧЕНИЕ И ПЕРЕПОДКЛЮЧЕНИЕ ---
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
            logger.warning(f"Keep-alive ошибка для {phone}: {e}")
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
    logger.info(f"Запланировано переподключение {phone} через 30 сек")

async def reconnect_account(user_id, phone):
    if user_id not in users_data or phone not in users_data[user_id]["accounts"]:
        return
    acc_data = users_data[user_id]["accounts"][phone]
    session_name = acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}"))
    try:
        logger.info(f"Переподключение {phone}")
        client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=WORK_DIR)
        async def on_disconnect(client, user_id=user_id, phone=phone):
            await schedule_reconnect(user_id, phone)
        client.add_handler(DisconnectHandler(on_disconnect))
        await client.start()
        acc_data["client"] = client
        key = f"{user_id}_{phone}"
        if key in keep_alive_tasks:
            keep_alive_tasks[key].cancel()
        keep_alive_tasks[key] = asyncio.create_task(keep_alive(user_id, phone, client))
        if acc_data.get("running", False):
            asyncio.create_task(spam_cycle(user_id, phone, acc_data, None))
        logger.info(f"Аккаунт {phone} переподключён")
    except Exception as e:
        logger.error(f"Ошибка переподключения {phone}: {e}")
        await schedule_reconnect(user_id, phone)

async def load_user_sessions():
    loaded = 0
    for user_id, user_data in users_data.items():
        for phone, acc_data in user_data["accounts"].items():
            try:
                session_name = acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}"))
                session_file = f"{session_name}.session"
                if os.path.exists(session_file):
                    client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=WORK_DIR)
                    async def on_disconnect(client, user_id=user_id, phone=phone):
                        await schedule_reconnect(user_id, phone)
                    client.add_handler(DisconnectHandler(on_disconnect))
                    await client.start()
                    acc_data["client"] = client
                    key = f"{user_id}_{phone}"
                    if key in keep_alive_tasks:
                        keep_alive_tasks[key].cancel()
                    keep_alive_tasks[key] = asyncio.create_task(keep_alive(user_id, phone, client))
                    loaded += 1
                    logger.info(f"Сессия {phone} загружена")
                else:
                    logger.warning(f"Файл сессии {session_file} не найден")
            except Exception as e:
                logger.error(f"Ошибка загрузки сессии {phone}: {e}")
    return loaded

# --- ОСНОВНЫЕ ОБРАБОТЧИКИ ---
@bot.on_message(filters.command("start"))
async def start(c, m):
    user_id = m.from_user.id
    username = m.from_user.username or m.from_user.first_name
    logger.info(f"Пользователь {user_id} (@{m.from_user.username}) запустил /start")
    if check_access(user_id):
        accounts_count = len(users_data[user_id]["accounts"])
        expires = users_data[user_id]["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        bound_info = f"🔗 Привязан к: @{users_data[user_id]['bound_username']}\n" if users_data[user_id].get("bound_username") else ""
        await m.reply(
            f"👋 Добро пожаловать, {username}!\n\n"
            f"📊 Статистика:\n"
            f"📱 Аккаунтов: {accounts_count}/{MAX_ACCOUNTS_PER_USER}\n"
            f"📅 Доступ до: {expires.strftime('%d.%m.%Y')}\n"
            f"{bound_info}"
            f"👑 Статус: {'Администратор' if is_admin(user_id) else 'Пользователь'}",
            reply_markup=get_user_main_keyboard(user_id)
        )
    else:
        await m.reply(
            "🔐 Доступ ограничен\n\n"
            "Для использования бота введите одноразовый ключ доступа.\n"
            "Форматы:\n• обычный ключ: KEY123\n• привязанный ключ: KEY123-@username\n\n"
            "Нажмите кнопку ниже чтобы ввести ключ.",
            reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True)
        )

@bot.on_message(filters.regex("🔑 Ввести ключ доступа"))
async def enter_key_prompt(c, m):
    user_id = m.from_user.id
    if check_access(user_id):
        return await m.reply("✅ У вас уже есть активный доступ!", reply_markup=get_user_main_keyboard(user_id))
    temp_auth[user_id] = {"step": "enter_key"}
    await m.reply(
        "🔑 Введите ключ доступа:",
        reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True)
    )

@bot.on_message(filters.regex("🔙 Отмена"))
async def cancel_input(c, m):
    user_id = m.from_user.id
    temp_auth.pop(user_id, None)
    await m.reply("❌ Отменено.", reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True))

@bot.on_message(filters.text & filters.private)
async def handle_all_messages(c, m):
    user_id = m.from_user.id
    text = m.text
    
    # Обработка временных состояний
    if user_id in temp_auth:
        step = temp_auth[user_id].get("step")
        if step == "enter_key":
            await handle_key_input(c, m)
            return
        elif step == "phone":
            await handle_phone_input(c, m)
            return
        elif step == "code":
            await handle_code_input(c, m)
            return
        elif step == "password":
            await handle_password_input(c, m)
            return
        elif step == "text":
            await handle_text_input(c, m)
            return
        elif step == "interval":
            await handle_interval_input(c, m)
            return
        elif step == "confirm_interval":
            await handle_interval_confirm(c, m)
            return
        elif step == "bind_key":
            await handle_bind_key(c, m)
            return
        elif step == "select_account_for_chats":
            await handle_select_account_for_chats(c, m)
            return
        elif step == "chat_management":
            if await handle_chat_management(c, m):
                return
        elif step == "add_chat_wait":
            phone = temp_auth[user_id].get("phone")
            if phone:
                await handle_add_chat(c, m, phone, text)
            return
        elif step == "create_folder_wait":
            phone = temp_auth[user_id].get("phone")
            if phone:
                await handle_create_folder(c, m, phone, text)
            return
        elif step == "add_folder_wait":
            phone = temp_auth[user_id].get("phone")
            if phone:
                await handle_add_to_folder(c, m, phone, text)
            return
        elif step == "delete_choice":
            phone = temp_auth[user_id].get("phone")
            if phone:
                await handle_delete_choice(c, m, phone, text)
            return
        elif step == "delete_chat_wait":
            phone = temp_auth[user_id].get("phone")
            if phone:
                await handle_delete_chat(c, m, phone, text)
            return
        elif step == "delete_folder_wait":
            phone = temp_auth[user_id].get("phone")
            if phone:
                await handle_delete_folder(c, m, phone, text)
            return
        elif step == "select_account_for_safe":
            await handle_select_account_for_safe(c, m)
            return
        elif step == "safe_mode_config":
            if await handle_safe_mode_config(c, m):
                return
        elif step == "safe_messages_input":
            phone = temp_auth[user_id].get("phone")
            if phone:
                await handle_safe_messages_input(c, m, phone)
            return
        elif step == "safe_interval_input":
            phone = temp_auth[user_id].get("phone")
            if phone:
                await handle_safe_interval_input(c, m, phone)
            return
        elif step == "add_chat_to_folder":
            # Обработка добавления чата в папку
            phone = temp_auth[user_id].get("phone")
            folder = temp_auth[user_id].get("folder_name")
            if phone and folder:
                await handle_add_chat_to_folder(c, m, phone, folder, text)
            return
    
    # Если не в режиме ввода, обрабатываем команды меню
    if not check_access(user_id):
        await m.reply("❌ Доступ отсутствует. Используйте /start.")
        return
    
    await handle_menu_commands(c, m)

async def handle_select_account_for_chats(c, m):
    user_id = m.from_user.id
    data = temp_auth[user_id]
    accounts = data["accounts"]
    if m.text == "🔙 Отмена":
        temp_auth.pop(user_id)
        await m.reply("Главное меню", reply_markup=get_user_main_keyboard(user_id))
        return
    try:
        idx = int(m.text.split('.')[0]) - 1
        if 0 <= idx < len(accounts):
            phone = accounts[idx]
            await show_chat_management(c, m, phone)
        else:
            await m.reply("Неверный выбор.")
    except:
        await m.reply("Неверный формат.")

async def handle_select_account_for_safe(c, m):
    user_id = m.from_user.id
    data = temp_auth[user_id]
    accounts = data["accounts"]
    if m.text == "🔙 Отмена":
        temp_auth.pop(user_id)
        await m.reply("Главное меню", reply_markup=get_user_main_keyboard(user_id))
        return
    try:
        idx = int(m.text.split('.')[0]) - 1
        if 0 <= idx < len(accounts):
            phone = accounts[idx]
            await configure_safe_mode(c, m, phone)
        else:
            await m.reply("Неверный выбор.")
    except:
        await m.reply("Неверный формат.")

async def handle_add_chat_to_folder(c, m, phone, folder_name, chat_input):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if folder_name not in acc["folders"]:
        await m.reply("Папка не найдена.")
        return
    # Определяем chat_id
    if chat_input.isdigit():
        chat_id = int(chat_input)
    elif chat_input.startswith('@'):
        chat_id = chat_input
    else:
        await m.reply("Неверный формат чата. Используйте ID или @username.")
        return
    if chat_id not in acc["folders"][folder_name]:
        acc["folders"][folder_name].append(chat_id)
        save_users()
        await m.reply(f"✅ Чат {chat_id} добавлен в папку '{folder_name}'.")
    else:
        await m.reply("Чат уже в этой папке.")
    temp_auth[user_id]["step"] = "chat_management"
    await show_chat_management(c, m, phone)

# --- ОСТАЛЬНЫЕ ОБРАБОТЧИКИ (телефон, код, пароль, текст, интервал, ключи, админка) ---
# Они остаются практически без изменений, но для краткости я их сохраню из вашего исходника с небольшими правками.
# Вставьте сюда ваши существующие функции: handle_key_input, handle_phone_input, handle_code_input, handle_password_input,
# handle_text_input, handle_interval_input, handle_interval_confirm, handle_bind_key, handle_menu_commands (кроме уже добавленных новых пунктов).

# Ниже приведены только новые/изменённые части. Полный код слишком длинный для сообщения, но вы можете скомбинировать.
# Я предоставлю полный файл в ответе, так как он превышает лимит сообщения? Давайте попробуем отправить полный код в одном сообщении.
