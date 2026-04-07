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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

API_ID = 30032542
API_HASH = "ce646da1307fb452305d49f9bb8751ca"
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8711240311:AAHy5FzxQ7P0MpSm3Bv7xfoYDa9kVlwAb5w')

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

KEYS_FILE = os.path.join(WORK_DIR, 'activation_keys.json')

def load_keys():
    default_keys = {
        "artem": "Администратор", "pryma": "Пользователь 2", "igor": "Пользователь 3",
        "fbfs-sdfs-456d-h34k": "Пользователь 4", "jhsd-j34k-dfyt-mh3l": "Пользователь 5",
        "fbgs-sdfs-d56d-g34k": "Пользователь 5", "jhsd-hj4k-43yt-mh3l": "Пользователь 6",
        "34gd-fgh5-hfg3-s37h": "Пользователь 7", "ADMIN": "Администратор",
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
KEY_EXPIRY_DAYS = 30
MAX_ACCOUNTS_PER_USER = 3

users_data = {}
temp_auth = {}
users_file = os.path.join(WORK_DIR, "bot_users.json")
reconnect_tasks = {}
keep_alive_tasks = {}

bot_session_dir = os.path.join(WORK_DIR, 'bot_session')
os.makedirs(bot_session_dir, exist_ok=True)
bot = Client("manager_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir=bot_session_dir)

# ================= ДОПОЛНИТЕЛЬНЫЕ СТРУКТУРЫ =================
# В accounts теперь храним:
#   "mode": "simple" или "safe"
#   "text": для простого режима (единый текст)
#   "interval": для простого режима (единый интервал)
#   "safe_messages": список из 5 строк
#   "safe_base_interval": базовый интервал в секундах
#   "chats": список ID/username выбранных чатов (если не используется папки)
#   "folders": {"имя_папки": [список чатов]}
#   "chat_settings": {
#       chat_id: {"text": "...", "interval": секунды}   # для простого режима
#   }
#   "folder_settings": {
#       folder_name: {"text": "...", "interval": секунды}
#   }
#   "last_message_index": для безопасного режима: {chat_id: последний использованный индекс (0-4)}

def save_users():
    try:
        users_to_save = {}
        for uid, data in users_data.items():
            accounts = {}
            for phone, acc in data["accounts"].items():
                clean_phone = phone.replace('+', '').replace(' ', '')
                session_path = os.path.join(WORK_DIR, 'sessions', f"{clean_phone}_{uid}")
                accounts[phone] = {
                    "mode": acc.get("mode", "simple"),
                    "text": acc.get("text", ""),
                    "interval": acc.get("interval", 3600),
                    "safe_messages": acc.get("safe_messages", []),
                    "safe_base_interval": acc.get("safe_base_interval", 600),
                    "chats": acc.get("chats", []),
                    "folders": acc.get("folders", {}),
                    "chat_settings": acc.get("chat_settings", {}),
                    "folder_settings": acc.get("folder_settings", {}),
                    "last_message_index": acc.get("last_message_index", {}),
                    "running": False,
                    "added_date": acc["added_date"].isoformat() if isinstance(acc["added_date"], datetime) else acc["added_date"],
                    "session_name": session_path
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
                            "mode": acc_data.get("mode", "simple"),
                            "text": acc_data.get("text", ""),
                            "interval": acc_data.get("interval", 3600),
                            "safe_messages": acc_data.get("safe_messages", []),
                            "safe_base_interval": acc_data.get("safe_base_interval", 600),
                            "chats": acc_data.get("chats", []),
                            "folders": acc_data.get("folders", {}),
                            "chat_settings": acc_data.get("chat_settings", {}),
                            "folder_settings": acc_data.get("folder_settings", {}),
                            "last_message_index": acc_data.get("last_message_index", {}),
                            "running": False,
                            "added_date": datetime.fromisoformat(acc_data["added_date"]) if isinstance(acc_data.get("added_date"), str) else datetime.now(),
                            "session_name": acc_data.get("session_name", "")
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
    return users_data.get(user_id, {}).get("is_admin", False)

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

# ---------- ФУНКЦИИ ДЛЯ РАБОТЫ С ЧАТАМИ И ПАПКАМИ ----------
async def manage_chats_menu(c, m):
    user_id = m.from_user.id
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        await m.reply("❌ У вас нет аккаунтов. Сначала добавьте аккаунт.")
        return
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
    text = f"📁 Управление чатами для {phone}\n\n"
    text += f"📋 Выбранные чаты: {len(acc.get('chats', []))}\n"
    text += f"📂 Папки: {', '.join(acc.get('folders', {}).keys()) if acc.get('folders') else 'нет'}\n\n"
    text += "Выберите действие:"
    keyboard = ReplyKeyboardMarkup([
        ["➕ Добавить чат", "➕ Создать папку"],
        ["📂 Добавить в папку", "🗑 Удалить чат/папку"],
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
        await m.reply("📝 Отправьте ID чата (число) или username (например @chat). Можно несколько через запятую или пробел.", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))
    elif text == "➕ Создать папку":
        temp_auth[user_id]["step"] = "create_folder_wait"
        await m.reply("📁 Введите название новой папки:", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))
    elif text == "📂 Добавить в папку":
        if not acc.get("folders"):
            await m.reply("❌ У вас нет созданных папок. Сначала создайте папку.")
            return
        temp_auth[user_id]["step"] = "select_folder_for_add"
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
    parts = re.split(r'[ ,;\n]+', raw_input)
    added = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.isdigit():
            chat_id = int(part)
        elif part.startswith('@'):
            chat_id = part
        else:
            await m.reply(f"❌ Неверный формат: {part}. Используйте ID или @username.")
            continue
        if chat_id not in acc.get("chats", []):
            acc.setdefault("chats", []).append(chat_id)
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
    if folder_name in acc.get("folders", {}):
        await m.reply("❌ Папка с таким именем уже существует.")
    else:
        acc.setdefault("folders", {})[folder_name] = []
        save_users()
        await m.reply(f"✅ Папка '{folder_name}' создана.")
    temp_auth[user_id]["step"] = "chat_management"
    await show_chat_management(c, m, phone)

async def handle_select_folder_for_add(c, m, phone, folder_name):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if folder_name not in acc.get("folders", {}):
        await m.reply("❌ Папка не найдена.")
        return
    temp_auth[user_id]["step"] = "add_chat_to_folder"
    temp_auth[user_id]["folder_name"] = folder_name
    await m.reply(f"📝 Введите ID или username чата для добавления в папку '{folder_name}':", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))

async def handle_add_chat_to_folder(c, m, phone, folder_name, chat_input):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if folder_name not in acc.get("folders", {}):
        await m.reply("Папка не найдена.")
        return
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

async def handle_delete_choice(c, m, phone, choice):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if choice == "1":
        temp_auth[user_id]["step"] = "delete_chat_wait"
        await m.reply("Введите ID или username чата для удаления:", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))
    elif choice == "2":
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
    if chat_id_str.isdigit():
        chat_id = int(chat_id_str)
    else:
        chat_id = chat_id_str
    removed = False
    if chat_id in acc.get("chats", []):
        acc["chats"].remove(chat_id)
        removed = True
    else:
        for folder, chats in acc.get("folders", {}).items():
            if chat_id in chats:
                chats.remove(chat_id)
                removed = True
                break
    if removed:
        save_users()
        await m.reply(f"✅ Чат {chat_id} удалён.")
    else:
        await m.reply("❌ Чат не найден.")
    temp_auth[user_id]["step"] = "chat_management"
    await show_chat_management(c, m, phone)

async def handle_delete_folder(c, m, phone, folder_name):
    user_id = m.from_user.id
    acc = users_data[user_id]["accounts"][phone]
    if folder_name in acc.get("folders", {}):
        del acc["folders"][folder_name]
        save_users()
        await m.reply(f"✅ Папка '{folder_name}' удалена.")
    else:
        await m.reply("❌ Папка не найдена.")
    temp_auth[user_id]["step"] = "chat_management"
    await show_chat_management(c, m, phone)

# ---------- НАСТРОЙКА БЕЗОПАСНОГО РЕЖИМА ----------
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
    text += "Выберите действие:\n1. Изменить сообщения\n2. Изменить интервал\n3. Включить безопасный режим\n4. Выключить безопасный режим\n5. Назад"
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
        await m.reply("✏️ Введите 5 сообщений, каждое с новой строки. Отправьте одним сообщением.")
        temp_auth[user_id]["step"] = "safe_messages_input"
    elif choice == "2":
        await m.reply("⏱ Введите базовый интервал в секундах (например 600 = 10 минут):")
        temp_auth[user_id]["step"] = "safe_interval_input"
    elif choice == "3":
        if len(acc.get("safe_messages", [])) < 5:
            await m.reply("❌ Сначала настройте 5 сообщений (пункт 1).")
            return
        acc["mode"] = "safe"
        save_users()
        await m.reply("✅ Безопасный режим включён.")
        await configure_safe_mode(c, m, phone)
    elif choice == "4":
        acc["mode"] = "simple"
        save_users()
        await m.reply("✅ Безопасный режим выключен.")
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
    await m.reply("✅ 5 сообщений сохранены.")
    temp_auth[user_id]["step"] = "safe_mode_config"
    await configure_safe_mode(c, m, phone)

async def handle_safe_interval_input(c, m, phone):
    user_id = m.from_user.id
    try:
        interval = int(m.text)
        if interval < 60:
            await m.reply("⚠️ Минимальный интервал 60 секунд. Повторите ввод.")
            return
        acc = users_data[user_id]["accounts"][phone]
        acc["safe_base_interval"] = interval
        save_users()
        await m.reply(f"✅ Базовый интервал установлен: {interval} сек.")
        temp_auth[user_id]["step"] = "safe_mode_config"
        await configure_safe_mode(c, m, phone)
    except ValueError:
        await m.reply("❌ Введите число.")

# ---------- ФУНКЦИИ РАССЫЛКИ (НОВАЯ ВЕРСИЯ) ----------
async def get_target_chats(client, acc):
    """Собирает список чатов для рассылки на основе настроек аккаунта"""
    target_chats = []
    # Если есть явно выбранные чаты (без папок)
    if acc.get("chats"):
        for chat in acc["chats"]:
            target_chats.append(chat)
    # Добавляем чаты из папок
    for folder, chats in acc.get("folders", {}).items():
        for chat in chats:
            if chat not in target_chats:
                target_chats.append(chat)
    # Если ничего не выбрано – рассылаем во все диалоги (как раньше)
    if not target_chats:
        async for dialog in client.get_dialogs():
            if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                target_chats.append(dialog.chat.id)
    return target_chats

async def spam_cycle(user_id, phone, acc, message):
    """Обновлённый цикл рассылки с поддержкой чатов/папок и безопасного режима"""
    status_msg = None
    if message:
        status_msg = await message.reply(f"🚀 Запуск рассылки для {phone}...")

    sent_stats = {}  # chat_id -> количество отправленных за этот цикл
    cycle_count = 0
    error_count = 0

    # Для безопасного режима храним последний индекс для каждого чата
    if acc.get("mode") == "safe":
        last_index = acc.get("last_message_index", {})
    else:
        last_index = None

    while acc.get("running", False):
        try:
            if "client" not in acc:
                logger.error(f"❌ Нет клиента для {phone}")
                error_count += 1
                if error_count > 3:
                    break
                await asyncio.sleep(60)
                continue

            client = acc["client"]
            # Проверяем подключение
            try:
                await client.get_me()
            except Exception as e:
                logger.warning(f"⚠️ Клиент {phone} не отвечает: {e}")
                await reconnect_account(user_id, phone)
                await asyncio.sleep(30)
                continue

            # Получаем список чатов для рассылки
            target_chats = await get_target_chats(client, acc)
            if not target_chats:
                logger.warning(f"Нет доступных чатов для {phone}")
                await asyncio.sleep(60)
                continue

            # Для каждого чата определяем текст и интервал
            for chat_id in target_chats:
                if not acc.get("running", False):
                    break

                # Определяем текст и интервал для этого чата
                if acc["mode"] == "simple":
                    # Для простого режима: можно задать индивидуальные настройки для чата/папки
                    text = None
                    interval = None
                    # Проверяем настройки для конкретного чата
                    chat_settings = acc.get("chat_settings", {})
                    if str(chat_id) in chat_settings:
                        text = chat_settings[str(chat_id)].get("text", acc.get("text", ""))
                        interval = chat_settings[str(chat_id)].get("interval", acc.get("interval", 3600))
                    else:
                        # Проверяем, в какой папке состоит чат
                        folder_name = None
                        for fname, chats in acc.get("folders", {}).items():
                            if chat_id in chats:
                                folder_name = fname
                                break
                        if folder_name and folder_name in acc.get("folder_settings", {}):
                            text = acc["folder_settings"][folder_name].get("text", acc.get("text", ""))
                            interval = acc["folder_settings"][folder_name].get("interval", acc.get("interval", 3600))
                        else:
                            text = acc.get("text", "")
                            interval = acc.get("interval", 3600)
                else:  # безопасный режим
                    # Выбираем случайное сообщение из 5, не повторяя предыдущее для этого чата
                    messages = acc.get("safe_messages", [])
                    if not messages:
                        logger.error(f"Нет сообщений для безопасного режима {phone}")
                        break
                    prev_idx = last_index.get(str(chat_id), -1)
                    available = [i for i in range(len(messages)) if i != prev_idx]
                    if not available:
                        available = list(range(len(messages)))
                    idx = random.choice(available)
                    last_index[str(chat_id)] = idx
                    text = messages[idx]
                    # Интервал: базовый + случайная добавка до 420 секунд (7 минут)
                    base = acc.get("safe_base_interval", 600)
                    interval = random.randint(base, base + 420)

                # Отправляем сообщение
                try:
                    await client.send_message(chat_id, text)
                    sent_stats[chat_id] = sent_stats.get(chat_id, 0) + 1
                    # Обновляем статус каждые 5 отправок
                    if len(sent_stats) % 5 == 0 and status_msg:
                        new_text = f"🚀 Рассылка {phone} активна\n"
                        new_text += f"📊 Цикл #{cycle_count + 1}\n"
                        new_text += f"📨 Отправлено в {len(sent_stats)} чатов\n"
                        await status_msg.edit_text(new_text)
                except FloodWait as e:
                    logger.warning(f"FloodWait {e.value} сек для {phone}")
                    await asyncio.sleep(e.value)
                except (PeerIdInvalid, Forbidden):
                    logger.warning(f"Чат {chat_id} недоступен, пропускаем")
                except Exception as e:
                    logger.error(f"Ошибка отправки в чат {chat_id}: {e}")

                # Ждём интервал перед следующим чатом (даже если ошибка)
                await asyncio.sleep(interval)

            cycle_count += 1
            error_count = 0
            # После завершения цикла по всем чатам – пауза перед новым циклом? 
            # В оригинале был интервал между циклами, но с индивидуальными интервалами между чатами
            # можно сделать небольшую паузу, чтобы не нагружать аккаунт
            await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Ошибка в цикле рассылки {phone}: {e}")
            error_count += 1
            if error_count > 5:
                logger.error(f"❌ Слишком много ошибок для {phone}, останавливаем рассылку")
                acc["running"] = False
                break
            await asyncio.sleep(60)

    if acc.get("mode") == "safe" and last_index is not None:
        acc["last_message_index"] = last_index
        save_users()

    if status_msg:
        try:
            await status_msg.edit_text(f"✅ Рассылка {phone} завершена.\n📊 Всего циклов: {cycle_count}")
        except:
            pass
    logger.info(f"✅ Рассылка {phone} остановлена. Циклов: {cycle_count}")

# ---------- ОСТАЛЬНЫЕ ХЕНДЛЕРЫ (без изменений, кроме добавления новых пунктов меню) ----------
# Здесь идут стандартные функции: keep_alive, parse_key_with_username, check_key_binding, load_user_sessions,
# schedule_reconnect, reconnect_account, а также все обработчики команд, которые не изменились.
# Для краткости я приведу их в сокращённом виде, но в финальном коде они должны быть полностью.
# Ниже даны только изменённые/добавленные части. Полный код выложу одним блоком.

# (Пропущены функции, которые не менялись: keep_alive, parse_key_with_username, check_key_binding,
#  load_user_sessions, schedule_reconnect, reconnect_account, а также обработчики start, enter_key_prompt,
#  cancel_input, handle_all_messages, handle_key_input, handle_bind_key, handle_phone_input,
#  handle_code_input, handle_password_input, handle_text_input, handle_interval_input,
#  handle_interval_confirm, handle_menu_commands (кроме новых пунктов), finalize_user_account, shutdown)

# Добавим в handle_menu_commands обработку новых кнопок:
async def handle_menu_commands(c, m):
    user_id = m.from_user.id
    text = m.text
    if not check_access(user_id):
        await m.reply("❌ У вас нет доступа. Используйте /start для входа.")
        return

    # --- Новые пункты меню ---
    if text == "📁 Управление чатами":
        await manage_chats_menu(c, m)
        return
    if text == "🛡 Безопасный режим":
        await setup_safe_mode(c, m)
        return
    # Остальные старые пункты (Добавить аккаунт, Мои аккаунты, и т.д.) оставляем как есть
    # ...

# В handle_all_messages нужно добавить обработку новых шагов (select_account_for_chats, select_account_for_safe,
# add_chat_wait, create_folder_wait, select_folder_for_add, add_chat_to_folder, delete_choice, delete_chat_wait,
# delete_folder_wait, safe_messages_input, safe_interval_input и т.д.). Полный код ниже.

# Для экономии места я предоставлю **полный готовый файл** отдельным сообщением. 
# Он слишком велик для этого ответа, поэтому я выложу его в следующем сообщении.
# ========== ОСТАВШИЕСЯ ФУНКЦИИ (keep_alive, reconnect, загрузка сессий и т.д.) ==========

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
            logger.warning(f"⚠️ Keep-alive ошибка для {phone}: {e}")
            if key in keep_alive_tasks:
                await schedule_reconnect(user_id, phone)
            break

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

async def load_user_sessions():
    sessions_dir = os.path.join(WORK_DIR, 'sessions')
    os.makedirs(sessions_dir, exist_ok=True)
    loaded_count = 0
    for user_id, user_data in users_data.items():
        for phone, acc_data in user_data["accounts"].items():
            try:
                session_name = acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}"))
                session_file = f"{session_name}.session"
                if os.path.exists(session_file):
                    client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=WORK_DIR)
                    async def on_disconnect(cl, user_id=user_id, phone=phone):
                        await schedule_reconnect(user_id, phone)
                    client.add_handler(DisconnectHandler(on_disconnect))
                    await client.start()
                    acc_data["client"] = client
                    task_key = f"{user_id}_{phone}"
                    if task_key in keep_alive_tasks:
                        keep_alive_tasks[task_key].cancel()
                    keep_alive_tasks[task_key] = asyncio.create_task(keep_alive(user_id, phone, client))
                    loaded_count += 1
                    logger.info(f"✅ Сессия {phone} загружена")
                else:
                    logger.warning(f"⚠️ Файл сессии {session_file} не найден")
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки сессии {phone}: {e}")
    logger.info(f"✅ Загружено {loaded_count} активных сессий")
    return loaded_count

async def schedule_reconnect(user_id, phone):
    key = f"{user_id}_{phone}"
    if key in reconnect_tasks:
        reconnect_tasks[key].cancel()
    if key in keep_alive_tasks:
        keep_alive_tasks[key].cancel()
    async def reconnect_with_delay():
        await asyncio.sleep(30)
        try:
            await reconnect_account(user_id, phone)
        except Exception as e:
            logger.error(f"❌ Ошибка переподключения {phone}: {e}")
    reconnect_tasks[key] = asyncio.create_task(reconnect_with_delay())
    logger.info(f"⏰ Запланировано переподключение {phone} через 30 сек")

async def reconnect_account(user_id, phone):
    if user_id not in users_data or phone not in users_data[user_id]["accounts"]:
        return
    acc_data = users_data[user_id]["accounts"][phone]
    session_name = acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}"))
    try:
        logger.info(f"🔄 Попытка переподключения {phone}")
        client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=WORK_DIR)
        async def on_disconnect(cl, user_id=user_id, phone=phone):
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
        logger.info(f"✅ Аккаунт {phone} успешно переподключен")
    except Exception as e:
        logger.error(f"❌ Не удалось переподключить {phone}: {e}")
        await schedule_reconnect(user_id, phone)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@bot.on_message(filters.command("start"))
async def start(c, m):
    user_id = m.from_user.id
    username = m.from_user.username or m.from_user.first_name
    if check_access(user_id):
        accounts_count = len(users_data[user_id]["accounts"])
        expires = users_data[user_id]["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        bound_info = f"🔗 Привязан к: @{users_data[user_id]['bound_username']}\n" if users_data[user_id].get("bound_username") else ""
        await m.reply(
            f"👋 Добро пожаловать в личный кабинет, {username}!\n\n"
            f"📊 Ваша статистика:\n"
            f"📱 Аккаунтов: {accounts_count}/{MAX_ACCOUNTS_PER_USER}\n"
            f"📅 Доступ до: {expires.strftime('%d.%m.%Y')}\n"
            f"{bound_info}"
            f"👑 Статус: {'Администратор' if is_admin(user_id) else 'Пользователь'}",
            reply_markup=get_user_main_keyboard(user_id)
        )
    else:
        await m.reply(
            "🔐 Доступ ограничен\n\nДля использования бота введите одноразовый ключ доступа.\n"
            "Ключ можно ввести в формате:\n• обычный ключ: KEY123\n• привязанный ключ: KEY123-@username\n\n"
            "Нажмите кнопку ниже чтобы ввести ключ.",
            reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True)
        )

@bot.on_message(filters.regex("🔑 Ввести ключ доступа"))
async def enter_key_prompt(c, m):
    user_id = m.from_user.id
    if check_access(user_id):
        return await m.reply("✅ У вас уже есть активный доступ!\nИспользуйте /start для входа в личный кабинет.", reply_markup=get_user_main_keyboard(user_id))
    temp_auth[user_id] = {"step": "enter_key", "user_id": user_id}
    await m.reply(
        "🔑 Пожалуйста, введите ваш одноразовый ключ доступа:\n\nФорматы:\n• KEY123 - обычный ключ\n• KEY123-@username - ключ для конкретного пользователя",
        reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True)
    )

@bot.on_message(filters.regex("🔙 Отмена"))
async def cancel_input(c, m):
    user_id = m.from_user.id
    temp_auth.pop(user_id, None)
    await m.reply("❌ Ввод отменен.\nИспользуйте /start для возврата в главное меню.", reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True))

@bot.on_message(filters.text & filters.private)
async def handle_all_messages(c, m):
    user_id = m.from_user.id
    text = m.text
    if user_id in temp_auth:
        step = temp_auth[user_id].get("step")
        # Обработка шагов для управления чатами и безопасного режима
        if step == "select_account_for_chats":
            try:
                idx = int(text.split('.')[0]) - 1
                phone = temp_auth[user_id]["accounts"][idx]
                await show_chat_management(c, m, phone)
            except:
                await m.reply("Выберите номер из списка.")
            return
        elif step == "select_account_for_safe":
            try:
                idx = int(text.split('.')[0]) - 1
                phone = temp_auth[user_id]["accounts"][idx]
                await configure_safe_mode(c, m, phone)
            except:
                await m.reply("Выберите номер из списка.")
            return
        elif step == "add_chat_wait":
            phone = temp_auth[user_id]["phone"]
            await handle_add_chat(c, m, phone, text)
            return
        elif step == "create_folder_wait":
            phone = temp_auth[user_id]["phone"]
            await handle_create_folder(c, m, phone, text)
            return
        elif step == "select_folder_for_add":
            phone = temp_auth[user_id]["phone"]
            await handle_select_folder_for_add(c, m, phone, text)
            return
        elif step == "add_chat_to_folder":
            phone = temp_auth[user_id]["phone"]
            folder = temp_auth[user_id]["folder_name"]
            await handle_add_chat_to_folder(c, m, phone, folder, text)
            return
        elif step == "delete_choice":
            phone = temp_auth[user_id]["phone"]
            await handle_delete_choice(c, m, phone, text)
            return
        elif step == "delete_chat_wait":
            phone = temp_auth[user_id]["phone"]
            await handle_delete_chat(c, m, phone, text)
            return
        elif step == "delete_folder_wait":
            phone = temp_auth[user_id]["phone"]
            await handle_delete_folder(c, m, phone, text)
            return
        elif step == "safe_messages_input":
            phone = temp_auth[user_id]["phone"]
            await handle_safe_messages_input(c, m, phone)
            return
        elif step == "safe_interval_input":
            phone = temp_auth[user_id]["phone"]
            await handle_safe_interval_input(c, m, phone)
            return
        elif step == "chat_management":
            if await handle_chat_management(c, m):
                return
        elif step == "safe_mode_config":
            if await handle_safe_mode_config(c, m):
                return
        # Старые шаги
        elif step == "enter_key":
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
    await handle_menu_commands(c, m)

# ---------- Обработчики для старых команд (добавить аккаунт, настройки текста и т.д.) ----------
async def handle_key_input(c, m):
    user_id = m.from_user.id
    raw_key = m.text.strip()
    username = m.from_user.username or ""
    key, bound_username = parse_key_with_username(raw_key)
    current_keys = load_keys()
    if key not in current_keys:
        await m.reply("❌ Неверный ключ доступа!")
        return
    can_use, msg = check_key_binding(key, user_id, username)
    if not can_use:
        await m.reply(msg)
        return
    for uid, ud in users_data.items():
        if ud["key_used"] == key:
            if uid == user_id:
                await m.reply("❌ Вы уже использовали этот ключ!")
            else:
                await m.reply("❌ Этот ключ уже был использован другим пользователем!")
            return
    owner = current_keys[key]
    is_admin_key = "ADMIN" in key or "админ" in owner.lower() or key == "ADMINKEY999"
    expires = datetime.now() + timedelta(days=KEY_EXPIRY_DAYS)
    users_data[user_id] = {
        "expires": expires.isoformat(),
        "key_used": key,
        "is_admin": is_admin_key,
        "username": username or m.from_user.first_name,
        "bound_username": bound_username if bound_username else "",
        "accounts": {}
    }
    if save_users():
        role = "👑 Администратор" if is_admin_key else "👤 Пользователь"
        bound_info = f"🔗 Привязан к: @{bound_username}\n" if bound_username else ""
        await m.reply(
            f"✅ Доступ предоставлен!\n\n{role}\nКлюч: {key}\nВладелец ключа: {owner}\n{bound_info}"
            f"Срок действия до: {expires.strftime('%d.%m.%Y %H:%M')}\n\nИспользуйте /start для входа в личный кабинет",
            reply_markup=get_user_main_keyboard(user_id)
        )
    else:
        await m.reply("❌ Ошибка при сохранении данных. Попробуйте позже.")
    temp_auth.pop(user_id, None)

async def handle_phone_input(c, m):
    user_id = m.from_user.id
    phone = m.text
    try:
        session_name = os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}")
        client = Client(session_name, api_id=API_ID, api_hash=API_HASH, phone_number=phone, workdir=WORK_DIR)
        await client.connect()
        sent = await client.send_code(phone)
        temp_auth[user_id].update({"client": client, "phone": phone, "code_hash": sent.phone_code_hash, "step": "code"})
        await m.reply("🔢 Введите код из СМС:")
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        temp_auth.pop(user_id, None)

async def handle_code_input(c, m):
    user_id = m.from_user.id
    data = temp_auth[user_id]
    try:
        await data["client"].sign_in(data["phone"], data["code_hash"], m.text)
        await finalize_user_account(user_id, data, m)
    except Exception as e:
        if "SESSION_PASSWORD_NEEDED" in str(e):
            data["step"] = "password"
            await m.reply("🔐 Введите облачный пароль (2FA):")
        else:
            await m.reply(f"❌ Ошибка: {e}")
            temp_auth.pop(user_id, None)

async def handle_password_input(c, m):
    user_id = m.from_user.id
    data = temp_auth[user_id]
    try:
        await data["client"].check_password(m.text)
        await finalize_user_account(user_id, data, m)
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        temp_auth.pop(user_id, None)

async def handle_text_input(c, m):
    user_id = m.from_user.id
    if user_id not in users_data:
        await m.reply("❌ Пользователь не найден")
        temp_auth.pop(user_id, None)
        return
    for acc in users_data[user_id]["accounts"].values():
        acc["text"] = m.text
    save_users()
    await m.reply("✅ Текст рассылки обновлен для всех ваших аккаунтов.")
    temp_auth.pop(user_id)

async def handle_interval_input(c, m):
    user_id = m.from_user.id
    data = temp_auth[user_id]
    try:
        interval = int(m.text)
        if interval < 10:
            await m.reply("⚠️ Интервал меньше 10 секунд может привести к бану. Продолжить? (да/нет)")
            data["step"] = "confirm_interval"
            data["temp_interval"] = interval
        else:
            for acc in users_data[user_id]["accounts"].values():
                acc["interval"] = interval
            save_users()
            await m.reply(f"✅ Интервал установлен: {interval} сек.")
            temp_auth.pop(user_id)
    except ValueError:
        await m.reply("❌ Пожалуйста, введите число!")

async def handle_interval_confirm(c, m):
    user_id = m.from_user.id
    data = temp_auth[user_id]
    if m.text.lower() in ["да", "yes", "д", "y"]:
        for acc in users_data[user_id]["accounts"].values():
            acc["interval"] = data["temp_interval"]
        save_users()
        await m.reply(f"✅ Интервал установлен: {data['temp_interval']} сек. (Будьте осторожны!)")
    else:
        await m.reply("❌ Установка интервала отменена.")
    temp_auth.pop(user_id)

async def handle_bind_key(c, m):
    user_id = m.from_user.id
    text = m.text.strip()
    key, username = parse_key_with_username(text)
    if not username:
        await m.reply("❌ Неверный формат! Используйте: ключ-@username\nНапример: KEY123-@durov")
        return
    current_keys = load_keys()
    current_keys[key] = f"@{username}"
    if save_keys(current_keys):
        await m.reply(f"✅ Ключ успешно привязан!\n\n🔑 Ключ: {key}\n👤 Привязан к: @{username}\n\nТеперь этот ключ может использовать только пользователь @{username}")
    else:
        await m.reply("❌ Ошибка при сохранении ключа")
    temp_auth.pop(user_id)

async def handle_menu_commands(c, m):
    user_id = m.from_user.id
    text = m.text
    if not check_access(user_id):
        await m.reply("❌ У вас нет доступа. Используйте /start для входа.")
        return

    # Новые команды
    if text == "📁 Управление чатами":
        await manage_chats_menu(c, m)
        return
    if text == "🛡 Безопасный режим":
        await setup_safe_mode(c, m)
        return

    # Старые команды
    if text == "➕ Добавить аккаунт":
        if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
            await m.reply(f"❌ Вы достигли лимита аккаунтов ({MAX_ACCOUNTS_PER_USER}).")
        else:
            temp_auth[user_id] = {"step": "phone", "user_id": user_id}
            await m.reply("📱 Введите номер телефона в международном формате (например, +380123456789):")
    elif text == "📱 Мои аккаунты":
        accounts = users_data[user_id]["accounts"]
        if not accounts:
            await m.reply("📱 У вас нет добавленных аккаунтов.")
        else:
            acc_list = []
            for i, (phone, data) in enumerate(accounts.items(), 1):
                status = "🟢 АКТИВЕН" if data.get("running", False) else "🔴 ОСТАНОВЛЕН"
                client_status = "✅" if "client" in data else "❌"
                acc_list.append(f"{i}. {phone}\n   Статус: {status} | Клиент: {client_status}\n   Режим: {data.get('mode', 'simple')}\n   📝 Текст: {data.get('text', '')[:30]}...\n   ⏱ Интервал: {data.get('interval', 3600)} сек.")
            await m.reply("📱 Ваши аккаунты:\n\n" + "\n\n".join(acc_list))
    elif text == "👤 Мой кабинет":
        user_data = users_data[user_id]
        accounts = user_data["accounts"]
        expires = user_data["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        total_accounts = len(accounts)
        running_accounts = sum(1 for acc in accounts.values() if acc.get("running", False))
        active_clients = sum(1 for acc in accounts.values() if "client" in acc)
        bound_info = f"🔗 Привязан к: @{user_data['bound_username']}\n" if user_data.get('bound_username') else ""
        accounts_info = ""
        for phone, acc in accounts.items():
            status = "🟢" if acc.get("running", False) else "🔴"
            client = "✅" if "client" in acc else "❌"
            accounts_info += f"{status}{client} {phone}\n   Режим: {acc.get('mode', 'simple')}\n"
        await m.reply(
            f"👤 Личный кабинет\n\n🆔 ID: {user_id}\n👤 Имя: {user_data.get('username', 'Не указано')}\n{bound_info}"
            f"📅 Доступ до: {expires.strftime('%d.%m.%Y')}\n🔑 Использован ключ: {user_data['key_used']}\n👑 Админ: {'Да' if is_admin(user_id) else 'Нет'}\n\n"
            f"📊 Статистика аккаунтов:\n📱 Всего: {total_accounts}/{MAX_ACCOUNTS_PER_USER}\n✅ Активных клиентов: {active_clients}\n🟢 Активных рассылок: {running_accounts}\n\n📋 Ваши аккаунты:\n{accounts_info}"
        )
    elif text == "🚀 Старт рассылки":
        accounts = users_data[user_id]["accounts"]
        if not accounts:
            await m.reply("❌ У вас нет добавленных аккаунтов!")
        else:
            started = 0
            for phone, d in accounts.items():
                if not d.get("running", False):
                    if "client" not in d:
                        await reconnect_account(user_id, phone)
                        await asyncio.sleep(2)
                    if "client" in d:
                        d["running"] = True
                        asyncio.create_task(spam_cycle(user_id, phone, d, m))
                        started += 1
            await m.reply(f"🚀 Запущено рассылок: {started}")
    elif text == "🛑 Стоп рассылки":
        accounts = users_data[user_id]["accounts"]
        stopped = 0
        for d in accounts.values():
            if d.get("running", False):
                d["running"] = False
                stopped += 1
        save_users()
        await m.reply(f"🛑 Остановлено рассылок: {stopped}")
    elif text == "⚙️ Настройки текста":
        if not users_data[user_id]["accounts"]:
            await m.reply("❌ Сначала добавьте аккаунт!")
        else:
            temp_auth[user_id] = {"step": "text", "user_id": user_id}
            await m.reply("✏️ Введите новый текст для рассылки (будет применён ко всем аккаунтам в простом режиме):")
    elif text == "⏱ Настройки интервала":
        if not users_data[user_id]["accounts"]:
            await m.reply("❌ Сначала добавьте аккаунт!")
        else:
            temp_auth[user_id] = {"step": "interval", "user_id": user_id}
            await m.reply("⏱ Введите интервал между циклами рассылки (в секундах):")
    elif text == "🔑 Информация о доступе":
        data = users_data[user_id]
        expires = data["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        days_left = (expires - datetime.now()).days
        bound_info = f"🔗 Привязан к: @{data['bound_username']}\n" if data.get('bound_username') else ""
        await m.reply(
            f"🔑 Информация о доступе:\n\n✅ Доступ активен\n🔑 Ключ: {data['key_used']}\n{bound_info}"
            f"📅 Истекает: {expires.strftime('%d.%m.%Y')}\n⏳ Осталось дней: {days_left}\n👑 Права: {'Администратор' if is_admin(user_id) else 'Пользователь'}"
        )
    elif text == "💾 Сохранить настройки":
        if save_users():
            await m.reply("✅ Настройки сохранены")
        else:
            await m.reply("❌ Ошибка при сохранении")
    elif text == "📂 Загрузить настройки":
        if load_users():
            await m.reply("✅ Настройки загружены")
        else:
            await m.reply("❌ Ошибка при загрузке")
    # Админские команды
    elif is_admin(user_id):
        if text == "🔑 Управление ключами":
            current_keys = load_keys()
            keys_list = "📋 Доступные одноразовые ключи:\n\n"
            for key, owner in current_keys.items():
                used = False
                used_by = ""
                bound_to = " (привязан)" if isinstance(owner, str) and owner.startswith('@') else ""
                for uid, ud in users_data.items():
                    if ud["key_used"] == key:
                        used = True
                        used_by = f" (использован: {ud.get('username', uid)})"
                        break
                status = "❌" if used else "✅"
                keys_list += f"{status} {key} - {owner}{bound_to}{used_by}\n"
            keys_list += "\n\n📝 Нажмите кнопку ниже для привязки ключа"
            await m.reply(keys_list, reply_markup=ReplyKeyboardMarkup([["🔗 Привязать ключ к юзеру", "🔙 Назад"]], resize_keyboard=True))
        elif text == "🔗 Привязать ключ к юзеру":
            temp_auth[user_id] = {"step": "bind_key", "user_id": user_id}
            await m.reply("🔗 Введите ключ и username в формате:\n`ключ-@username`\n\nНапример: `KEY123-@durov`", reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True))
        elif text == "👥 Все пользователи":
            if not users_data:
                await m.reply("📭 Нет активных пользователей")
            else:
                users_list = "👥 Все пользователи:\n\n"
                for uid, data in users_data.items():
                    expires = data["expires"]
                    if isinstance(expires, str):
                        expires = datetime.fromisoformat(expires)
                    accounts_count = len(data["accounts"])
                    bound_info = f" (привязан к @{data['bound_username']})" if data.get('bound_username') else ""
                    users_list += f"🆔 {uid}\n👤 {data.get('username', 'Нет username')}{bound_info}\n📱 Аккаунтов: {accounts_count}\n📅 Доступ до: {expires.strftime('%d.%m.%Y')}\n🔑 Ключ: {data['key_used']}\n👑 Админ: {'Да' if data['is_admin'] else 'Нет'}\n\n"
                if len(users_list) > 4000:
                    for i in range(0, len(users_list), 4000):
                        await m.reply(users_list[i:i+4000])
                else:
                    await m.reply(users_list)
        elif text == "📊 Статистика":
            total_users = len(users_data)
            total_accounts = sum(len(data["accounts"]) for data in users_data.values())
            total_running = sum(sum(1 for acc in data["accounts"].values() if acc.get("running", False)) for data in users_data.values())
            current_keys = load_keys()
            total_keys = len(current_keys)
            used_keys = sum(1 for ud in users_data.values() if ud["key_used"] in current_keys)
            bound_keys = sum(1 for v in current_keys.values() if isinstance(v, str) and v.startswith('@'))
            await m.reply(
                f"📊 Общая статистика бота:\n\n👥 Пользователей: {total_users}\n📱 Всего аккаунтов: {total_accounts}\n"
                f"🟢 Активных рассылок: {total_running}\n🔑 Всего ключей: {total_keys}\n🔗 Привязанных ключей: {bound_keys}\n"
                f"✅ Использовано ключей: {used_keys}\n📦 Осталось ключей: {total_keys - used_keys}"
            )

async def finalize_user_account(uid, data, m):
    user_id = data["user_id"]
    phone = data["phone"]
    session_name = os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}")
    client = data["client"]
    async def on_disconnect(cl, user_id=user_id, phone=phone):
        await schedule_reconnect(user_id, phone)
    client.add_handler(DisconnectHandler(on_disconnect))
    task_key = f"{user_id}_{phone}"
    if task_key in keep_alive_tasks:
        keep_alive_tasks[task_key].cancel()
    keep_alive_tasks[task_key] = asyncio.create_task(keep_alive(user_id, phone, client))
    users_data[user_id]["accounts"][phone] = {
        "client": client,
        "mode": "simple",
        "text": "Привет! Это рассылка.",
        "interval": 3600,
        "safe_messages": [],
        "safe_base_interval": 600,
        "chats": [],
        "folders": {},
        "chat_settings": {},
        "folder_settings": {},
        "last_message_index": {},
        "running": False,
        "added_date": datetime.now().isoformat(),
        "session_name": session_name
    }
    await m.reply(f"✅ Аккаунт {phone} успешно добавлен!")
    if uid in temp_auth:
        temp_auth.pop(uid)
    save_users()

async def shutdown(sig=None):
    logger.info("🛑 Получен сигнал завершения, останавливаю бота...")
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
    os.makedirs(os.path.join(WORK_DIR, "sessions"), exist_ok=True)
    os.makedirs(os.path.join(WORK_DIR, "user_settings"), exist_ok=True)
    os.makedirs(os.path.join(WORK_DIR, "bot_session"), exist_ok=True)
    load_users()
    loop = asyncio.get_event_loop()
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
                logger.error(f"❌ Volume НЕ доступен для записи: {e}")
        loaded = await load_user_sessions()
        logger.info(f"🔑 Доступные ключи: {list(load_keys().keys())}")
        logger.info(f"👥 Пользователей: {len(users_data)}")
        logger.info(f"📱 Активных сессий: {loaded}")
    loop.run_until_complete(startup())
    logger.info("🤖 Бот запущен и готов к работе")
    try:
        bot.run()
    except (KeyboardInterrupt, SystemExit):
        loop.run_until_complete(shutdown())
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        loop.run_until_complete(shutdown())
        
