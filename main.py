import asyncio
import os
import json
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.types import ReplyKeyboardMarkup
from pyrogram.errors import PeerIdInvalid, Forbidden

# --- КОНФИГ ---
API_ID = 30032542
API_HASH = "ce646da1307fb452305d49f9bb8751ca"
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8711240311:AAHy5FzxQ7P0MpSm3Bv7xfoYDa9kVlwAb5w')

# === НАСТРОЙКА ОДНОРАЗОВЫХ КЛЮЧЕЙ ===
ONE_TIME_KEYS = {
    "SECRET123": "Пользователь 1",
    "ABCDEF456": "Пользователь 2",
    "ADMINKEY999": "Администратор",
}

KEY_EXPIRY_DAYS = 30
MAX_ACCOUNTS_PER_USER = 3  # Максимум аккаунтов на одного пользователя
# ====================================

bot = Client("manager_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Структура данных пользователей
users_data = {}
temp_auth = {}
settings_file = "bot_settings.json"
users_file = "bot_users.json"

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ---

def save_users():
    """Сохраняет данные пользователей в файл"""
    users_to_save = {}
    for uid, data in users_data.items():
        users_to_save[str(uid)] = {
            "expires": data["expires"].isoformat(),
            "key_used": data["key_used"],
            "is_admin": data["is_admin"],
            "username": data.get("username", ""),
            "accounts": {
                phone: {
                    "text": acc["text"],
                    "interval": acc["interval"],
                    "running": False,
                    "added_date": acc["added_date"].isoformat() if isinstance(acc["added_date"], datetime) else acc["added_date"]
                }
                for phone, acc in data["accounts"].items()
            }
        }
    
    with open(users_file, 'w', encoding='utf-8') as f:
        json.dump(users_to_save, f, ensure_ascii=False, indent=2)

def load_users():
    """Загружает данные пользователей из файла"""
    global users_data
    try:
        if os.path.exists(users_file):
            with open(users_file, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
            
            for uid, data in loaded_data.items():
                uid = int(uid)
                expires = datetime.fromisoformat(data["expires"])
                
                if expires > datetime.now():
                    accounts = {}
                    for phone, acc_data in data.get("accounts", {}).items():
                        accounts[phone] = {
                            "text": acc_data["text"],
                            "interval": acc_data["interval"],
                            "running": False,
                            "added_date": datetime.fromisoformat(acc_data["added_date"]) if isinstance(acc_data.get("added_date"), str) else datetime.now()
                        }
                    
                    users_data[uid] = {
                        "expires": expires,
                        "key_used": data["key_used"],
                        "is_admin": data["is_admin"],
                        "username": data.get("username", ""),
                        "accounts": accounts
                    }
            
            print(f"✅ Загружено {len(users_data)} пользователей")
    except Exception as e:
        print(f"❌ Ошибка загрузки пользователей: {e}")

async def load_user_sessions():
    """Загружает сессии для всех пользователей"""
    if not os.path.exists("sessions"):
        os.makedirs("sessions")
    
    for filename in os.listdir("sessions"):
        if filename.endswith(".session"):
            try:
                parts = filename.replace(".session", "").split("_")
                if len(parts) >= 2:
                    phone = parts[0]
                    user_id = int(parts[1])
                    
                    if user_id in users_data:
                        client = Client(f"sessions/{filename.replace('.session', '')}", api_id=API_ID, api_hash=API_HASH)
                        await client.start()
                        
                        if phone in users_data[user_id]["accounts"]:
                            users_data[user_id]["accounts"][phone]["client"] = client
                        else:
                            users_data[user_id]["accounts"][phone] = {
                                "client": client,
                                "text": "Привет! Это рассылка.",
                                "interval": 3600,
                                "running": False,
                                "added_date": datetime.now()
                            }
                        
                        print(f"✅ Сессия {phone} загружена для пользователя {user_id}")
            except Exception as e:
                print(f"❌ Ошибка загрузки сессии {filename}: {e}")

def check_access(user_id):
    """Проверяет доступ пользователя"""
    if user_id in users_data:
        user_data = users_data[user_id]
        if user_data["expires"] > datetime.now():
            return True
        else:
            del users_data[user_id]
            save_users()
    return False

def is_admin(user_id):
    """Проверяет, является ли пользователь администратором"""
    if user_id in users_data:
        return users_data[user_id].get("is_admin", False)
    return False

def get_user_main_keyboard(user_id):
    """Возвращает клавиатуру для конкретного пользователя"""
    if is_admin(user_id):
        return ReplyKeyboardMarkup([
            ["➕ Добавить аккаунт", "📱 Мои аккаунты"],
            ["👤 Мой кабинет", "🚀 Старт рассылки"],
            ["🛑 Стоп рассылки", "⚙️ Настройки текста"],
            ["⏱ Настройки интервала", "💾 Сохранить настройки"],
            ["📂 Загрузить настройки", "🔑 Управление ключами"],
            ["👥 Все пользователи", "📊 Статистика"]
        ], resize_keyboard=True)
    else:
        return ReplyKeyboardMarkup([
            ["➕ Добавить аккаунт", "📱 Мои аккаунты"],
            ["👤 Мой кабинет", "🚀 Старт рассылки"],
            ["🛑 Стоп рассылки", "⚙️ Настройки текста"],
            ["⏱ Настройки интервала", "💾 Сохранить настройки"],
            ["📂 Загрузить настройки", "🔑 Информация о доступе"]
        ], resize_keyboard=True)

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С НАСТРОЙКАМИ ПОЛЬЗОВАТЕЛЯ ---

def save_user_settings(user_id):
    """Сохраняет настройки конкретного пользователя"""
    if user_id not in users_data:
        return
    
    user_settings = {
        "text": list(users_data[user_id]["accounts"].values())[0]["text"] if users_data[user_id]["accounts"] else "Привет! Это рассылка.",
        "interval": list(users_data[user_id]["accounts"].values())[0]["interval"] if users_data[user_id]["accounts"] else 3600,
        "accounts": {}
    }
    
    for phone, data in users_data[user_id]["accounts"].items():
        user_settings["accounts"][phone] = {
            "text": data["text"],
            "interval": data["interval"]
        }
    
    user_settings_dir = f"user_settings/{user_id}"
    if not os.path.exists(user_settings_dir):
        os.makedirs(user_settings_dir)
    
    with open(f"{user_settings_dir}/settings.json", 'w', encoding='utf-8') as f:
        json.dump(user_settings, f, ensure_ascii=False, indent=2)

def load_user_settings(user_id):
    """Загружает настройки конкретного пользователя"""
    try:
        settings_file = f"user_settings/{user_id}/settings.json"
        if os.path.exists(settings_file):
            with open(settings_file, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            
            if user_id in users_data:
                for phone, data in users_data[user_id]["accounts"].items():
                    if phone in settings.get("accounts", {}):
                        acc_settings = settings["accounts"][phone]
                        data["text"] = acc_settings.get("text", settings.get("text", "Привет! Это рассылка."))
                        data["interval"] = acc_settings.get("interval", settings.get("interval", 3600))
            
            return True
    except Exception as e:
        print(f"❌ Ошибка загрузки настроек пользователя {user_id}: {e}")
    return False

# --- ФУНКЦИИ ДЛЯ РАССЫЛКИ ---

async def spam_cycle(user_id, phone, data, message):
    """Фоновый процесс рассылки для конкретного пользователя"""
    status_msg = await message.reply(f"🚀 Запуск рассылки для {phone}...")
    sent_chats = []

    while data["running"]:
        try:
            async for dialog in data["client"].get_dialogs():
                if not data["running"]: 
                    break
                if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    try:
                        await data["client"].send_message(dialog.chat.id, data["text"])
                       
                        sent_chats.append(dialog.chat.title)
                        new_text = f"🚀 Рассылка {phone} активна\n\nПоследние чаты:\n" + "\n".join(sent_chats[-10:])
                        await status_msg.edit_text(new_text)
                       
                        await asyncio.sleep(0.2)
                    except (PeerIdInvalid, Forbidden): 
                        continue
                    except Exception: 
                        continue
            await asyncio.sleep(data["interval"])
        except Exception:
            await asyncio.sleep(60)
   
    await status_msg.edit_text(f"✅ Рассылка {phone} завершена.\nВсего чатов: {len(sent_chats)}")

# --- ХЕНДЛЕРЫ ---

@bot.on_message(filters.command("start"))
async def start(c, m):
    user_id = m.from_user.id
    username = m.from_user.username or m.from_user.first_name
    
    if check_access(user_id):
        accounts_count = len(users_data[user_id]["accounts"])
        await m.reply(
            f"👋 Добро пожаловать в личный кабинет, {username}!\n\n"
            f"📊 Ваша статистика:\n"
            f"📱 Аккаунтов: {accounts_count}/{MAX_ACCOUNTS_PER_USER}\n"
            f"📅 Доступ до: {users_data[user_id]['expires'].strftime('%d.%m.%Y')}\n"
            f"👑 Статус: {'Администратор' if is_admin(user_id) else 'Пользователь'}",
            reply_markup=get_user_main_keyboard(user_id)
        )
    else:
        await m.reply(
            "🔐 Доступ ограничен\n\n"
            "Для использования бота введите одноразовый ключ доступа.\n"
            "Нажмите кнопку ниже чтобы ввести ключ.",
            reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True)
        )

# НОВЫЙ ХЕНДЛЕР: Обработка нажатия кнопки "🔑 Ввести ключ доступа"
@bot.on_message(filters.regex("🔑 Ввести ключ доступа"))
async def enter_key_prompt(c, m):
    user_id = m.from_user.id
    
    # Если пользователь уже имеет доступ, сообщаем об этом
    if check_access(user_id):
        return await m.reply(
            "✅ У вас уже есть активный доступ!\n"
            "Используйте /start для входа в личный кабинет.",
            reply_markup=get_user_main_keyboard(user_id)
        )
    
    # Устанавливаем шаг ввода ключа
    temp_auth[user_id] = {"step": "enter_key", "user_id": user_id}
    await m.reply(
        "🔑 Пожалуйста, введите ваш одноразовый ключ доступа:",
        reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True)
    )

# НОВЫЙ ХЕНДЛЕР: Отмена ввода
@bot.on_message(filters.regex("🔙 Отмена"))
async def cancel_input(c, m):
    user_id = m.from_user.id
    if user_id in temp_auth:
        temp_auth.pop(user_id)
    
    await m.reply(
        "❌ Ввод отменен.\n"
        "Используйте /start для возврата в главное меню.",
        reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True)
    )

@bot.on_message(filters.regex("➕ Добавить аккаунт"))
async def add_account(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    # Проверяем лимит аккаунтов
    if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
        return await m.reply(f"❌ Вы достигли лимита аккаунтов ({MAX_ACCOUNTS_PER_USER}).")
    
    temp_auth[user_id] = {"step": "phone", "user_id": user_id}
    await m.reply("📱 Введите номер телефона в международном формате (например, +380123456789):")

@bot.on_message(filters.regex("📱 Мои аккаунты"))
async def my_accounts(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    accounts = users_data[user_id]["accounts"]
    
    if not accounts:
        return await m.reply(
            "📱 У вас нет добавленных аккаунтов.\n"
            "Используйте кнопку '➕ Добавить аккаунт' чтобы добавить."
        )
    
    acc_list = []
    for i, (phone, data) in enumerate(accounts.items(), 1):
        status = "🟢 АКТИВЕН" if data.get("running", False) else "🔴 ОСТАНОВЛЕН"
        acc_list.append(
            f"{i}. {phone}\n"
            f"   Статус: {status}\n"
            f"   📝 Текст: {data['text'][:30]}...\n"
            f"   ⏱ Интервал: {data['interval']} сек."
        )
    
    await m.reply("📱 Ваши аккаунты:\n\n" + "\n\n".join(acc_list))

@bot.on_message(filters.regex("👤 Мой кабинет"))
async def my_cabinet(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    user_data = users_data[user_id]
    accounts = user_data["accounts"]
    
    total_accounts = len(accounts)
    running_accounts = sum(1 for acc in accounts.values() if acc.get("running", False))
    
    accounts_info = ""
    for phone, acc in accounts.items():
        status = "🟢" if acc.get("running", False) else "🔴"
        accounts_info += f"{status} {phone}\n   📝 {acc['text'][:20]}...\n"
    
    await m.reply(
        f"👤 Личный кабинет\n\n"
        f"🆔 ID: {user_id}\n"
        f"👤 Имя: {user_data.get('username', 'Не указано')}\n"
        f"📅 Доступ до: {user_data['expires'].strftime('%d.%m.%Y')}\n"
        f"🔑 Использован ключ: {user_data['key_used']}\n"
        f"👑 Админ: {'Да' if is_admin(user_id) else 'Нет'}\n\n"
        f"📊 Статистика аккаунтов:\n"
        f"📱 Всего: {total_accounts}/{MAX_ACCOUNTS_PER_USER}\n"
        f"🟢 Активных рассылок: {running_accounts}\n\n"
        f"📋 Ваши аккаунты:\n{accounts_info}"
    )

@bot.on_message(filters.regex("🚀 Старт рассылки"))
async def run(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        return await m.reply("❌ У вас нет добавленных аккаунтов!")
    
    started = 0
    for phone, d in accounts.items():
        if not d.get("running", False) and "client" in d:
            d["running"] = True
            asyncio.create_task(spam_cycle(user_id, phone, d, m))
            started += 1
    
    await m.reply(f"🚀 Запущено рассылок: {started}")

@bot.on_message(filters.regex("🛑 Стоп рассылки"))
async def stop(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    accounts = users_data[user_id]["accounts"]
    stopped = 0
    for d in accounts.values():
        if d.get("running", False):
            d["running"] = False
            stopped += 1
    
    await m.reply(f"🛑 Остановлено рассылок: {stopped}")

@bot.on_message(filters.regex("⚙️ Настройки текста"))
async def set_t(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    if not users_data[user_id]["accounts"]:
        return await m.reply("❌ Сначала добавьте аккаунт!")
    
    temp_auth[user_id] = {"step": "text", "user_id": user_id}
    await m.reply("✏️ Введите новый текст для рассылки (будет применен ко всем вашим аккаунтам):")

@bot.on_message(filters.regex("⏱ Настройки интервала"))
async def set_i(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    if not users_data[user_id]["accounts"]:
        return await m.reply("❌ Сначала добавьте аккаунт!")
    
    temp_auth[user_id] = {"step": "interval", "user_id": user_id}
    await m.reply("⏱ Введите интервал между циклами рассылки (в секундах):")

@bot.on_message(filters.regex("💾 Сохранить настройки"))
async def save_settings_handler(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    if not users_data[user_id]["accounts"]:
        return await m.reply("❌ Нет аккаунтов для сохранения настроек.")
    
    save_user_settings(user_id)
    await m.reply("✅ Ваши настройки сохранены!")

@bot.on_message(filters.regex("📂 Загрузить настройки"))
async def load_settings_handler(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    if not users_data[user_id]["accounts"]:
        return await m.reply("❌ Сначала добавьте аккаунты.")
    
    if load_user_settings(user_id):
        await m.reply("✅ Ваши настройки загружены!")
    else:
        await m.reply("❌ Не удалось загрузить настройки")

@bot.on_message(filters.regex("🔑 Информация о доступе"))
async def access_info(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа")
    
    data = users_data[user_id]
    days_left = (data["expires"] - datetime.now()).days
    
    await m.reply(
        f"🔑 Информация о доступе:\n\n"
        f"✅ Доступ активен\n"
        f"🔑 Ключ: {data['key_used']}\n"
        f"📅 Истекает: {data['expires'].strftime('%d.%m.%Y')}\n"
        f"⏳ Осталось дней: {days_left}\n"
        f"👑 Права: {'Администратор' if is_admin(user_id) else 'Пользователь'}"
    )

# --- АДМИН ФУНКЦИИ ---

@bot.on_message(filters.regex("🔑 Управление ключами"))
async def manage_keys(c, m):
    user_id = m.from_user.id
    if not is_admin(user_id):
        return await m.reply("❌ Эта функция только для администраторов")
    
    keys_list = "📋 Доступные одноразовые ключи:\n\n"
    for key, owner in ONE_TIME_KEYS.items():
        used = False
        used_by = ""
        for uid, user_data in users_data.items():
            if user_data["key_used"] == key:
                used = True
                used_by = f" (использован: {user_data.get('username', uid)})"
                break
        
        status = "❌" if used else "✅"
        keys_list += f"{status} {key} - {owner}{used_by}\n"
    
    await m.reply(keys_list)

@bot.on_message(filters.regex("👥 Все пользователи"))
async def all_users(c, m):
    user_id = m.from_user.id
    if not is_admin(user_id):
        return await m.reply("❌ Эта функция только для администраторов")
    
    if not users_data:
        return await m.reply("📭 Нет активных пользователей")
    
    users_list = "👥 Все пользователи:\n\n"
    for uid, data in users_data.items():
        accounts_count = len(data["accounts"])
        users_list += f"🆔 {uid}\n"
        users_list += f"👤 {data.get('username', 'Нет username')}\n"
        users_list += f"📱 Аккаунтов: {accounts_count}\n"
        users_list += f"📅 Доступ до: {data['expires'].strftime('%d.%m.%Y')}\n"
        users_list += f"👑 Админ: {'Да' if data['is_admin'] else 'Нет'}\n\n"
    
    if len(users_list) > 4000:
        for i in range(0, len(users_list), 4000):
            await m.reply(users_list[i:i+4000])
    else:
        await m.reply(users_list)

@bot.on_message(filters.regex("📊 Статистика"))
async def stats(c, m):
    user_id = m.from_user.id
    if not is_admin(user_id):
        return await m.reply("❌ Эта функция только для администраторов")
    
    total_users = len(users_data)
    total_accounts = sum(len(data["accounts"]) for data in users_data.values())
    total_running = sum(
        sum(1 for acc in data["accounts"].values() if acc.get("running", False)) 
        for data in users_data.values()
    )
    
    total_keys = len(ONE_TIME_KEYS)
    used_keys = sum(1 for user_data in users_data.values() if user_data["key_used"] in ONE_TIME_KEYS)
    
    stats_text = (
        f"📊 Общая статистика бота:\n\n"
        f"👥 Пользователей: {total_users}\n"
        f"📱 Всего аккаунтов: {total_accounts}\n"
        f"🟢 Активных рассылок: {total_running}\n"
        f"🔑 Всего ключей: {total_keys}\n"
        f"✅ Использовано ключей: {used_keys}\n"
        f"📦 Осталось ключей: {total_keys - used_keys}\n"
    )
    
    await m.reply(stats_text)

# --- ОБРАБОТЧИК ВВОДА ---

@bot.on_message(filters.private & ~filters.command(["start"]))
async def auth_handler(c, m):
    uid = m.from_user.id
    
    # Проверяем, находится ли пользователь в режиме ввода
    if uid not in temp_auth:
        # Если не в режиме ввода, игнорируем сообщение
        return
    
    data = temp_auth[uid]
    
    try:
        if data["step"] == "enter_key":
            key = m.text.strip()
            
            if key in ONE_TIME_KEYS:
                key_used = False
                for user_data in users_data.values():
                    if user_data["key_used"] == key:
                        key_used = True
                        break
                
                if key_used:
                    await m.reply("❌ Этот ключ уже был использован!")
                else:
                    owner = ONE_TIME_KEYS[key]
                    is_admin_key = "ADMIN" in key or "админ" in owner.lower()
                    
                    expires = datetime.now() + timedelta(days=KEY_EXPIRY_DAYS)
                    username = m.from_user.username or m.from_user.first_name
                    
                    users_data[uid] = {
                        "expires": expires,
                        "key_used": key,
                        "is_admin": is_admin_key,
                        "username": username,
                        "accounts": {}
                    }
                    
                    save_users()
                    
                    role = "👑 Администратор" if is_admin_key else "👤 Пользователь"
                    await m.reply(
                        f"✅ Доступ предоставлен!\n\n"
                        f"{role}\n"
                        f"Ключ: {key}\n"
                        f"Владелец ключа: {owner}\n"
                        f"Срок действия до: {expires.strftime('%d.%m.%Y %H:%M')}\n\n"
                        f"Используйте /start для входа в личный кабинет",
                        reply_markup=get_user_main_keyboard(uid)
                    )
            else:
                await m.reply("❌ Неверный ключ доступа!")
            
            temp_auth.pop(uid)
        
        elif data["step"] == "phone":
            user_id = data["user_id"]
            phone = m.text
            
            session_name = f"sessions/{phone}_{user_id}"
            client = Client(session_name, api_id=API_ID, api_hash=API_HASH, phone_number=phone)
            await client.connect()
            sent = await client.send_code(phone)
            
            data.update({
                "client": client,
                "phone": phone,
                "code_hash": sent.phone_code_hash,
                "step": "code"
            })
            await m.reply("🔢 Введите код из СМС:")
            
        elif data["step"] == "code":
            try:
                await data["client"].sign_in(data["phone"], data["code_hash"], m.text)
                await finalize_user_account(uid, data, m)
            except Exception as e:
                if "SESSION_PASSWORD_NEEDED" in str(e):
                    data["step"] = "password"
                    await m.reply("🔐 Введите облачный пароль (2FA):")
                else:
                    raise e
                    
        elif data["step"] == "password":
            await data["client"].check_password(m.text)
            await finalize_user_account(uid, data, m)
            
        elif data["step"] == "text":
            user_id = data["user_id"]
            for acc in users_data[user_id]["accounts"].values():
                acc["text"] = m.text
            await m.reply("✅ Текст рассылки обновлен для всех ваших аккаунтов.")
            temp_auth.pop(uid)
            
        elif data["step"] == "interval":
            user_id = data["user_id"]
            try:
                interval = int(m.text)
                if interval < 10:
                    await m.reply("⚠️ Интервал меньше 10 секунд может привести к бану. Продолжить? (да/нет)")
                    data["step"] = "confirm_interval"
                    data["temp_interval"] = interval
                else:
                    for acc in users_data[user_id]["accounts"].values():
                        acc["interval"] = interval
                    await m.reply(f"✅ Интервал установлен: {interval} сек.")
                    temp_auth.pop(uid)
            except ValueError:
                await m.reply("❌ Пожалуйста, введите число!")
                
        elif data["step"] == "confirm_interval":
            user_id = data["user_id"]
            if m.text.lower() in ["да", "yes", "д", "y"]:
                for acc in users_data[user_id]["accounts"].values():
                    acc["interval"] = data["temp_interval"]
                await m.reply(f"✅ Интервал установлен: {data['temp_interval']} сек. (Будьте осторожны!)")
            else:
                await m.reply("❌ Установка интервала отменена.")
            temp_auth.pop(uid)
            
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        temp_auth.pop(uid, None)

async def finalize_user_account(uid, data, m):
    user_id = data["user_id"]
    phone = data["phone"]
    
    users_data[user_id]["accounts"][phone] = {
        "client": data["client"],
        "text": "Привет! Это рассылка.",
        "interval": 3600,
        "running": False,
        "added_date": datetime.now()
    }
    
    await m.reply(f"✅ Аккаунт {phone} успешно добавлен в ваш личный кабинет!")
    temp_auth.pop(uid)
    save_users()
    save_user_settings(user_id)

if __name__ == "__main__":
    load_users()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(load_user_sessions())
    print(f"🔑 Доступные ключи: {list(ONE_TIME_KEYS.keys())}")
    print(f"👥 Пользователей: {len(users_data)}")

    bot.run()
