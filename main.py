import asyncio
import os
import json
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.types import ReplyKeyboardMarkup
from pyrogram.errors import PeerIdInvalid, Forbidden, SessionRevoked, AuthKeyUnregistered, Unauthorized
from pyrogram.handlers import DisconnectHandler
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- КОНФИГ ---
API_ID = 30032542
API_HASH = "ce646da1307fb452305d49f9bb8751ca"
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8711240311:AAHy5FzxQ7P0MpSm3Bv7xfoYDa9kVlwAb5w')

# === НАСТРОЙКА ОДНОРАЗОВЫХ КЛЮЧЕЙ ===
ONE_TIME_KEYS = {
    "SECRET123": "Пользователь 1",
    "pruma": "Пользователь 2",
    "Iggor": "Пользователь 3",
    "SECRET123": "Пользователь 4",
    "ABCDEF456": "Пользователь 5",
    "ADMINKEY": "Администратор",
}

KEY_EXPIRY_DAYS = 30
MAX_ACCOUNTS_PER_USER = 3
# ====================================

bot = Client("manager_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Структура данных пользователей
users_data = {}
temp_auth = {}
settings_file = "bot_settings.json"
users_file = "bot_users.json"
reconnect_tasks = {}  # Для отслеживания задач переподключения

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ---

def save_users():
    """Сохраняет данные пользователей в файл"""
    users_to_save = {}
    for uid, data in users_data.items():
        # Сохраняем только сериализуемые данные, без клиентов
        accounts = {}
        for phone, acc in data["accounts"].items():
            accounts[phone] = {
                "text": acc["text"],
                "interval": acc["interval"],
                "running": False,  # Всегда сохраняем как остановленные
                "added_date": acc["added_date"].isoformat() if isinstance(acc["added_date"], datetime) else acc["added_date"],
                "session_name": f"sessions/{phone}_{uid}"  # Сохраняем имя сессии
            }
        
        users_to_save[str(uid)] = {
            "expires": data["expires"].isoformat(),
            "key_used": data["key_used"],
            "is_admin": data["is_admin"],
            "username": data.get("username", ""),
            "accounts": accounts
        }
    
    with open(users_file, 'w', encoding='utf-8') as f:
        json.dump(users_to_save, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Сохранено {len(users_data)} пользователей")

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
                            "added_date": datetime.fromisoformat(acc_data["added_date"]) if isinstance(acc_data.get("added_date"), str) else datetime.now(),
                            "session_name": acc_data.get("session_name", f"sessions/{phone}_{uid}")
                        }
                    
                    users_data[uid] = {
                        "expires": expires,
                        "key_used": data["key_used"],
                        "is_admin": data["is_admin"],
                        "username": data.get("username", ""),
                        "accounts": accounts
                    }
            
            logger.info(f"✅ Загружено {len(users_data)} пользователей")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки пользователей: {e}")
        return False

async def load_user_sessions():
    """Загружает сессии для всех пользователей"""
    if not os.path.exists("sessions"):
        os.makedirs("sessions")
        logger.info("📁 Создана папка sessions")
    
    loaded_count = 0
    for user_id, user_data in users_data.items():
        for phone, acc_data in user_data["accounts"].items():
            try:
                session_name = acc_data.get("session_name", f"sessions/{phone}_{user_id}")
                
                # Проверяем существование файла сессии
                session_file = f"{session_name}.session"
                if os.path.exists(session_file):
                    client = Client(session_name, api_id=API_ID, api_hash=API_HASH)
                    
                    # Добавляем обработчик отключения
                    async def on_disconnect(client, user_id=user_id, phone=phone):
                        logger.warning(f"⚠️ Аккаунт {phone} отключился, планируем переподключение")
                        await schedule_reconnect(user_id, phone)
                    
                    client.add_handler(DisconnectHandler(on_disconnect))
                    
                    try:
                        await client.start()
                        acc_data["client"] = client
                        loaded_count += 1
                        logger.info(f"✅ Сессия {phone} загружена для пользователя {user_id}")
                    except (SessionRevoked, AuthKeyUnregistered, Unauthorized) as e:
                        logger.error(f"❌ Сессия {phone} недействительна: {e}")
                        # Удаляем недействительный файл сессии
                        if os.path.exists(session_file):
                            os.remove(session_file)
                    except Exception as e:
                        logger.error(f"❌ Ошибка загрузки сессии {phone}: {e}")
                else:
                    logger.warning(f"⚠️ Файл сессии {session_file} не найден")
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки сессии {phone}: {e}")
    
    logger.info(f"✅ Загружено {loaded_count} активных сессий")

async def schedule_reconnect(user_id, phone):
    """Планирует переподключение аккаунта"""
    key = f"{user_id}_{phone}"
    
    # Отменяем существующую задачу переподключения
    if key in reconnect_tasks:
        reconnect_tasks[key].cancel()
    
    # Создаем новую задачу с задержкой
    async def reconnect_with_delay():
        await asyncio.sleep(60)  # Ждем 60 секунд перед переподключением
        try:
            await reconnect_account(user_id, phone)
        except Exception as e:
            logger.error(f"❌ Ошибка переподключения {phone}: {e}")
    
    reconnect_tasks[key] = asyncio.create_task(reconnect_with_delay())

async def reconnect_account(user_id, phone):
    """Переподключает аккаунт"""
    if user_id not in users_data or phone not in users_data[user_id]["accounts"]:
        return
    
    acc_data = users_data[user_id]["accounts"][phone]
    session_name = acc_data.get("session_name", f"sessions/{phone}_{user_id}")
    
    try:
        # Пробуем переподключиться
        client = Client(session_name, api_id=API_ID, api_hash=API_HASH)
        
        async def on_disconnect(client, user_id=user_id, phone=phone):
            logger.warning(f"⚠️ Аккаунт {phone} снова отключился")
            await schedule_reconnect(user_id, phone)
        
        client.add_handler(DisconnectHandler(on_disconnect))
        await client.start()
        
        # Восстанавливаем состояние
        acc_data["client"] = client
        if acc_data.get("running", False):
            # Если рассылка была активна, перезапускаем
            asyncio.create_task(spam_cycle(user_id, phone, acc_data, None))
        
        logger.info(f"✅ Аккаунт {phone} успешно переподключен")
    except Exception as e:
        logger.error(f"❌ Не удалось переподключить {phone}: {e}")

def check_access(user_id):
    """Проверяет доступ пользователя"""
    if user_id in users_data:
        user_data = users_data[user_id]
        if user_data["expires"] > datetime.now():
            return True
        else:
            # Закрываем все клиенты перед удалением
            for acc in user_data["accounts"].values():
                if "client" in acc:
                    try:
                        asyncio.create_task(acc["client"].stop())
                    except:
                        pass
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
        logger.error(f"❌ Ошибка загрузки настроек пользователя {user_id}: {e}")
    return False

# --- ФУНКЦИИ ДЛЯ РАССЫЛКИ ---

async def spam_cycle(user_id, phone, data, message):
    """Фоновый процесс рассылки для конкретного пользователя"""
    status_msg = None
    if message:
        status_msg = await message.reply(f"🚀 Запуск рассылки для {phone}...")
    
    sent_chats = []
    error_count = 0

    while data.get("running", False):
        try:
            if "client" not in data:
                logger.error(f"❌ Нет клиента для {phone}")
                error_count += 1
                if error_count > 3:
                    break
                await asyncio.sleep(60)
                continue
            
            # Проверяем, подключен ли клиент
            try:
                await data["client"].get_me()
            except:
                # Пробуем переподключиться
                logger.warning(f"⚠️ Клиент {phone} не отвечает, пробуем переподключиться")
                await reconnect_account(user_id, phone)
                await asyncio.sleep(30)
                continue
            
            async for dialog in data["client"].get_dialogs():
                if not data.get("running", False): 
                    break
                
                if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    try:
                        await data["client"].send_message(dialog.chat.id, data["text"])
                       
                        sent_chats.append(dialog.chat.title)
                        if status_msg:
                            new_text = f"🚀 Рассылка {phone} активна\n\nОтправлено в {len(sent_chats)} чатов\nПоследние:\n" + "\n".join(sent_chats[-5:])
                            try:
                                await status_msg.edit_text(new_text)
                            except:
                                pass
                       
                        await asyncio.sleep(0.2)  # Небольшая задержка между сообщениями
                    except (PeerIdInvalid, Forbidden): 
                        continue
                    except Exception as e:
                        logger.error(f"Ошибка отправки: {e}")
                        continue
            
            error_count = 0  # Сбрасываем счетчик ошибок после успешного цикла
            await asyncio.sleep(data["interval"])
            
        except Exception as e:
            logger.error(f"Ошибка в цикле рассылки {phone}: {e}")
            error_count += 1
            if error_count > 5:
                logger.error(f"❌ Слишком много ошибок для {phone}, останавливаем рассылку")
                data["running"] = False
                break
            await asyncio.sleep(60)
   
    if status_msg:
        await status_msg.edit_text(f"✅ Рассылка {phone} завершена.\nВсего чатов: {len(sent_chats)}")
    
    logger.info(f"✅ Рассылка {phone} остановлена")

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

@bot.on_message(filters.regex("🔑 Ввести ключ доступа"))
async def enter_key_prompt(c, m):
    user_id = m.from_user.id
    
    if check_access(user_id):
        return await m.reply(
            "✅ У вас уже есть активный доступ!\n"
            "Используйте /start для входа в личный кабинет.",
            reply_markup=get_user_main_keyboard(user_id)
        )
    
    temp_auth[user_id] = {"step": "enter_key", "user_id": user_id}
    await m.reply(
        "🔑 Пожалуйста, введите ваш одноразовый ключ доступа:",
        reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True)
    )

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
        client_status = "✅" if "client" in data else "❌"
        acc_list.append(
            f"{i}. {phone}\n"
            f"   Статус: {status} | Клиент: {client_status}\n"
            f"   📝 Текст: {data['text'][:30]}...\n"
            f"   ⏱ Интервал: {data['interval']} сек."
        )
    
    await m.reply("📱 Ваши аккаунты:\n\n" + "\n\n".join(acc_list))

# [Остальные хендлеры остаются без изменений...]

@bot.on_message(filters.regex("👤 Мой кабинет"))
async def my_cabinet(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа. Введите ключ через /start")
    
    user_data = users_data[user_id]
    accounts = user_data["accounts"]
    
    total_accounts = len(accounts)
    running_accounts = sum(1 for acc in accounts.values() if acc.get("running", False))
    active_clients = sum(1 for acc in accounts.values() if "client" in acc)
    
    accounts_info = ""
    for phone, acc in accounts.items():
        status = "🟢" if acc.get("running", False) else "🔴"
        client = "✅" if "client" in acc else "❌"
        accounts_info += f"{status}{client} {phone}\n   📝 {acc['text'][:20]}...\n"
    
    await m.reply(
        f"👤 Личный кабинет\n\n"
        f"🆔 ID: {user_id}\n"
        f"👤 Имя: {user_data.get('username', 'Не указано')}\n"
        f"📅 Доступ до: {user_data['expires'].strftime('%d.%m.%Y')}\n"
        f"🔑 Использован ключ: {user_data['key_used']}\n"
        f"👑 Админ: {'Да' if is_admin(user_id) else 'Нет'}\n\n"
        f"📊 Статистика аккаунтов:\n"
        f"📱 Всего: {total_accounts}/{MAX_ACCOUNTS_PER_USER}\n"
        f"✅ Активных клиентов: {active_clients}\n"
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
        if not d.get("running", False):
            if "client" not in d:
                # Пробуем загрузить сессию
                await reconnect_account(user_id, phone)
                await asyncio.sleep(2)
            
            if "client" in d:
                d["running"] = True
                asyncio.create_task(spam_cycle(user_id, phone, d, m))
                started += 1
    
    await m.reply(f"🚀 Запущено рассылок: {started}")

# [Остальные хендлеры остаются без изменений...]

async def finalize_user_account(uid, data, m):
    """Завершает добавление аккаунта"""
    user_id = data["user_id"]
    phone = data["phone"]
    session_name = f"sessions/{phone}_{user_id}"
    
    # Сохраняем клиент
    client = data["client"]
    
    # Добавляем обработчик отключения
    async def on_disconnect(client, user_id=user_id, phone=phone):
        logger.warning(f"⚠️ Аккаунт {phone} отключился")
        await schedule_reconnect(user_id, phone)
    
    client.add_handler(DisconnectHandler(on_disconnect))
    
    # Сохраняем в данные пользователя
    users_data[user_id]["accounts"][phone] = {
        "client": client,
        "text": "Привет! Это рассылка.",
        "interval": 3600,
        "running": False,
        "added_date": datetime.now(),
        "session_name": session_name
    }
    
    await m.reply(f"✅ Аккаунт {phone} успешно добавлен в ваш личный кабинет!")
    temp_auth.pop(uid)
    save_users()
    save_user_settings(user_id)
    
    logger.info(f"✅ Аккаунт {phone} добавлен для пользователя {user_id}")

if __name__ == "__main__":
    # Создаем необходимые папки
    os.makedirs("sessions", exist_ok=True)
    os.makedirs("user_settings", exist_ok=True)
    
    # Загружаем данные
    load_users()
    
    # Запускаем загрузку сессий в отдельной задаче
    loop = asyncio.get_event_loop()
    
    async def startup():
        await load_user_sessions()
        logger.info(f"🔑 Доступные ключи: {list(ONE_TIME_KEYS.keys())}")
        logger.info(f"👥 Пользователей: {len(users_data)}")
    
    loop.run_until_complete(startup())
    
    # Запускаем бота
    logger.info("🤖 Бот запущен и готов к работе")
    bot.run()

