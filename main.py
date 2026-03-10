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
    "ABCDEF456": "Пользователь 2",
    "ADMINKEY999": "Администратор",
}

KEY_EXPIRY_DAYS = 30
MAX_ACCOUNTS_PER_USER = 3
# ====================================

bot = Client("manager_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Структура данных пользователей
users_data = {}
temp_auth = {}
users_file = "bot_users.json"
reconnect_tasks = {}  # Для отслеживания задач переподключения
session_health_check = {}  # Для проверки здоровья сессий

# --- КЛАСС ДЛЯ УПРАВЛЕНИЯ СЕССИЯМИ ---
class SessionManager:
    """Менеджер для управления сессиями аккаунтов"""
    
    @staticmethod
    def get_session_path(phone, user_id):
        """Возвращает путь к файлу сессии"""
        # Очищаем номер телефона от лишних символов для имени файла
        clean_phone = phone.replace('+', '').replace(' ', '')
        return f"sessions/{clean_phone}_{user_id}"
    
    @staticmethod
    def list_user_sessions(user_id):
        """Возвращает список всех сессий пользователя"""
        sessions = []
        sessions_dir = "sessions"
        if os.path.exists(sessions_dir):
            for filename in os.listdir(sessions_dir):
                if filename.endswith(".session") and f"_{user_id}" in filename:
                    sessions.append(filename.replace(".session", ""))
        return sessions
    
    @staticmethod
    def delete_session(session_name):
        """Удаляет файл сессии"""
        session_file = f"{session_name}.session"
        if os.path.exists(session_file):
            os.remove(session_file)
            logger.info(f"✅ Сессия {session_name} удалена")
            return True
        return False
    
    @staticmethod
    async def verify_session(client, phone):
        """Проверяет, работает ли сессия"""
        try:
            me = await client.get_me()
            logger.info(f"✅ Сессия {phone} работает (аккаунт: {me.phone_number})")
            return True
        except Exception as e:
            logger.error(f"❌ Сессия {phone} не работает: {e}")
            return False

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ---

def save_users():
    """Сохраняет данные пользователей в файл"""
    users_to_save = {}
    for uid, data in users_data.items():
        accounts = {}
        for phone, acc in data["accounts"].items():
            # Очищаем номер для имени файла
            clean_phone = phone.replace('+', '').replace(' ', '')
            accounts[phone] = {
                "text": acc["text"],
                "interval": acc["interval"],
                "running": False,
                "added_date": acc["added_date"].isoformat() if isinstance(acc["added_date"], datetime) else acc["added_date"],
                "session_name": f"sessions/{clean_phone}_{uid}"
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
                            "session_name": acc_data.get("session_name", SessionManager.get_session_path(phone, uid))
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
    failed_count = 0
    
    for user_id, user_data in users_data.items():
        for phone, acc_data in user_data["accounts"].items():
            try:
                session_name = acc_data.get("session_name", SessionManager.get_session_path(phone, user_id))
                
                # Проверяем существование файла сессии
                session_file = f"{session_name}.session"
                if os.path.exists(session_file):
                    client = Client(session_name, api_id=API_ID, api_hash=API_HASH)
                    
                    # Добавляем обработчик отключения
                    async def on_disconnect(client, user_id=user_id, phone=phone):
                        logger.warning(f"⚠️ Аккаунт {phone} отключился")
                        await schedule_reconnect(user_id, phone)
                    
                    client.add_handler(DisconnectHandler(on_disconnect))
                    
                    try:
                        await client.start()
                        
                        # Проверяем, работает ли сессия
                        if await SessionManager.verify_session(client, phone):
                            acc_data["client"] = client
                            loaded_count += 1
                            logger.info(f"✅ Сессия {phone} загружена для пользователя {user_id}")
                        else:
                            # Сессия не работает, удаляем файл
                            await client.stop()
                            SessionManager.delete_session(session_name)
                            failed_count += 1
                            
                    except (SessionRevoked, AuthKeyUnregistered, Unauthorized) as e:
                        logger.error(f"❌ Сессия {phone} недействительна: {e}")
                        SessionManager.delete_session(session_name)
                        failed_count += 1
                    except Exception as e:
                        logger.error(f"❌ Ошибка загрузки сессии {phone}: {e}")
                        failed_count += 1
                else:
                    logger.warning(f"⚠️ Файл сессии {session_file} не найден")
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки сессии {phone}: {e}")
                failed_count += 1
    
    logger.info(f"✅ Загружено {loaded_count} активных сессий, {failed_count} не загружено")

async def schedule_reconnect(user_id, phone):
    """Планирует переподключение аккаунта"""
    key = f"{user_id}_{phone}"
    
    # Отменяем существующую задачу переподключения
    if key in reconnect_tasks:
        reconnect_tasks[key].cancel()
    
    # Создаем новую задачу с задержкой
    async def reconnect_with_delay():
        await asyncio.sleep(60)
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
    session_name = acc_data.get("session_name", SessionManager.get_session_path(phone, user_id))
    
    try:
        # Проверяем, существует ли файл сессии
        if not os.path.exists(f"{session_name}.session"):
            logger.error(f"❌ Файл сессии {session_name} не найден")
            return
        
        # Пробуем переподключиться
        client = Client(session_name, api_id=API_ID, api_hash=API_HASH)
        
        async def on_disconnect(client, user_id=user_id, phone=phone):
            logger.warning(f"⚠️ Аккаунт {phone} снова отключился")
            await schedule_reconnect(user_id, phone)
        
        client.add_handler(DisconnectHandler(on_disconnect))
        await client.start()
        
        # Проверяем подключение
        if await SessionManager.verify_session(client, phone):
            # Останавливаем старый клиент если есть
            if "client" in acc_data:
                try:
                    await acc_data["client"].stop()
                except:
                    pass
            
            # Сохраняем новый клиент
            acc_data["client"] = client
            
            # Если рассылка была активна, перезапускаем
            if acc_data.get("running", False):
                asyncio.create_task(spam_cycle(user_id, phone, acc_data, None))
            
            logger.info(f"✅ Аккаунт {phone} успешно переподключен")
        else:
            await client.stop()
            logger.error(f"❌ Не удалось подтвердить подключение {phone}")
            
    except Exception as e:
        logger.error(f"❌ Не удалось переподключить {phone}: {e}")

# --- ФУНКЦИЯ ДЛЯ ПРОВЕРКИ ЗДОРОВЬЯ СЕССИЙ ---
async def health_check_loop():
    """Периодическая проверка всех сессий"""
    while True:
        await asyncio.sleep(300)  # Проверяем каждые 5 минут
        
        logger.info("🔍 Запуск проверки здоровья сессий...")
        for user_id, user_data in users_data.items():
            for phone, acc_data in user_data["accounts"].items():
                if "client" in acc_data:
                    try:
                        # Проверяем, отвечает ли клиент
                        await acc_data["client"].get_me()
                    except Exception as e:
                        logger.warning(f"⚠️ Сессия {phone} не отвечает: {e}")
                        # Пробуем переподключиться
                        asyncio.create_task(reconnect_account(user_id, phone))

# [Остальные функции остаются без изменений...]

async def finalize_user_account(uid, data, m):
    """Завершает добавление аккаунта"""
    user_id = data["user_id"]
    phone = data["phone"]
    session_name = SessionManager.get_session_path(phone, user_id)
    
    # Сохраняем клиент
    client = data["client"]
    
    # Добавляем обработчик отключения
    async def on_disconnect(client, user_id=user_id, phone=phone):
        logger.warning(f"⚠️ Аккаунт {phone} отключился")
        await schedule_reconnect(user_id, phone)
    
    client.add_handler(DisconnectHandler(on_disconnect))
    
    # Проверяем, нет ли уже такой сессии
    if phone in users_data[user_id]["accounts"]:
        old_data = users_data[user_id]["accounts"][phone]
        if "client" in old_data:
            try:
                await old_data["client"].stop()
            except:
                pass
    
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
    
    logger.info(f"✅ Аккаунт {phone} добавлен для пользователя {user_id}")

# --- НОВАЯ ФУНКЦИЯ: Удаление аккаунта ---
@bot.on_message(filters.regex("❌ Удалить аккаунт"))
async def delete_account_prompt(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа")
    
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        return await m.reply("📱 У вас нет аккаунтов для удаления")
    
    # Показываем список аккаунтов для удаления
    buttons = []
    for phone in accounts.keys():
        buttons.append([f"❌ {phone}"])
    buttons.append(["🔙 Отмена"])
    
    await m.reply(
        "Выберите аккаунт для удаления:",
        reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True)
    )
    
    temp_auth[user_id] = {"step": "delete_account"}

@bot.on_message(filters.regex("❌ \+?\d+"))
async def delete_account_confirm(c, m):
    user_id = m.from_user.id
    if user_id not in temp_auth or temp_auth[user_id].get("step") != "delete_account":
        return
    
    phone = m.text.replace("❌ ", "")
    
    if phone in users_data[user_id]["accounts"]:
        # Останавливаем рассылку если активна
        if users_data[user_id]["accounts"][phone].get("running", False):
            users_data[user_id]["accounts"][phone]["running"] = False
        
        # Закрываем клиент
        if "client" in users_data[user_id]["accounts"][phone]:
            try:
                await users_data[user_id]["accounts"][phone]["client"].stop()
            except:
                pass
        
        # Удаляем файл сессии
        session_name = users_data[user_id]["accounts"][phone].get(
            "session_name", 
            SessionManager.get_session_path(phone, user_id)
        )
        SessionManager.delete_session(session_name)
        
        # Удаляем из данных
        del users_data[user_id]["accounts"][phone]
        save_users()
        
        await m.reply(f"✅ Аккаунт {phone} удален", reply_markup=get_user_main_keyboard(user_id))
    else:
        await m.reply("❌ Аккаунт не найден")
    
    temp_auth.pop(user_id, None)

# --- НОВАЯ ФУНКЦИЯ: Список сессий ---
@bot.on_message(filters.regex("📁 Мои сессии"))
async def list_sessions(c, m):
    user_id = m.from_user.id
    if not check_access(user_id):
        return await m.reply("❌ У вас нет доступа")
    
    sessions = SessionManager.list_user_sessions(user_id)
    
    if not sessions:
        await m.reply("📁 У вас нет сохраненных сессий")
    else:
        sessions_list = "📁 Ваши сессии:\n\n"
        for session in sessions:
            sessions_list += f"• {session}\n"
        
        await m.reply(sessions_list)

if __name__ == "__main__":
    # Создаем необходимые папки
    os.makedirs("sessions", exist_ok=True)
    os.makedirs("user_settings", exist_ok=True)
    
    # Загружаем данные
    load_users()
    
    # Запускаем загрузку сессий и проверку здоровья
    loop = asyncio.get_event_loop()
    
    async def startup():
        await load_user_sessions()
        # Запускаем фоновую проверку здоровья сессий
        asyncio.create_task(health_check_loop())
        logger.info(f"🔑 Доступные ключи: {list(ONE_TIME_KEYS.keys())}")
        logger.info(f"👥 Пользователей: {len(users_data)}")
    
    loop.run_until_complete(startup())
    
    # Запускаем бота
    logger.info("🤖 Бот запущен и готов к работе")
    bot.run()
