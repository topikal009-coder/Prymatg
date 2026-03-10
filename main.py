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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
reconnect_tasks = {}

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
                            "session_name": acc_data.get("session_name", f"sessions/{phone.replace('+', '').replace(' ', '')}_{uid}")
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
                session_name = acc_data.get("session_name", f"sessions/{phone.replace('+', '').replace(' ', '')}_{user_id}")
                
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
    session_name = acc_data.get("session_name", f"sessions/{phone.replace('+', '').replace(' ', '')}_{user_id}")
    
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
                       
                        await asyncio.sleep(0.2)
                    except (PeerIdInvalid, Forbidden): 
                        continue
                    except Exception as e:
                        logger.error(f"Ошибка отправки: {e}")
                        continue
            
            error_count = 0
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
    
    logger.info(f"Пользователь {user_id} запустил /start")
    
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
    logger.info(f"Пользователь {user_id} нажал кнопку ввода ключа")
    
    if check_access(user_id):
        return await m.reply(
            "✅ У вас уже есть активный доступ!\n"
            "Используйте /start для входа в личный кабинет.",
            reply_markup=get_user_main_keyboard(user_id)
        )
    
    temp_auth[user_id] = {"step": "enter_key"}
    logger.info(f"Установлен шаг enter_key для пользователя {user_id}")
    
    await m.reply(
        "🔑 Пожалуйста, введите ваш одноразовый ключ доступа:",
        reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True)
    )

@bot.on_message(filters.regex("🔙 Отмена"))
async def cancel_input(c, m):
    user_id = m.from_user.id
    logger.info(f"Пользователь {user_id} отменил ввод")
    
    if user_id in temp_auth:
        temp_auth.pop(user_id)
    
    await m.reply(
        "❌ Ввод отменен.\n"
        "Используйте /start для возврата в главное меню.",
        reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True)
    )

# --- ВАЖНО: Этот хендлер обрабатывает ВСЕ текстовые сообщения ---
@bot.on_message(filters.text & filters.private)
async def handle_all_messages(c, m):
    """Универсальный обработчик всех текстовых сообщений"""
    user_id = m.from_user.id
    text = m.text
    
    logger.info(f"Получено сообщение от {user_id}: {text}")
    
    # Проверяем, находится ли пользователь в режиме ввода ключа
    if user_id in temp_auth:
        step = temp_auth[user_id].get("step")
        logger.info(f"Пользователь {user_id} в шаге: {step}")
        
        if step == "enter_key":
            # Обрабатываем ввод ключа
            key = text.strip()
            logger.info(f"Пользователь {user_id} ввел ключ: {key}")
            
            if key in ONE_TIME_KEYS:
                # Проверяем, не использован ли ключ
                key_used = False
                for user_data in users_data.values():
                    if user_data["key_used"] == key:
                        key_used = True
                        break
                
                if key_used:
                    await m.reply("❌ Этот ключ уже был использован!")
                    logger.info(f"Ключ {key} уже использован")
                else:
                    owner = ONE_TIME_KEYS[key]
                    is_admin_key = "ADMIN" in key or "админ" in owner.lower()
                    
                    expires = datetime.now() + timedelta(days=KEY_EXPIRY_DAYS)
                    username = m.from_user.username or m.from_user.first_name
                    
                    # Создаем нового пользователя
                    users_data[user_id] = {
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
                        reply_markup=get_user_main_keyboard(user_id)
                    )
                    
                    logger.info(f"✅ Пользователь {user_id} получил доступ с ключом {key}")
                    
                    # Очищаем временные данные
                    temp_auth.pop(user_id, None)
            else:
                await m.reply("❌ Неверный ключ доступа!")
                logger.info(f"Неверный ключ: {key}")
            
            return  # Важно: возвращаем, чтобы не обрабатывать дальше
        
        elif step == "phone":
            # Обработка ввода телефона (для добавления аккаунта)
            await handle_phone_input(c, m)
            return
        elif step == "code":
            # Обработка ввода кода
            await handle_code_input(c, m)
            return
        elif step == "password":
            # Обработка ввода пароля 2FA
            await handle_password_input(c, m)
            return
        elif step == "text":
            # Обработка ввода текста рассылки
            await handle_text_input(c, m)
            return
        elif step == "interval":
            # Обработка ввода интервала
            await handle_interval_input(c, m)
            return
        elif step == "confirm_interval":
            # Подтверждение интервала
            await handle_interval_confirm(c, m)
            return
    
    # Если пользователь не в режиме ввода, проверяем команды из меню
    await handle_menu_commands(c, m)

async def handle_phone_input(c, m):
    """Обработка ввода номера телефона"""
    user_id = m.from_user.id
    phone = m.text
    
    try:
        session_name = f"sessions/{phone.replace('+', '').replace(' ', '')}_{user_id}"
        client = Client(session_name, api_id=API_ID, api_hash=API_HASH, phone_number=phone)
        await client.connect()
        sent = await client.send_code(phone)
        
        temp_auth[user_id].update({
            "client": client,
            "phone": phone,
            "code_hash": sent.phone_code_hash,
            "step": "code"
        })
        await m.reply("🔢 Введите код из СМС:")
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        temp_auth.pop(user_id, None)

async def handle_code_input(c, m):
    """Обработка ввода кода"""
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
    """Обработка ввода пароля 2FA"""
    user_id = m.from_user.id
    data = temp_auth[user_id]
    
    try:
        await data["client"].check_password(m.text)
        await finalize_user_account(user_id, data, m)
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        temp_auth.pop(user_id, None)

async def handle_text_input(c, m):
    """Обработка ввода текста рассылки"""
    user_id = m.from_user.id
    data = temp_auth[user_id]
    
    for acc in users_data[user_id]["accounts"].values():
        acc["text"] = m.text
    
    await m.reply("✅ Текст рассылки обновлен для всех ваших аккаунтов.")
    temp_auth.pop(user_id)

async def handle_interval_input(c, m):
    """Обработка ввода интервала"""
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
            await m.reply(f"✅ Интервал установлен: {interval} сек.")
            temp_auth.pop(user_id)
    except ValueError:
        await m.reply("❌ Пожалуйста, введите число!")

async def handle_interval_confirm(c, m):
    """Подтверждение интервала"""
    user_id = m.from_user.id
    data = temp_auth[user_id]
    
    if m.text.lower() in ["да", "yes", "д", "y"]:
        for acc in users_data[user_id]["accounts"].values():
            acc["interval"] = data["temp_interval"]
        await m.reply(f"✅ Интервал установлен: {data['temp_interval']} сек. (Будьте осторожны!)")
    else:
        await m.reply("❌ Установка интервала отменена.")
    
    temp_auth.pop(user_id)

async def handle_menu_commands(c, m):
    """Обработка команд из меню"""
    user_id = m.from_user.id
    text = m.text
    
    # Сначала проверяем доступ
    if not check_access(user_id):
        await m.reply("❌ У вас нет доступа. Используйте /start для входа.")
        return
    
    # Обрабатываем команды меню
    if text == "➕ Добавить аккаунт":
        if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
            await m.reply(f"❌ Вы достигли лимита аккаунтов ({MAX_ACCOUNTS_PER_USER}).")
        else:
            temp_auth[user_id] = {"step": "phone"}
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
                acc_list.append(
                    f"{i}. {phone}\n"
                    f"   Статус: {status} | Клиент: {client_status}\n"
                    f"   📝 Текст: {data['text'][:30]}...\n"
                    f"   ⏱ Интервал: {data['interval']} сек."
                )
            await m.reply("📱 Ваши аккаунты:\n\n" + "\n\n".join(acc_list))
    
    elif text == "👤 Мой кабинет":
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
        
        await m.reply(f"🛑 Остановлено рассылок: {stopped}")
    
    elif text == "⚙️ Настройки текста":
        if not users_data[user_id]["accounts"]:
            await m.reply("❌ Сначала добавьте аккаунт!")
        else:
            temp_auth[user_id] = {"step": "text"}
            await m.reply("✏️ Введите новый текст для рассылки:")
    
    elif text == "⏱ Настройки интервала":
        if not users_data[user_id]["accounts"]:
            await m.reply("❌ Сначала добавьте аккаунт!")
        else:
            temp_auth[user_id] = {"step": "interval"}
            await m.reply("⏱ Введите интервал между циклами рассылки (в секундах):")
    
    elif text == "🔑 Информация о доступе":
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
    
    # Админские команды
    elif is_admin(user_id):
        if text == "🔑 Управление ключами":
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
        
        elif text == "👥 Все пользователи":
            if not users_data:
                await m.reply("📭 Нет активных пользователей")
            else:
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
        
        elif text == "📊 Статистика":
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

async def finalize_user_account(uid, data, m):
    """Завершает добавление аккаунта"""
    user_id = data["user_id"]
    phone = data["phone"]
    session_name = f"sessions/{phone.replace('+', '').replace(' ', '')}_{user_id}"
    
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
    
    await m.reply(f"✅ Аккаунт {phone} успешно добавлен!")
    temp_auth.pop(uid)
    save_users()
    
    logger.info(f"✅ Аккаунт {phone} добавлен для пользователя {user_id}")

if __name__ == "__main__":
    # Создаем необходимые папки
    os.makedirs("sessions", exist_ok=True)
    os.makedirs("user_settings", exist_ok=True)
    
    # Загружаем данные
    load_users()
    
    # Запускаем загрузку сессий
    loop = asyncio.get_event_loop()
    
    async def startup():
        await load_user_sessions()
        logger.info(f"🔑 Доступные ключи: {list(ONE_TIME_KEYS.keys())}")
        logger.info(f"👥 Пользователей: {len(users_data)}")
    
    loop.run_until_complete(startup())
    
    # Запускаем бота
    logger.info("🤖 Бот запущен и готов к работе")
    bot.run()
