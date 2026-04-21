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
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Volume не смонтирован в {WORK_DIR}")
        os.makedirs(WORK_DIR, exist_ok=True)
else:
    WORK_DIR = os.path.dirname(os.path.abspath(__file__))

# Создаём папки
os.makedirs(WORK_DIR, exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, 'sessions'), exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, 'user_settings'), exist_ok=True)

logger.info(f"📁 Рабочая директория: {WORK_DIR}")
logger.info(f"📁 На Railway: {IS_RAILWAY}")

# === НАСТРОЙКА КЛЮЧЕЙ (месяц, год, навсегда) ===
KEYS_FILE = os.path.join(WORK_DIR, 'activation_keys.json')

def load_keys():
    default_keys = {
        # Ключи на месяц (30 дней)
        "MONTH_KEY_001": ("Месячный ключ", 30),
        "MONTH_KEY_002": ("Месячный ключ", 30),
        # Ключи на год (365 дней)
        "YEAR_KEY_001": ("Годовой ключ", 365),
        "YEAR_KEY_002": ("Годовой ключ", 365),
        # Ключи навсегда (10 лет)
        "FOREVER_KEY_001": ("Ключ навсегда", 3650),
        "FOREVER_KEY_002": ("Ключ навсегда", 3650),
        # Админский ключ (навсегда + админка)
        "ADMIN_FOREVER": ("Администратор", 3650, True)
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

# --- ЗАГРУЗКА / СОХРАНЕНИЕ ПОЛЬЗОВАТЕЛЕЙ ---
users_data = {}
temp_auth = {}
users_file = os.path.join(WORK_DIR, "bot_users.json")
reconnect_tasks = {}
keep_alive_tasks = {}

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

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def is_admin(user_id):
    return users_data.get(user_id, {}).get("is_admin", False)

def has_active_subscription(user_id):
    """Проверяет, активна ли подписка (для запуска рассылки)"""
    if user_id not in users_data:
        return False
    expires = users_data[user_id]["expires"]
    if isinstance(expires, str):
        expires = datetime.fromisoformat(expires)
    return expires > datetime.now()

# Для остальных действий доступ всегда открыт
def can_use_bot(user_id):
    return user_id in users_data  # пользователь зарегистрирован

# --- КЛАВИАТУРЫ (инлайн) ---
def get_main_keyboard(user_id):
    kb = [
        [InlineKeyboardButton("👤 Мой профиль", callback_data="profile")],
        [InlineKeyboardButton("🚀 Запуск рассылки", callback_data="start_menu")],
        [InlineKeyboardButton("🛑 Стоп рассылки", callback_data="stop_all")],
        [InlineKeyboardButton("🔑 Информация о доступе", callback_data="access_info")]
    ]
    if is_admin(user_id):
        kb.append([InlineKeyboardButton("🛠 Админ панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

def get_start_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Сменить интервал", callback_data="change_interval")],
        [InlineKeyboardButton("✏️ Сменить текст", callback_data="change_text")],
        [InlineKeyboardButton("🚀 Запустить (обычный)", callback_data="start_normal")],
        [InlineKeyboardButton("🛡 Безопасный режим", callback_data="start_safe")]
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

# --- ПРИВЕТСТВЕННОЕ ФОТО ---
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

# --- БЕЗОПАСНЫЙ РЕЖИМ: РАНДОМНЫЙ ТЕКСТ И ИНТЕРВАЛ ---
async def safe_spam_cycle(user_id, phone, data, message):
    """Рассылка в безопасном режиме: случайный текст из списка, интервал 55-70 мин"""
    status_msg = None
    if message:
        status_msg = await message.reply(f"🛡 Запуск безопасной рассылки для {phone}...")

    texts = data.get("texts_list", [])
    if not texts:
        texts = [data["text"]]  # если список пуст, используем текущий текст
    base_interval = data.get("base_interval", 3600)  # 3600 сек = 1 час
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

            # Проверяем клиент
            try:
                me = await data["client"].get_me()
                if not me:
                    raise Exception("Не удалось получить информацию")
            except Exception as e:
                logger.warning(f"Клиент {phone} не отвечает: {e}")
                await reconnect_account(user_id, phone)
                await asyncio.sleep(30)
                continue

            # Собираем чаты
            dialogs = []
            async for dialog in data["client"].get_dialogs():
                if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    dialogs.append(dialog)

            # Выбираем случайный текст
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

            # Рандомная задержка от 55 до 70 минут (3300-4200 сек)
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
            await status_msg.edit_text(f"✅ Безопасная рассылка {phone} завершена. Всего циклов: {cycle_count}, чатов: {len(sent_chats)}")
        except:
            pass
    logger.info(f"Безопасная рассылка {phone} остановлена")

# --- ОБЫЧНЫЙ ЦИКЛ РАССЫЛКИ (без изменений) ---
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

# --- ПЕРЕПОДКЛЮЧЕНИЕ И KEEP-ALIVE (как в оригинале) ---
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

# --- ОБРАБОТЧИКИ КОМАНД И CALLBACK'ОВ ---
@bot.on_message(filters.command("start"))
async def start_cmd(c: Client, m: Message):
    user_id = m.from_user.id
    username = m.from_user.username or m.from_user.first_name

    # Если пользователь не зарегистрирован, просим ключ
    if user_id not in users_data:
        await m.reply(
            "🔐 *Доступ ограничен*\n\n"
            "Для использования бота введите одноразовый ключ доступа.\n"
            "Ключ можно ввести в формате:\n"
            "• обычный ключ: `KEY123`\n"
            "• привязанный ключ: `KEY123-@username`\n\n"
            "Нажмите кнопку ниже чтобы ввести ключ.",
            reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True),
            parse_mode=enums.ParseMode.MARKDOWN
        )
        return

    # Отправляем приветственное фото (если установлено)
    photo_id = get_welcome_photo_id()
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

@bot.on_message(filters.regex("🔑 Ввести ключ доступа"))
async def enter_key_prompt(c, m):
    user_id = m.from_user.id
    if user_id in users_data:
        await m.reply("✅ У вас уже есть доступ!", reply_markup=get_main_keyboard(user_id))
        return
    temp_auth[user_id] = {"step": "enter_key"}
    await m.reply(
        "🔑 Введите одноразовый ключ:\n\n"
        "Форматы:\n"
        "• `KEY123`\n"
        "• `KEY123-@username`",
        reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@bot.on_message(filters.regex("🔙 Отмена"))
async def cancel_input(c, m):
    user_id = m.from_user.id
    temp_auth.pop(user_id, None)
    await m.reply("❌ Ввод отменён.", reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True))

# --- ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ (ключ, номер, код, 2FA, текст, интервал, тексты для безопасного режима) ---
@bot.on_message(filters.text & filters.private)
async def handle_text(c: Client, m: Message):
    user_id = m.from_user.id
    text = m.text

    # Режим ввода ключа
    if user_id in temp_auth and temp_auth[user_id].get("step") == "enter_key":
        await process_key_input(c, m)
        return

    # Режим добавления аккаунта
    if user_id in temp_auth and temp_auth[user_id].get("step") == "phone":
        await process_phone_input(c, m)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "code":
        await process_code_input(c, m)
        return
    if user_id in temp_auth and temp_auth[user_id].get("step") == "password":
        await process_password_input(c, m)
        return

    # Режим смены текста
    if user_id in temp_auth and temp_auth[user_id].get("step") == "change_text":
        new_text = text.strip()
        for acc in users_data[user_id]["accounts"].values():
            acc["text"] = new_text
        save_users()
        await m.reply("✅ Текст рассылки обновлён для всех аккаунтов.", reply_markup=get_main_keyboard(user_id))
        temp_auth.pop(user_id)
        return

    # Режим смены интервала
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

    # Режим ввода текстов для безопасного режима (ожидаем 3-5 строк)
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

    # Режим ввода базового интервала для безопасного режима
    if user_id in temp_auth and temp_auth[user_id].get("step") == "safe_interval":
        try:
            base_int = int(text)
            if base_int < 60:
                await m.reply("❌ Интервал должен быть не менее 60 секунд.")
                return
            temp_auth[user_id]["base_interval"] = base_int
            # Запускаем безопасную рассылку для всех аккаунтов
            accounts = users_data[user_id]["accounts"]
            if not accounts:
                await m.reply("❌ Нет добавленных аккаунтов.")
                temp_auth.pop(user_id)
                return
            for phone, acc in accounts.items():
                acc["safe_mode"] = True
                acc["texts_list"] = temp_auth[user_id]["texts_list"]
                acc["base_interval"] = base_int
                acc["text"] = temp_auth[user_id]["texts_list"][0]  # на всякий случай
                if acc.get("running", False):
                    acc["running"] = False
                if "client" not in acc:
                    await reconnect_account(user_id, phone)
                    await asyncio.sleep(2)
                if "client" in acc:
                    acc["running"] = True
                    asyncio.create_task(safe_spam_cycle(user_id, phone, acc, m))
            save_users()
            await m.reply(f"🛡 Безопасная рассылка запущена для {len(accounts)} аккаунтов.\nБазовый интервал: {base_int} сек.\nРеальный интервал: 55-70 мин.", reply_markup=get_main_keyboard(user_id))
            temp_auth.pop(user_id)
        except ValueError:
            await m.reply("❌ Введите число.")
        return

    # Если нет активного режима, просто показываем главное меню
    if user_id in users_data:
        await m.reply("Используйте кнопки меню.", reply_markup=get_main_keyboard(user_id))
    else:
        await m.reply("Введите /start для начала.")

async def process_key_input(c, m):
    user_id = m.from_user.id
    raw_key = m.text.strip()
    username = m.from_user.username or ""

    # Парсим привязку
    key, bound_username = parse_key_with_username(raw_key)
    current_keys = load_keys()

    if key not in current_keys:
        await m.reply("❌ Неверный ключ доступа!")
        return

    key_info = current_keys[key]
    # key_info может быть кортежем (описание, дни, is_admin?) или строкой для совместимости
    if isinstance(key_info, tuple):
        if len(key_info) == 2:
            desc, days = key_info
            is_admin_key = False
        else:
            desc, days, is_admin_key = key_info
    else:
        desc = key_info
        days = 30
        is_admin_key = (key == "ADMIN_FOREVER")

    # Проверяем привязку, если ключ привязан к username
    if bound_username:
        if bound_username.lower() != username.lower():
            await m.reply(f"❌ Этот ключ привязан к @{bound_username}")
            return

    # Проверяем, не использован ли ключ
    for uid, udata in users_data.items():
        if udata["key_used"] == key:
            await m.reply("❌ Этот ключ уже был использован!")
            return

    # Создаём пользователя
    expires = datetime.now() + timedelta(days=days)
    users_data[user_id] = {
        "expires": expires.isoformat(),
        "key_used": key,
        "is_admin": is_admin_key,
        "username": username or m.from_user.first_name,
        "bound_username": bound_username or "",
        "accounts": {}
    }
    save_users()
    role = "👑 Администратор" if is_admin_key else "👤 Пользователь"
    await m.reply(
        f"✅ *Доступ предоставлен!*\n\n{role}\nКлюч: `{key}`\nСрок действия до: {expires.strftime('%d.%m.%Y')}\n\nИспользуйте /start для входа.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=get_main_keyboard(user_id)
    )
    temp_auth.pop(user_id, None)

# --- ДОБАВЛЕНИЕ АККАУНТА (телефон, код, пароль) ---
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

# --- CALLBACK'И ---
@bot.on_callback_query()
async def handle_callback(c: Client, query: CallbackQuery):
    user_id = query.from_user.id
    data = query.data

    if user_id not in users_data:
        await query.answer("❌ Сначала получите доступ через /start", show_alert=True)
        return

    if data == "profile":
        await show_profile(query)
    elif data == "start_menu":
        await query.message.edit_reply_markup(reply_markup=get_start_menu_keyboard())
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
    elif data == "access_info":
        await show_access_info(query)
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
        await query.message.reply("📸 Отправьте новое приветственное фото (как обычное изображение).")
        await query.answer()
    elif data == "back_to_main":
        await query.message.edit_reply_markup(reply_markup=get_main_keyboard(user_id))
    else:
        await query.answer("⛔ Недостаточно прав", show_alert=True)

async def show_profile(query: CallbackQuery):
    user_id = query.from_user.id
    data = users_data[user_id]
    accounts = data["accounts"]
    total = len(accounts)
    running = sum(1 for a in accounts.values() if a.get("running", False))

    text = f"👤 *Мой профиль*\n\n"
    text += f"🆔 ID: `{user_id}`\n"
    text += f"👤 Имя: {data.get('username', 'Не указано')}\n"
    if data.get('bound_username'):
        text += f"🔗 Привязан к: @{data['bound_username']}\n"
    # Убираем строку "Админ: Да/Нет"
    text += f"📱 Аккаунтов: {total}/{MAX_ACCOUNTS_PER_USER}\n"
    text += f"🟢 Активных рассылок: {running}\n"
    text += f"📅 Доступ до: {datetime.fromisoformat(data['expires']).strftime('%d.%m.%Y')}\n\n"

    if accounts:
        text += "📋 *Список аккаунтов* (без смайликов):\n"
        for i, (phone, acc) in enumerate(accounts.items(), 1):
            status = "Активен" if acc.get("running", False) else "Остановлен"
            client_ok = "✅" if "client" in acc else "❌"
            safe_mark = "🛡" if acc.get("safe_mode", False) else ""
            text += f"{i}. {phone} {client_ok} {status} {safe_mark}\n"
            text += f"   Текст: {acc['text'][:40]}...\n"
            text += f"   Интервал: {acc['interval']} сек.\n"
    else:
        text += "📭 Нет добавленных аккаунтов.\n"

    # Кнопка добавления аккаунта прямо в профиле
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")]
    ])
    await query.message.edit_text(text, reply_markup=kb, parse_mode=enums.ParseMode.MARKDOWN)

@bot.on_callback_query(filters.regex("add_account"))
async def add_account_callback(c, query):
    user_id = query.from_user.id
    if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
        await query.answer(f"❌ Лимит {MAX_ACCOUNTS_PER_USER} аккаунтов", show_alert=True)
        return
    temp_auth[user_id] = {"step": "phone", "user_id": user_id}
    await query.message.reply("📱 Введите номер телефона в международном формате (например, +380123456789):")
    await query.answer()

async def start_normal_ras(query: CallbackQuery):
    user_id = query.from_user.id
    # Проверяем подписку
    if not has_active_subscription(user_id):
        await query.answer("❌ Ваша подписка истекла! Для запуска рассылки продлите доступ.", show_alert=True)
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
    await query.message.reply(f"🚀 Запущено обычных рассылок: {started}")
    await query.answer()

async def start_safe_mode(query: CallbackQuery):
    user_id = query.from_user.id
    if not has_active_subscription(user_id):
        await query.answer("❌ Подписка истекла! Для безопасного режима нужна активная подписка.", show_alert=True)
        return
    accounts = users_data[user_id]["accounts"]
    if not accounts:
        await query.answer("❌ Нет аккаунтов", show_alert=True)
        return

    # Запрашиваем у пользователя 3-5 текстов
    temp_auth[user_id] = {"step": "safe_texts"}
    await query.message.reply(
        "🛡 *Безопасный режим*\n\n"
        "Отправьте от 3 до 5 текстов (каждый с новой строки).\n"
        "Бот будет случайным образом выбирать один из них при каждой отправке.\n\n"
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
    await query.message.reply(f"🛑 Остановлено рассылок: {stopped}")
    await query.answer()

async def show_access_info(query: CallbackQuery):
    user_id = query.from_user.id
    data = users_data[user_id]
    expires = datetime.fromisoformat(data["expires"])
    days_left = (expires - datetime.now()).days
    text = (
        f"🔑 *Информация о доступе*\n\n"
        f"✅ Доступ {'активен' if has_active_subscription(user_id) else 'ИСТЁК'}\n"
        f"🔑 Ключ: `{data['key_used']}`\n"
        f"📅 Истекает: {expires.strftime('%d.%m.%Y')}\n"
        f"⏳ Осталось дней: {days_left}\n"
        f"👑 Права: {'Администратор' if is_admin(user_id) else 'Пользователь'}\n\n"
        f"*Рассылка доступна только при активной подписке.*"
    )
    await query.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=enums.ParseMode.MARKDOWN)

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
        await query.message.edit_text(text, parse_mode=enums.ParseMode.MARKDOWN)
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
    text += "На месяц: `MONTH_KEY_001`, `MONTH_KEY_002`\n"
    text += "На год: `YEAR_KEY_001`, `YEAR_KEY_002`\n"
    text += "Навсегда: `FOREVER_KEY_001`, `FOREVER_KEY_002`\n"
    text += "Админ: `ADMIN_FOREVER`"
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

# --- ФУНКЦИЯ ГРАЦИОЗНОГО ЗАВЕРШЕНИЯ ---
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

# --- ЗАПУСК ---
if __name__ == "__main__":
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig)))

    os.makedirs(os.path.join(WORK_DIR, "sessions"), exist_ok=True)
    os.makedirs(os.path.join(WORK_DIR, "user_settings"), exist_ok=True)
    os.makedirs(os.path.join(WORK_DIR, "bot_session"), exist_ok=True)

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
        await load_user_sessions()  # загружаем сессии (функция осталась из оригинального кода)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(startup())
    logger.info("🤖 Бот запущен")
    try:
        bot.run()
    except (KeyboardInterrupt, SystemExit):
        loop.run_until_complete(shutdown())
