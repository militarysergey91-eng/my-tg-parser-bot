import asyncio
import logging
from datetime import datetime, timedelta, timezone
import os
import json
import re
import time
import sys
import signal
from PIL import Image

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.types import ParseMode, ContentType, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils import executor
from docx import Document
from docx.shared import Inches, Pt, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeExpiredError, PhoneCodeInvalidError
from telethon.sessions import StringSession
import pandas as pd
import openpyxl

# ========== НАСТРОЙКИ ==========
API_TOKEN = '8029293386:AAG-Hih0eVun77YYcY8zf6PyDjQERmQCx9w'  # Твой токен

# Данные от Telegram API
API_ID = 38892524
API_HASH = 'd71ef1a657ab20d2a47a52130626c939'

# Файл для хранения списка каналов
CHANNELS_FILE = 'saved_channels.json'

# Файл для хранения сессий в текстовом формате (вместо SQLite)
SESSIONS_DIR = 'telegram_sessions'

# Папка для временного хранения изображений
IMAGES_DIR = 'temp_images'

# ID администратора (твой ID)
ADMIN_ID = 5224743551

# Создаем папки, если их нет
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(IMAGES_DIR, exist_ok=True)

# ========== ПРЕДОТВРАЩЕНИЕ КОНФЛИКТОВ ==========
def signal_handler(sig, frame):
    print('Остановка бота...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ========== ИНИЦИАЛИЗАЦИЯ ==========
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
logging.basicConfig(level=logging.INFO)

# Временное хранилище для данных пользователя
user_data = {}
auth_data = {}  # Для хранения временных данных авторизации (только для админа)
session_strings = {}  # Храним сессии в памяти
stop_flags = {}  # Флаги для остановки формирования отчета

# Глобальная сессия для всех пользователей
MASTER_SESSION = None

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С ФАЙЛАМИ ==========
def load_channels():
    """Загружает сохраненные каналы из JSON"""
    if os.path.exists(CHANNELS_FILE):
        with open(CHANNELS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_channels(channels):
    """Сохраняет каналы в JSON"""
    with open(CHANNELS_FILE, 'w', encoding='utf-8') as f:
        json.dump(channels, f, ensure_ascii=False, indent=2)

def extract_channel_name(url):
    """Извлекает имя канала из ссылки"""
    match = re.search(r't\.me/(?:s/)?([a-zA-Z0-9_]+)', url)
    if match:
        return match.group(1)
    return None

def safe_remove_file(filename):
    """Безопасное удаление файла с повторными попытками"""
    for i in range(5):
        try:
            if os.path.exists(filename):
                os.remove(filename)
                return True
        except PermissionError:
            time.sleep(0.5)
        except Exception:
            pass
    return False

def save_session_string(session_string):
    """Сохраняет мастер-сессию в текстовый файл"""
    global MASTER_SESSION
    try:
        session_file = os.path.join(SESSIONS_DIR, f'master_session.txt')
        with open(session_file, 'w', encoding='utf-8') as f:
            f.write(session_string)
        MASTER_SESSION = session_string
        return True
    except Exception as e:
        print(f"Ошибка сохранения сессии: {e}")
        return False

def load_master_session():
    """Загружает мастер-сессию из текстового файла"""
    global MASTER_SESSION
    try:
        session_file = os.path.join(SESSIONS_DIR, f'master_session.txt')
        if os.path.exists(session_file):
            with open(session_file, 'r', encoding='utf-8') as f:
                MASTER_SESSION = f.read().strip()
                return MASTER_SESSION
    except Exception as e:
        print(f"Ошибка загрузки сессии: {e}")
    return None

def remove_master_session():
    """Удаляет мастер-сессию"""
    global MASTER_SESSION
    try:
        session_file = os.path.join(SESSIONS_DIR, f'master_session.txt')
        if os.path.exists(session_file):
            os.remove(session_file)
        MASTER_SESSION = None
        return True
    except Exception:
        pass
    return False

def cleanup_temp_files(user_id):
    """Очищает временные файлы пользователя"""
    try:
        for file in os.listdir(IMAGES_DIR):
            if file.startswith(f"img_{user_id}_"):
                os.remove(os.path.join(IMAGES_DIR, file))
    except Exception as e:
        print(f"Ошибка очистки временных файлов: {e}")

async def download_media(message, user_id, client):
    """Скачивает медиафайл из сообщения и сохраняет локально"""
    try:
        if message.media:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            file_extension = '.jpg'
            
            if hasattr(message.media, 'document') and message.media.document:
                doc = message.media.document
                if doc.attributes:
                    for attr in doc.attributes:
                        if hasattr(attr, 'file_name') and attr.file_name:
                            file_extension = os.path.splitext(attr.file_name)[1]
                            break
                if not file_extension:
                    mime_to_ext = {
                        'image/jpeg': '.jpg',
                        'image/png': '.png',
                        'image/gif': '.gif',
                        'video/mp4': '.mp4',
                        'video/quicktime': '.mov',
                        'application/pdf': '.pdf',
                        'application/msword': '.doc',
                        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx'
                    }
                    file_extension = mime_to_ext.get(doc.mime_type, '.bin')
            
            local_filename = f"img_{user_id}_{timestamp}{file_extension}"
            local_path = os.path.join(IMAGES_DIR, local_filename)
            
            path = await message.download_media(file=local_path)
            return path if os.path.exists(path) else None
    except Exception as e:
        print(f"Ошибка скачивания медиа: {e}")
    return None

def add_image_to_doc(doc, image_path, max_width_inches=6):
    """Добавляет изображение в документ Word"""
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            aspect = height / width
            doc_width = min(max_width_inches, width / 96)
            doc_height = doc_width * aspect
            doc.add_picture(image_path, width=Cm(doc_width * 2.54))
            caption = doc.add_paragraph()
            caption.alignment = WD_ALIGN_PARAGRAPH.CENTER
            caption.add_run(f"Изображение").italic = True
            return True
    except Exception as e:
        print(f"Ошибка добавления изображения в документ: {e}")
        return False

# ========== ФУНКЦИИ ДЛЯ ПРОВЕРКИ АДМИНА ==========
def is_admin(user_id):
    """Проверяет, является ли пользователь администратором"""
    return user_id == ADMIN_ID

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(user_id):
    """Главная клавиатура с кнопками (разная для админа и обычных пользователей)"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    # Общие кнопки для всех
    keyboard.add(
        KeyboardButton("📋 Мои каналы"),
        KeyboardButton("➕ Добавить канал"),
        KeyboardButton("🔍 Поиск по словам"),
        KeyboardButton("🔄 Собрать всё"),
        KeyboardButton("❓ Помощь"),
        KeyboardButton("⏹️ Стоп")
    )
    
    # Кнопки только для админа
    if is_admin(user_id):
        keyboard.add(
            KeyboardButton("🔄 Сменить аккаунт"),
            KeyboardButton("🚪 Разлогиниться"),
            KeyboardButton("👤 Мой аккаунт")
        )
    
    return keyboard

def get_period_keyboard():
    """Клавиатура для выбора периода (одинаковая для всех)"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🕐 1 час"),
        KeyboardButton("🕒 3 часа"),
        KeyboardButton("🕖 7 часов"),
        KeyboardButton("📅 24 часа"),
        KeyboardButton("📆 3 дня"),
        KeyboardButton("📆 7 дней"),
        KeyboardButton("◀️ Назад в меню")
    )
    return keyboard

def get_image_option_keyboard():
    """Клавиатура для выбора - сохранять изображения или нет"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🖼️ С картинками"),
        KeyboardButton("📝 Только текст"),
        KeyboardButton("◀️ Назад в меню")
    )
    return keyboard

def get_auth_keyboard():
    """Клавиатура для процесса авторизации (только для админа)"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    keyboard.add(
        KeyboardButton("❌ Отменить авторизацию"),
        KeyboardButton("◀️ Назад в меню")
    )
    return keyboard

# ========== КОМАНДЫ ==========
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Обработка команды /start"""
    user_id = message.from_user.id
    
    welcome_text = (
        "👋 Добро пожаловать!\n\n"
        "Я бот для сбора информации из Telegram-каналов.\n\n"
        "Как пользоваться:\n"
        "1️⃣ Нажми '➕ Добавить канал' и отправь ссылку на канал\n"
        "2️⃣ Добавь несколько каналов\n"
        "3️⃣ Нажми '🔍 Поиск по словам' для поиска по ключевым словам\n"
        "4️⃣ Или '🔄 Собрать всё' для получения всех постов\n"
        "5️⃣ Выбери период времени (1 час, 3 часа, 7 дней и т.д.)\n"
        "6️⃣ Выбери формат отчета: с картинками или только текст\n\n"
        "⏹️ Остановить формирование отчета - нажми '⏹️ Стоп'\n"
    )
    
    # Добавляем информацию для админа
    if is_admin(user_id):
        welcome_text += (
            "\n👑 Вы администратор. Доступны дополнительные функции:\n"
            "• 🔄 Сменить аккаунт - авторизоваться под другим номером\n"
            "• 🚪 Разлогиниться - выйти из аккаунта\n"
            "• 👤 Мой аккаунт - информация о текущем аккаунте\n"
        )
    
    # Проверяем наличие мастер-сессии
    global MASTER_SESSION
    if not MASTER_SESSION:
        MASTER_SESSION = load_master_session()
    
    if not MASTER_SESSION and is_admin(user_id):
        welcome_text += (
            "\n⚠️ Мастер-сессия не найдена!\n"
            "Нажми '🔍 Поиск по словам' для авторизации."
        )
    
    await message.reply(welcome_text, reply_markup=get_main_keyboard(user_id))

@dp.message_handler(commands=['reset'])
async def cmd_reset(message: types.Message):
    """Сброс сессии и авторизации (только для админа)"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.reply("❌ Эта команда доступна только администратору.")
        return
    
    # Останавливаем текущий процесс если есть
    if user_id in stop_flags:
        stop_flags[user_id] = True
    
    # Закрываем все клиенты для админа
    if user_id in auth_data and 'client' in auth_data[user_id]:
        try:
            await auth_data[user_id]['client'].disconnect()
        except:
            pass
    
    # Удаляем мастер-сессию
    remove_master_session()
    
    # Очищаем временные файлы
    cleanup_temp_files(user_id)
    
    # Очищаем данные авторизации
    if user_id in auth_data:
        del auth_data[user_id]
    
    await message.reply(
        "🗑 Мастер-сессия полностью сброшена!\n\n"
        "При следующем использовании нужно будет авторизоваться заново.",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(commands=['debug'])
async def cmd_debug(message: types.Message):
    """Отладка - показать состояние"""
    user_id = message.from_user.id
    
    if is_admin(user_id):
        session_file = os.path.join(SESSIONS_DIR, f'master_session.txt')
        session_exists = os.path.exists(session_file)
        
        in_auth = user_id in auth_data
        auth_state = auth_data[user_id].get('state') if in_auth else None
        is_stopping = user_id in stop_flags and stop_flags[user_id]
        
        session_size = os.path.getsize(session_file) if session_exists else 0
        
        debug_text = (
            f"🔍 Отладка (админ)\n\n"
            f"📁 Мастер-сессия: {'✅ есть' if session_exists else '❌ нет'} ({session_size} байт)\n"
            f"🔄 В процессе авторизации: {'✅ да' if in_auth else '❌ нет'}\n"
            f"📌 Состояние: {auth_state if auth_state else '-'}\n"
            f"⏹️ Процесс остановлен: {'✅ да' if is_stopping else '❌ нет'}\n"
            f"🆔 User ID: {user_id}"
        )
    else:
        is_stopping = user_id in stop_flags and stop_flags[user_id]
        debug_text = (
            f"🔍 Отладка\n\n"
            f"⏹️ Процесс остановлен: {'✅ да' if is_stopping else '❌ нет'}\n"
            f"🆔 User ID: {user_id}"
        )
    
    await message.reply(debug_text)

@dp.message_handler(commands=['checksession'])
async def cmd_checksession(message: types.Message):
    """Проверка сессии"""
    user_id = message.from_user.id
    
    global MASTER_SESSION
    if not MASTER_SESSION:
        MASTER_SESSION = load_master_session()
    
    if not MASTER_SESSION:
        await message.reply("❌ Мастер-сессия не найдена. Обратитесь к администратору.")
        return
    
    if is_admin(user_id):
        await message.reply("🔍 Проверяю мастер-сессию...")
        
        try:
            client = TelegramClient(StringSession(MASTER_SESSION), API_ID, API_HASH)
            await client.connect()
            
            if await client.is_user_authorized():
                me = await client.get_me()
                await message.reply(
                    f"✅ Мастер-сессия работает!\n\n"
                    f"Имя: {me.first_name}\n"
                    f"Username: @{me.username}\n"
                    f"ID: {me.id}"
                )
                
                new_session_string = client.session.save()
                save_session_string(new_session_string)
            else:
                await message.reply("❌ Мастер-сессия есть, но не активна. Нужно авторизоваться заново.")
                remove_master_session()
            
            await client.disconnect()
        except Exception as e:
            await message.reply(f"❌ Ошибка при проверке: {str(e)}")
            remove_master_session()
    else:
        await message.reply("✅ Мастер-сессия активна. Бот готов к работе.")

@dp.message_handler(lambda message: message.text == "📋 Мои каналы")
async def show_channels(message: types.Message):
    """Показывает список сохраненных каналов"""
    user_id = message.from_user.id
    
    channels = load_channels()
    
    if not channels:
        await message.reply(
            "📭 У тебя пока нет каналов\n\n"
            "Нажми '➕ Добавить канал' и отправь ссылку на Telegram-канал",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    for i, channel in enumerate(channels):
        btn_text = f"{channel['name']}"
        keyboard.add(InlineKeyboardButton(btn_text, callback_data=f"delete_{i}"))
    
    keyboard.add(InlineKeyboardButton("❌ Удалить все", callback_data="delete_all"))
    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    
    text = f"📋 Твои каналы:\n\n"
    for i, channel in enumerate(channels, 1):
        text += f"{i}. {channel['name']} - {channel['url']}\n"
    
    text += f"\nВсего каналов: {len(channels)}\n\n"
    text += "Нажми на канал чтобы удалить его"
    
    await message.reply(text, reply_markup=keyboard)

@dp.message_handler(lambda message: message.text == "➕ Добавить канал")
async def add_channel_prompt(message: types.Message):
    """Запрашивает ссылку на канал"""
    user_id = message.from_user.id
    
    await message.reply(
        "🔗 Отправь ссылку на Telegram-канал\n\n"
        "Примеры:\n"
        "• https://t.me/durov\n"
        "• https://t.me/s/breakingmash\n"
        "• @durov\n\n"
        "Я сам определю название канала!"
    )
    
    user_data[user_id] = {'state': 'waiting_channel_link'}

@dp.message_handler(lambda message: message.text == "🔍 Поиск по словам")
async def search_by_keywords(message: types.Message):
    """Поиск по ключевым словам"""
    user_id = message.from_user.id
    
    global MASTER_SESSION
    if not MASTER_SESSION:
        MASTER_SESSION = load_master_session()
    
    if not MASTER_SESSION:
        if is_admin(user_id):
            await message.reply(
                "🔄 Мастер-сессия не найдена. Начинаю процесс авторизации...",
                reply_markup=get_auth_keyboard()
            )
            await start_authorization(user_id, message)
        else:
            await message.reply(
                "❌ Мастер-сессия не найдена. Обратитесь к администратору.",
                reply_markup=get_main_keyboard(user_id)
            )
        return
    
    channels = load_channels()
    
    if not channels:
        await message.reply(
            "❌ Сначала добавь каналы через '➕ Добавить канал'",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    await message.reply(
        "⏱ Выбери период времени\n\n"
        "За какой период собирать посты?",
        reply_markup=get_period_keyboard()
    )
    
    user_data[user_id] = {'state': 'waiting_period'}

@dp.message_handler(lambda message: message.text == "🔄 Собрать всё")
async def collect_all(message: types.Message):
    """Собрать все посты без фильтра"""
    user_id = message.from_user.id
    
    global MASTER_SESSION
    if not MASTER_SESSION:
        MASTER_SESSION = load_master_session()
    
    if not MASTER_SESSION:
        if is_admin(user_id):
            await message.reply(
                "🔄 Мастер-сессия не найдена. Начинаю процесс авторизации...",
                reply_markup=get_auth_keyboard()
            )
            await start_authorization(user_id, message)
        else:
            await message.reply(
                "❌ Мастер-сессия не найдена. Обратитесь к администратору.",
                reply_markup=get_main_keyboard(user_id)
            )
        return
    
    channels = load_channels()
    
    if not channels:
        await message.reply(
            "❌ Сначала добавь каналы через '➕ Добавить канал'",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    await message.reply(
        "⏱ Выбери период времени\n\n"
        "За какой период собирать все посты?",
        reply_markup=get_period_keyboard()
    )
    
    user_data[user_id] = {
        'state': 'waiting_period',
        'keywords': 'все'
    }

@dp.message_handler(lambda message: message.text == "❓ Помощь")
async def show_help(message: types.Message):
    """Показывает справку"""
    user_id = message.from_user.id
    
    help_text = (
        "❓ Помощь\n\n"
        "📋 Мои каналы - посмотреть добавленные каналы\n"
        "➕ Добавить канал - добавить новый канал по ссылке\n"
        "🔍 Поиск по словам - найти посты по ключевым словам\n"
        "🔄 Собрать всё - получить все посты\n"
        "⏹️ Стоп - остановить формирование отчета\n\n"
        "После выбора периода появится выбор:\n"
        "🖼️ С картинками - сохранить изображения в отчет\n"
        "📝 Только текст - только текст, без картинок\n\n"
        "Как удалить канал:\n"
        "1. Нажми '📋 Мои каналы'\n"
        "2. Нажми на название канала в списке\n\n"
        "Команды:\n"
        "/debug - диагностика\n"
        "/checksession - проверка сессии"
    )
    
    if is_admin(user_id):
        help_text += (
            "\n\n👑 Команды администратора:\n"
            "🔄 Сменить аккаунт - авторизоваться под другим номером\n"
            "🚪 Разлогиниться - выйти из аккаунта\n"
            "👤 Мой аккаунт - информация о текущем аккаунте\n"
            "/reset - сброс авторизации"
        )
    
    await message.reply(help_text, reply_markup=get_main_keyboard(user_id))

@dp.message_handler(lambda message: message.text == "❌ Отменить авторизацию")
async def cancel_auth(message: types.Message):
    """Отмена процесса авторизации (только для админа)"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        return
    
    if user_id in auth_data:
        if 'client' in auth_data[user_id]:
            try:
                await auth_data[user_id]['client'].disconnect()
            except:
                pass
        del auth_data[user_id]
    
    await message.reply(
        "❌ Авторизация отменена.",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.from_user.id in user_data and user_data[message.from_user.id].get('state') == 'waiting_period')
async def process_period(message: types.Message):
    """Обработка выбора периода"""
    user_id = message.from_user.id
    period_text = message.text
    
    period_hours = 0
    
    if period_text == "🕐 1 час":
        period_hours = 1
    elif period_text == "🕒 3 часа":
        period_hours = 3
    elif period_text == "🕖 7 часов":
        period_hours = 7
    elif period_text == "📅 24 часа":
        period_hours = 24
    elif period_text == "📆 3 дня":
        period_hours = 72
    elif period_text == "📆 7 дней":
        period_hours = 168
    elif period_text == "◀️ Назад в меню":
        await message.reply("Главное меню:", reply_markup=get_main_keyboard(user_id))
        del user_data[user_id]
        return
    else:
        await message.reply(
            "❌ Пожалуйста, выбери период из кнопок",
            reply_markup=get_period_keyboard()
        )
        return
    
    user_data[user_id]['period_hours'] = period_hours
    
    await message.reply(
        "🖼️ Выбери формат отчета:\n\n"
        "• С картинками - будут сохранены все изображения\n"
        "• Только текст - только текст, без картинок (быстрее)",
        reply_markup=get_image_option_keyboard()
    )
    
    user_data[user_id]['state'] = 'waiting_image_option'

@dp.message_handler(lambda message: message.from_user.id in user_data and user_data[message.from_user.id].get('state') == 'waiting_image_option')
async def process_image_option(message: types.Message):
    """Обработка выбора - с картинками или без"""
    user_id = message.from_user.id
    option = message.text
    
    if option == "◀️ Назад в меню":
        await message.reply("Главное меню:", reply_markup=get_main_keyboard(user_id))
        del user_data[user_id]
        return
    elif option not in ["🖼️ С картинками", "📝 Только текст"]:
        await message.reply(
            "❌ Пожалуйста, выбери вариант из кнопок",
            reply_markup=get_image_option_keyboard()
        )
        return
    
    save_images = (option == "🖼️ С картинками")
    user_data[user_id]['save_images'] = save_images
    
    if 'keywords' in user_data[user_id]:
        channels = load_channels()
        user_data[user_id]['channels'] = channels
        user_data[user_id]['state'] = 'collecting'
        
        period_hours = user_data[user_id]['period_hours']
        if period_hours == 1:
            period_text = "1 час"
        elif period_hours == 3:
            period_text = "3 часа"
        elif period_hours == 7:
            period_text = "7 часов"
        elif period_hours == 24:
            period_text = "24 часа"
        elif period_hours == 72:
            period_text = "3 дня"
        else:
            period_text = "7 дней"
        
        await message.reply(
            f"🔄 Начинаю сбор всех постов за {period_text}\n"
            f"Формат: {'с картинками' if save_images else 'только текст'}",
            reply_markup=get_main_keyboard(user_id)
        )
        
        await collect_and_send_report(user_id)
    else:
        await message.reply(
            f"🔍 Введи ключевые слова для поиска\n\n"
            f"Формат: {'с картинками' if save_images else 'только текст'}\n\n"
            f"Например: новости, вакансии, работа\n"
            f"Или отправь 'все' для поиска без фильтра"
        )
        
        user_data[user_id]['state'] = 'waiting_keywords'

# ========== ФУНКЦИИ АВТОРИЗАЦИИ (только для админа) ==========
async def start_authorization(user_id, message):
    """Начинает процесс авторизации"""
    if not is_admin(user_id):
        return
    
    try:
        if user_id in auth_data and 'client' in auth_data[user_id]:
            try:
                await auth_data[user_id]['client'].disconnect()
            except:
                pass
        
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        
        auth_data[user_id] = {
            'client': client,
            'state': 'waiting_phone'
        }
        
        await message.reply(
            "📱 Введи номер телефона\n\n"
            "Формат: +7XXXXXXXXXX\n"
            "Например: +79123456789\n\n"
            "Или нажми '❌ Отменить авторизацию' для отмены.",
            reply_markup=get_auth_keyboard()
        )
        
    except Exception as e:
        await message.reply(f"❌ Ошибка при запуске авторизации: {str(e)}")
        if user_id in auth_data:
            del auth_data[user_id]

async def start_authorization_without_message(user_id):
    """Начинает процесс авторизации без объекта message"""
    if not is_admin(user_id):
        return
    
    try:
        if user_id in auth_data and 'client' in auth_data[user_id]:
            try:
                await auth_data[user_id]['client'].disconnect()
            except:
                pass
        
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        
        auth_data[user_id] = {
            'client': client,
            'state': 'waiting_phone'
        }
        
        await bot.send_message(
            user_id, 
            "📱 Введи номер телефона\n\n"
            "Формат: +7XXXXXXXXXX\n"
            "Например: +79123456789\n\n"
            "Или нажми '❌ Отменить авторизацию' для отмены.",
            reply_markup=get_auth_keyboard()
        )
        
    except Exception as e:
        await bot.send_message(user_id, f"❌ Ошибка при запуске авторизации: {str(e)}")
        if user_id in auth_data:
            del auth_data[user_id]

@dp.message_handler(lambda message: message.from_user.id in auth_data and auth_data[message.from_user.id].get('state') == 'waiting_phone')
async def process_phone(message: types.Message):
    """Обработка ввода номера телефона"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        return
    
    phone = message.text.strip()
    
    if phone == "❌ Отменить авторизацию":
        await cancel_auth(message)
        return
    
    phone = re.sub(r'[^\d+]', '', phone)
    
    status_msg = await message.reply("📲 Отправляю запрос на код подтверждения...")
    
    try:
        client = auth_data[user_id]['client']
        result = await client.send_code_request(phone)
        
        auth_data[user_id]['phone'] = phone
        auth_data[user_id]['phone_code_hash'] = result.phone_code_hash
        auth_data[user_id]['state'] = 'waiting_code'
        
        await status_msg.delete()
        await message.reply(
            "🔐 Код подтверждения отправлен!\n\n"
            "✅ Проверь Telegram на телефоне\n"
            "✅ Код должен прийти в личные сообщения\n"
            "✅ Введи код (только цифры)\n\n"
            "Код действует ограниченное время. Введи его быстро!\n\n"
            "Или нажми '❌ Отменить авторизацию' для отмены.",
            reply_markup=get_auth_keyboard()
        )
        
    except Exception as e:
        await status_msg.delete()
        error_text = str(e)
        
        if "FLOOD_WAIT" in error_text:
            wait_time = re.search(r'(\d+)', error_text)
            wait = wait_time.group(1) if wait_time else "несколько"
            await message.reply(
                f"⏳ Слишком много попыток\n\n"
                f"Нужно подождать {wait} секунд",
                reply_markup=get_main_keyboard(user_id)
            )
        elif "PHONE_NUMBER_INVALID" in error_text:
            await message.reply(
                "❌ Неверный формат номера\n\n"
                "Используй формат: +79123456789\n\n"
                "Попробуй снова:",
                reply_markup=get_auth_keyboard()
            )
            return
        else:
            await message.reply(
                f"❌ Ошибка: {error_text[:200]}",
                reply_markup=get_main_keyboard(user_id)
            )
        
        await client.disconnect()
        if user_id in auth_data:
            del auth_data[user_id]

@dp.message_handler(lambda message: message.from_user.id in auth_data and auth_data[message.from_user.id].get('state') == 'waiting_code')
async def process_code(message: types.Message):
    """Обработка ввода кода подтверждения"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        return
    
    code = message.text.strip()
    
    if code == "❌ Отменить авторизацию":
        await cancel_auth(message)
        return
    
    code = re.sub(r'\D', '', code)
    
    status_msg = await message.reply(f"🔐 Проверяю код...")
    
    try:
        client = auth_data[user_id]['client']
        phone = auth_data[user_id]['phone']
        
        await client.sign_in(phone, code)
        
        await status_msg.delete()
        
        session_string = client.session.save()
        save_session_string(session_string)
        
        me = await client.get_me()
        
        await message.reply(
            f"✅ *Авторизация успешна!*\n\n"
            f"Мастер-аккаунт: {me.first_name}\n"
            f"Username: @{me.username}\n"
            f"Телефон: {phone}\n\n"
            f"Теперь все пользователи могут пользоваться ботом.\n"
            f"Нажми '🔍 Поиск по словам' или '🔄 Собрать всё'",
            parse_mode='Markdown',
            reply_markup=get_main_keyboard(user_id)
        )
        
        if user_id in auth_data:
            del auth_data[user_id]
        
    except SessionPasswordNeededError:
        await status_msg.delete()
        auth_data[user_id]['state'] = 'waiting_password'
        await message.reply(
            "🔐 Требуется пароль двухфакторной аутентификации\n\n"
            "Введи свой пароль:",
            reply_markup=get_auth_keyboard()
        )
    except PhoneCodeExpiredError:
        await status_msg.delete()
        await message.reply(
            "❌ Код истек\n\n"
            "Нажми '🔄 Сменить аккаунт' и попробуй снова, или введи номер заново:",
            reply_markup=get_auth_keyboard()
        )
        auth_data[user_id]['state'] = 'waiting_phone'
    except PhoneCodeInvalidError:
        await status_msg.delete()
        await message.reply(
            "❌ Неверный код!\n\n"
            "Попробуй ввести ещё раз (только цифры):",
            reply_markup=get_auth_keyboard()
        )
    except Exception as e:
        await status_msg.delete()
        error_text = str(e)
        
        if "CODE_INVALID" in error_text:
            await message.reply(
                "❌ Неверный код!\n\n"
                "Попробуй ввести ещё раз (только цифры):",
                reply_markup=get_auth_keyboard()
            )
        elif "CODE_EXPIRED" in error_text:
            await message.reply(
                "❌ Код истек\n\n"
                "Введи номер телефона заново:",
                reply_markup=get_auth_keyboard()
            )
            auth_data[user_id]['state'] = 'waiting_phone'
        else:
            await message.reply(
                f"❌ Ошибка: {error_text[:200]}",
                reply_markup=get_main_keyboard(user_id)
            )
            await client.disconnect()
            if user_id in auth_data:
                del auth_data[user_id]

@dp.message_handler(lambda message: message.from_user.id in auth_data and auth_data[message.from_user.id].get('state') == 'waiting_password')
async def process_password(message: types.Message):
    """Обработка ввода пароля двухфакторки"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        return
    
    password = message.text.strip()
    
    if password == "❌ Отменить авторизацию":
        await cancel_auth(message)
        return
    
    try:
        client = auth_data[user_id]['client']
        await client.sign_in(password=password)
        
        session_string = client.session.save()
        save_session_string(session_string)
        
        me = await client.get_me()
        
        await message.reply(
            f"✅ *Авторизация успешна!*\n\n"
            f"Мастер-аккаунт: {me.first_name}\n"
            f"Username: @{me.username}\n\n"
            f"Теперь все пользователи могут пользоваться ботом.\n"
            f"Нажми '🔍 Поиск по словам' или '🔄 Собрать всё'",
            parse_mode='Markdown',
            reply_markup=get_main_keyboard(user_id)
        )
        
        if user_id in auth_data:
            del auth_data[user_id]
        
    except Exception as e:
        await message.reply(
            f"❌ Неверный пароль!\n\n"
            f"Попробуй ещё раз:",
            reply_markup=get_auth_keyboard()
        )

# ========== ОБРАБОТЧИКИ ТЕКСТОВЫХ СООБЩЕНИЙ ==========
@dp.message_handler(lambda message: message.from_user.id in user_data and user_data[message.from_user.id].get('state') == 'waiting_channel_link')
async def process_channel_link(message: types.Message):
    """Обрабатывает добавление канала по ссылке"""
    try:
        user_id = message.from_user.id
        link = message.text.strip()
        
        link = link.replace('@', '')
        if not link.startswith('http'):
            if '/' not in link:
                link = f"https://t.me/{link}"
        
        channel_name = extract_channel_name(link)
        if not channel_name:
            channel_name = link.split('/')[-1]
        
        status_msg = await message.reply("🔄 Проверяю доступность канала...")
        
        try:
            global MASTER_SESSION
            if MASTER_SESSION:
                client = TelegramClient(StringSession(MASTER_SESSION), API_ID, API_HASH)
                await client.connect()
                
                if await client.is_user_authorized():
                    entity = await client.get_entity(link)
                    if hasattr(entity, 'title') and entity.title:
                        channel_name = entity.title
                
                await client.disconnect()
            
        except Exception as e:
            pass
        
        await status_msg.delete()
        
        channels = load_channels()
        
        for ch in channels:
            if ch['url'] == link or ch['name'].lower() == channel_name.lower():
                await message.reply(
                    f"❌ Канал {channel_name} уже добавлен!\n"
                    f"Ссылка: {ch['url']}",
                    reply_markup=get_main_keyboard(user_id)
                )
                del user_data[user_id]
                return
        
        channels.append({'name': channel_name, 'url': link})
        save_channels(channels)
        
        del user_data[user_id]
        
        await message.reply(
            f"✅ Канал успешно добавлен!\n\n"
            f"📌 Название: {channel_name}\n"
            f"🔗 Ссылка: {link}\n\n"
            f"Всего каналов: {len(channels)}",
            reply_markup=get_main_keyboard(user_id)
        )
        
    except Exception as e:
        await message.reply(f"❌ Ошибка: {str(e)}")

@dp.message_handler(lambda message: message.from_user.id in user_data and user_data[message.from_user.id].get('state') == 'waiting_keywords')
async def process_keywords(message: types.Message):
    """Обрабатывает ключевые слова и запускает сбор"""
    keywords = message.text.strip()
    user_id = message.from_user.id
    
    channels = load_channels()
    
    period_hours = user_data[user_id].get('period_hours', 168)
    save_images = user_data[user_id].get('save_images', True)
    
    user_data[user_id] = {
        'state': 'collecting',
        'keywords': keywords,
        'channels': channels,
        'period_hours': period_hours,
        'save_images': save_images
    }
    
    if period_hours == 1:
        period_text = "1 час"
    elif period_hours == 3:
        period_text = "3 часа"
    elif period_hours == 7:
        period_text = "7 часов"
    elif period_hours == 24:
        period_text = "24 часа"
    elif period_hours == 72:
        period_text = "3 дня"
    else:
        period_text = "7 дней"
    
    await message.reply(
        f"🔍 Начинаю поиск\n"
        f"Ключевые слова: {keywords}\n"
        f"Период: {period_text}\n"
        f"Каналов для анализа: {len(channels)}\n"
        f"Формат: {'с картинками' if save_images else 'только текст'}\n\n"
        f"⏳ Это займет некоторое время...\n"
        f"Чтобы остановить, нажми '⏹️ Стоп'",
        reply_markup=get_main_keyboard(user_id)
    )
    
    await collect_and_send_report(user_id)

# ========== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК ==========
@dp.callback_query_handler(lambda c: c.data.startswith('delete_'))
async def process_delete_callback(callback_query: types.CallbackQuery):
    """Обрабатывает удаление канала"""
    await bot.answer_callback_query(callback_query.id)
    
    user_id = callback_query.from_user.id
    
    data = callback_query.data
    channels = load_channels()
    
    if data == "delete_all":
        save_channels([])
        await bot.send_message(
            user_id,
            "🗑 Все каналы удалены",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    index = int(data.split('_')[1])
    if index < len(channels):
        deleted = channels.pop(index)
        save_channels(channels)
        
        await bot.send_message(
            user_id,
            f"🗑 Канал удален\n\n"
            f"Название: {deleted['name']}\n"
            f"Ссылка: {deleted['url']}",
            reply_markup=get_main_keyboard(user_id)
        )

@dp.callback_query_handler(lambda c: c.data == 'back_to_main')
async def process_back_callback(callback_query: types.CallbackQuery):
    """Возврат в главное меню"""
    await bot.answer_callback_query(callback_query.id)
    
    user_id = callback_query.from_user.id
    
    await bot.send_message(
        user_id,
        "Главное меню:",
        reply_markup=get_main_keyboard(user_id)
    )

# ========== ФУНКЦИЯ СБОРА И ОТПРАВКИ ОТЧЕТА ==========
async def collect_and_send_report(user_id):
    """Сбор данных и отправка отчёта"""
    client = None
    try:
        stop_flags[user_id] = False
        
        keywords = user_data[user_id]['keywords']
        channels = user_data[user_id]['channels']
        period_hours = user_data[user_id].get('period_hours', 168)
        save_images = user_data[user_id].get('save_images', True)
        
        if period_hours == 1:
            period_display = "1 час"
        elif period_hours == 3:
            period_display = "3 часа"
        elif period_hours == 7:
            period_display = "7 часов"
        elif period_hours == 24:
            period_display = "24 часа"
        elif period_hours == 72:
            period_display = "3 дня"
        else:
            period_display = "7 дней"
        
        await bot.send_message(user_id, "🔄 Подключаюсь к Telegram...")
        
        global MASTER_SESSION
        if not MASTER_SESSION:
            MASTER_SESSION = load_master_session()
        
        if not MASTER_SESSION:
            await bot.send_message(user_id, 
                "❌ Мастер-сессия не найдена. Обратитесь к администратору.",
                reply_markup=get_main_keyboard(user_id)
            )
            return
        
        client = TelegramClient(StringSession(MASTER_SESSION), API_ID, API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            await bot.send_message(user_id, 
                "❌ Мастер-сессия не активна. Администратору нужно авторизоваться заново.",
                reply_markup=get_main_keyboard(user_id)
            )
            await client.disconnect()
            remove_master_session()
            return
        
        new_session_string = client.session.save()
        if new_session_string != MASTER_SESSION:
            save_session_string(new_session_string)
        
        await bot.send_message(user_id, f"✅ Подключение установлено! Начинаю сбор...")
        
        doc = Document()
        
        title = doc.add_heading('Отчёт по Telegram-каналам', 0)
        title.alignment = 1
        
        doc.add_paragraph(f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        doc.add_paragraph(f"Ключевые слова: {keywords}")
        doc.add_paragraph(f"Период: последние {period_display}")
        doc.add_paragraph()
        
        total_posts = 0
        processed_channels = 0
        stopped_early = False
        
        now = datetime.now().astimezone()
        start_time = now - timedelta(hours=period_hours)
        
        for channel in channels:
            if stop_flags.get(user_id, False):
                stopped_early = True
                await bot.send_message(user_id, 
                    f"⏹️ Формирование отчета остановлено по вашему запросу.\n"
                    f"Обработано каналов: {processed_channels}/{len(channels)}"
                )
                break
            
            processed_channels += 1
            await bot.send_message(user_id, f"📱 [{processed_channels}/{len(channels)}] Анализирую: {channel['name']}")
            
            try:
                entity = await client.get_entity(channel['url'])
                
                doc.add_heading(f"Канал: {channel['name']}", level=1)
                doc.add_paragraph(f"Ссылка: {channel['url']}")
                doc.add_paragraph()
                
                posts_count = 0
                
                async for message in client.iter_messages(entity, offset_date=now, reverse=False):
                    if posts_count % 5 == 0 and stop_flags.get(user_id, False):
                        stopped_early = True
                        await bot.send_message(user_id, f"⏹️ Останавливаю анализ канала {channel['name']}...")
                        break
                    
                    if message.date:
                        msg_date = message.date
                        if msg_date.tzinfo is None:
                            msg_date = msg_date.replace(tzinfo=timezone.utc)
                        if msg_date < start_time:
                            break
                    
                    should_include = False
                    if keywords.lower() == 'все':
                        should_include = True
                    elif message.text:
                        text = message.text
                        if any(kw.strip().lower() in text.lower() for kw in keywords.split(',')):
                            should_include = True
                    
                    if should_include:
                        p = doc.add_paragraph()
                        
                        display_date = message.date
                        if display_date:
                            if display_date.tzinfo is not None:
                                display_date = display_date.astimezone()
                            p.add_run(f"📅 {display_date.strftime('%d.%m.%Y %H:%M')}\n").bold = True
                        
                        stats = []
                        if hasattr(message, 'views') and message.views:
                            stats.append(f"👁 {message.views} просмотров")
                        if hasattr(message, 'forwards') and message.forwards:
                            stats.append(f"🔄 {message.forwards} репостов")
                        
                        if hasattr(message, 'reactions') and message.reactions:
                            reactions_text = []
                            for reaction in message.reactions.results:
                                if hasattr(reaction, 'reaction'):
                                    emoji = reaction.reaction.emoticon if hasattr(reaction.reaction, 'emoticon') else '👍'
                                    reactions_text.append(f"{emoji} {reaction.count}")
                            if reactions_text:
                                stats.append(f"💬 Реакции: {' '.join(reactions_text)}")
                        
                        if stats:
                            p.add_run(" | ".join(stats) + "\n")
                        
                        if message.text:
                            text = message.text
                            if len(text) > 5000:
                                text = text[:5000] + "..."
                            p.add_run(text)
                        
                        if save_images and message.media:
                            media_info = []
                            if hasattr(message.media, 'photo'):
                                media_info.append("📷 Фото")
                                image_path = await download_media(message, user_id, client)
                                if image_path:
                                    doc.add_paragraph()
                                    if add_image_to_doc(doc, image_path):
                                        media_info.append("(сохранено)")
                            
                            if hasattr(message.media, 'document'):
                                doc_type = "📎 Документ"
                                if hasattr(message.media.document, 'mime_type'):
                                    mime = message.media.document.mime_type
                                    if 'video' in mime:
                                        doc_type = "🎥 Видео"
                                    elif 'audio' in mime:
                                        doc_type = "🎵 Аудио"
                                    elif 'image' in mime:
                                        doc_type = "🖼 Изображение"
                                        if save_images:
                                            image_path = await download_media(message, user_id, client)
                                            if image_path:
                                                doc.add_paragraph()
                                                if add_image_to_doc(doc, image_path):
                                                    media_info.append(f"{doc_type} (сохранено)")
                                                else:
                                                    media_info.append(doc_type)
                                        else:
                                            media_info.append(doc_type)
                                    else:
                                        media_info.append(doc_type)
                                else:
                                    media_info.append(doc_type)
                            
                            if media_info and not any('(сохранено)' in info for info in media_info):
                                p.add_run(f"\n📎 Медиа: {', '.join(media_info)}")
                        elif message.media and not save_images:
                            media_info = []
                            if hasattr(message.media, 'photo'):
                                media_info.append("📷 Фото")
                            if hasattr(message.media, 'document'):
                                doc_type = "📎 Документ"
                                if hasattr(message.media.document, 'mime_type'):
                                    mime = message.media.document.mime_type
                                    if 'video' in mime:
                                        doc_type = "🎥 Видео"
                                    elif 'audio' in mime:
                                        doc_type = "🎵 Аудио"
                                    elif 'image' in mime:
                                        doc_type = "🖼 Изображение"
                                media_info.append(doc_type)
                            if media_info:
                                p.add_run(f"\n📎 Медиа: {', '.join(media_info)}")
                        
                        if message.id:
                            channel_username = channel['url'].split('/')[-1]
                            p.add_run(f"\n🔗 Ссылка: https://t.me/{channel_username}/{message.id}")
                        
                        if hasattr(message, 'post_author') and message.post_author:
                            p.add_run(f"\n✍️ Автор: {message.post_author}")
                        elif hasattr(message, 'sender_id') and message.sender_id:
                            try:
                                sender = await client.get_entity(message.sender_id)
                                if hasattr(sender, 'first_name') or hasattr(sender, 'last_name'):
                                    name = f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip()
                                    if name:
                                        p.add_run(f"\n✍️ От: {name}")
                            except:
                                pass
                        
                        doc.add_paragraph()
                        posts_count += 1
                        total_posts += 1
                        
                        if posts_count % 20 == 0:
                            await bot.send_message(user_id, f"📊 Найдено {posts_count} постов в канале {channel['name']}...")
                
                if stop_flags.get(user_id, False):
                    stopped_early = True
                    await bot.send_message(user_id, f"⏹️ Анализ канала {channel['name']} прерван.")
                    break
                
                doc.add_paragraph(f"✅ Найдено постов: {posts_count}")
                doc.add_page_break()
                
            except FloodWaitError as e:
                wait_time = e.seconds
                await bot.send_message(
                    user_id, 
                    f"⚠️ Превышен лимит запросов. Жду {wait_time} секунд..."
                )
                await asyncio.sleep(wait_time)
                continue
                
            except Exception as e:
                await bot.send_message(user_id, f"⚠️ Ошибка с каналом {channel['name']}: {str(e)[:200]}")
                doc.add_paragraph(f"❌ Ошибка доступа к каналу: {channel['name']}")
                doc.add_page_break()
        
        doc.add_heading('Итоговая статистика', level=1)
        doc.add_paragraph(f"Всего обработано каналов: {processed_channels}/{len(channels)}")
        doc.add_paragraph(f"Всего найдено постов: {total_posts}")
        doc.add_paragraph(f"Период: последние {period_display}")
        doc.add_paragraph(f"Начало периода: {start_time.strftime('%d.%m.%Y %H:%M')}")
        doc.add_paragraph(f"Формат отчета: {'с картинками' if save_images else 'только текст'}")
        if stopped_early:
            doc.add_paragraph("⚠️ Отчет был остановлен досрочно по запросу пользователя")
        
        output_file = f"report_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_file)
        
        with open(output_file, 'rb') as f:
            caption = f"✅ Отчет готов!\n\n"
            caption += f"📊 Найдено постов: {total_posts}\n"
            caption += f"📁 Каналов: {processed_channels}/{len(channels)}\n"
            caption += f"🔍 Ключевые слова: {keywords}\n"
            caption += f"⏱ Период: {period_display}\n"
            caption += f"🖼️ Формат: {'с картинками' if save_images else 'только текст'}\n"
            if stopped_early:
                caption += f"⚠️ Отчет остановлен досрочно\n"
            
            await bot.send_document(
                user_id, 
                f, 
                caption=caption
            )
        
        if os.path.exists(output_file):
            os.remove(output_file)
        
        cleanup_temp_files(user_id)
        
        if user_id in user_data:
            del user_data[user_id]
        
        await client.disconnect()
        
        if user_id in stop_flags:
            del stop_flags[user_id]
        
    except Exception as e:
        await bot.send_message(user_id, f"❌ Произошла ошибка: {str(e)}")
        logging.error(f"Error for user {user_id}: {e}")
        
        cleanup_temp_files(user_id)
        
        if user_id in user_data:
            del user_data[user_id]
        
        if user_id in stop_flags:
            del stop_flags[user_id]
        
        if client:
            await client.disconnect()

# ========== ОБРАБОТЧИК НЕИЗВЕСТНЫХ КОМАНД ==========
@dp.message_handler()
async def handle_unknown(message: types.Message):
    """Обработка неизвестных команд"""
    user_id = message.from_user.id
    
    if is_admin(user_id) and user_id in auth_data:
        if auth_data[user_id].get('state') == 'waiting_phone':
            await process_phone(message)
            return
        elif auth_data[user_id].get('state') == 'waiting_code':
            await process_code(message)
            return
        elif auth_data[user_id].get('state') == 'waiting_password':
            await process_password(message)
            return
    
    if message.text and ('t.me/' in message.text or '@' in message.text):
        user_data[user_id] = {'state': 'waiting_channel_link'}
        await process_channel_link(message)
    else:
        await message.reply(
            "Я не понимаю эту команду.\n"
            "Используй кнопки внизу экрана 👇",
            reply_markup=get_main_keyboard(user_id)
        )

# ========== ЗАПУСК ==========
if __name__ == '__main__':
    # Загружаем мастер-сессию при старте
    MASTER_SESSION = load_master_session()
    
    print("🤖 Бот запущен! Нажми Ctrl+C для остановки")
    
    # Принудительно очищаем вебхуки перед запуском
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def on_startup(dp):
        await bot.delete_webhook()
        await asyncio.sleep(0.5)
        print("✅ Вебхуки очищены, бот готов к работе")
    
    # Запускаем с правильными параметрами
    executor.start_polling(
        dp, 
        skip_updates=True, 
        on_startup=on_startup,
        timeout=30,
        relax=0.1
    )

