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
import pandas as pd
import openpyxl

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.types import ParseMode, ContentType, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils import executor
from docx import Document
from docx.shared import Inches, Pt, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError, PhoneCodeExpiredError, PhoneCodeInvalidError
from telethon.sessions import StringSession

# ========== НАСТРОЙКИ ==========
API_TOKEN = '8029293386:AAG-Hih0eVun77YYcY8zf6PyDjQERmQCx9w'  # Твой токен

# Данные от Telegram API
API_ID = 38892524
API_HASH = 'd71ef1a657ab20d2a47a52130626c939'

# Файл для хранения списка каналов (общий для всех пользователей)
CHANNELS_FILE = 'saved_channels.json'

# Файл для хранения сессий в текстовом формате
SESSIONS_DIR = 'telegram_sessions'

# Папка для временного хранения изображений
IMAGES_DIR = 'temp_images'

# Папка для загруженных файлов
UPLOADS_DIR = 'uploads'

# ID администратора (твой ID)
ADMIN_ID = 5224743551

# Создаем папки, если их нет
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

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
stop_flags = {}  # Флаги для остановки формирования отчета

# Глобальная сессия для всех пользователей
MASTER_SESSION = None

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С ФАЙЛАМИ ==========
def load_channels():
    """Загружает сохраненные каналы из JSON (общие для всех)"""
    if os.path.exists(CHANNELS_FILE):
        with open(CHANNELS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_channels(channels):
    """Сохраняет каналы в JSON (общие для всех)"""
    with open(CHANNELS_FILE, 'w', encoding='utf-8') as f:
        json.dump(channels, f, ensure_ascii=False, indent=2)

def normalize_channel_url(url):
    """Нормализует URL канала, убирая лишние t.me/"""
    # Убираем @ в начале
    url = url.replace('@', '')
    
    # Если ссылка уже содержит t.me, нормализуем её
    if 't.me/' in url:
        # Разбиваем по t.me/ и берем последнюю часть
        parts = url.split('t.me/')
        username = parts[-1].strip('/')
        # Убираем лишние слеши в username
        username = username.split('/')[0]
        return f"https://t.me/{username}"
    
    # Если просто имя канала
    if '/' not in url and not url.startswith('http'):
        return f"https://t.me/{url}"
    
    return url

def extract_channel_name(url):
    """Извлекает имя канала из ссылки"""
    # Сначала нормализуем URL
    url = normalize_channel_url(url)
    match = re.search(r't\.me/(?:s/)?([a-zA-Z0-9_]+)', url)
    if match:
        return match.group(1)
    return None

def export_channels_to_excel():
    """Экспортирует каналы в Excel файл"""
    try:
        channels = load_channels()
        
        # Создаем DataFrame
        df = pd.DataFrame(channels)
        
        # Добавляем столбец с номером
        df.insert(0, '№', range(1, len(df) + 1))
        
        # Сохраняем в Excel
        filename = f"channels_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        filepath = os.path.join(UPLOADS_DIR, filename)
        
        df.to_excel(filepath, index=False, engine='openpyxl')
        
        return filepath
    except Exception as e:
        print(f"Ошибка экспорта в Excel: {e}")
        return None

def import_channels_from_excel(file_path):
    """Импортирует каналы из Excel файла"""
    try:
        # Читаем Excel файл
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Получаем текущие каналы
        current_channels = load_channels()
        current_urls = [ch['url'] for ch in current_channels]
        
        new_channels = []
        duplicates = []
        invalid = []
        
        for index, row in df.iterrows():
            # Ищем столбцы с названием и ссылкой
            name_col = None
            url_col = None
            
            for col in df.columns:
                col_lower = str(col).lower()
                if 'название' in col_lower or 'name' in col_lower or 'канал' in col_lower:
                    name_col = col
                if 'ссылка' in col_lower or 'url' in col_lower or 'link' in col_lower:
                    url_col = col
            
            if name_col and url_col:
                name = str(row[name_col]).strip()
                url = str(row[url_col]).strip()
                
                # Проверяем, что ссылка валидная
                if url and ('t.me/' in url or '@' in url):
                    # Нормализуем ссылку
                    url = normalize_channel_url(url)
                    
                    # Проверяем на дубликаты
                    if url in current_urls:
                        duplicates.append(f"{name} - {url}")
                    else:
                        new_channels.append({'name': name, 'url': url})
                        current_urls.append(url)
                else:
                    invalid.append(f"{name} - {url}")
        
        # Добавляем новые каналы
        if new_channels:
            current_channels.extend(new_channels)
            save_channels(current_channels)
        
        return {
            'success': True,
            'added': len(new_channels),
            'duplicates': duplicates,
            'invalid': invalid,
            'total': len(current_channels)
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

def import_channels_from_txt(file_path):
    """Импортирует каналы из текстового файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        current_channels = load_channels()
        current_urls = [ch['url'] for ch in current_channels]
        
        new_channels = []
        duplicates = []
        invalid = []
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            # Пытаемся разделить строку на название и ссылку
            parts = line.split(',')
            if len(parts) >= 2:
                name = parts[0].strip()
                url = parts[1].strip()
            else:
                # Если только ссылка, пытаемся извлечь название
                url = line
                name = extract_channel_name(url) or url.split('/')[-1]
            
            # Проверяем, что ссылка валидная
            if url and ('t.me/' in url or '@' in url):
                # Нормализуем ссылку
                url = normalize_channel_url(url)
                
                # Проверяем на дубликаты
                if url in current_urls:
                    duplicates.append(f"{name} - {url}")
                else:
                    new_channels.append({'name': name, 'url': url})
                    current_urls.append(url)
            else:
                invalid.append(f"{name} - {url}")
        
        # Добавляем новые каналы
        if new_channels:
            current_channels.extend(new_channels)
            save_channels(current_channels)
        
        return {
            'success': True,
            'added': len(new_channels),
            'duplicates': duplicates,
            'invalid': invalid,
            'total': len(current_channels)
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

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
            return True
    except Exception as e:
        print(f"Ошибка добавления изображения в документ: {e}")
        return False

def add_hyperlink(paragraph, text, url):
    """Добавляет гиперссылку в параграф Word документа"""
    # Получаем часть документа
    part = paragraph.part
    
    # Создаем элемент гиперссылки
    hyperlink = OxmlElement('w:hyperlink')
    hyperlink.set(qn('r:id'), part.relate_to(url, 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink', is_external=True))
    
    # Создаем элемент прогона
    r = OxmlElement('w:r')
    
    # Создаем элемент свойств прогона
    rPr = OxmlElement('w:rPr')
    
    # Создаем элемент стиля (синий, подчеркнутый для ссылки)
    rStyle = OxmlElement('w:rStyle')
    rStyle.set(qn('w:val'), 'Hyperlink')
    rPr.append(rStyle)
    
    r.append(rPr)
    
    # Создаем элемент текста
    t = OxmlElement('w:t')
    t.text = text
    r.append(t)
    
    hyperlink.append(r)
    paragraph._p.append(hyperlink)
    
    return hyperlink

# ========== ФУНКЦИИ ДЛЯ ПРОВЕРКИ АДМИНА ==========
def is_admin(user_id):
    """Проверяет, является ли пользователь администратором"""
    return user_id == ADMIN_ID

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(user_id):
    """Главная клавиатура с кнопками"""
    
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("📋 Список каналов"),
        KeyboardButton("➕ Добавить канал"),
        KeyboardButton("📤 Импорт каналов"),
        KeyboardButton("📥 Экспорт каналов"),
        KeyboardButton("🔍 Поиск"),
        KeyboardButton("❓ Помощь"),
        KeyboardButton("⏹️ Стоп")
    )
    
    # Кнопки только для админа
    if is_admin(user_id):
        keyboard.add(
            KeyboardButton("🔄 Собрать всё"),
            KeyboardButton("🚪 Разлогиниться")
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

def get_channels_management_keyboard():
    """Клавиатура для управления каналами"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("📤 Экспорт в Excel"),
        KeyboardButton("📥 Импорт из Excel"),
        KeyboardButton("📥 Импорт из TXT"),
        KeyboardButton("◀️ Назад в меню")
    )
    return keyboard

# ========== КОМАНДЫ ==========
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Обработка команды /start"""
    user_id = message.from_user.id
    
    # Загружаем общий список каналов
    channels = load_channels()
    
    welcome_text = (
        "👋 Добро пожаловать!\n\n"
        "Я бот для сбора информации из Telegram-каналов.\n\n"
        "📊 Общий список каналов доступен всем пользователям\n"
        "✏️ Все пользователи могут добавлять каналы\n\n"
        "Как пользоваться:\n"
        "1️⃣ Нажми '➕ Добавить канал' и отправь ссылку на канал\n"
        "2️⃣ Канал добавится в общий список для всех\n"
        "3️⃣ Нажми '🔍 Поиск' для поиска по ключевым словам\n"
        "4️⃣ Выбери период времени\n"
        "5️⃣ Выбери формат отчета\n\n"
        "📤 Импорт/экспорт каналов:\n"
        "• Экспорт - сохранить список каналов в Excel\n"
        "• Импорт - загрузить каналы из Excel или TXT\n\n"
        "⏹️ Остановить формирование отчета - нажми '⏹️ Стоп'\n"
    )
    
    # Добавляем информацию о количестве каналов
    welcome_text += f"\n📌 Всего каналов в базе: {len(channels)}"
    
    # Добавляем информацию для админа
    if is_admin(user_id):
        welcome_text += (
            "\n\n👑 Вы администратор. Доступны дополнительные функции:\n"
            "• 🔄 Собрать всё - получить все посты без фильтра\n"
            "• 🚪 Разлогиниться - выйти из аккаунта\n"
        )
    
    # Проверяем наличие мастер-сессии
    global MASTER_SESSION
    if not MASTER_SESSION:
        MASTER_SESSION = load_master_session()
    
    if not MASTER_SESSION and is_admin(user_id):
        welcome_text += (
            "\n⚠️ Мастер-сессия не найдена!\n"
            "Нажми '🔍 Поиск' для авторизации."
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
        channels = load_channels()
        
        debug_text = (
            f"🔍 Отладка (админ)\n\n"
            f"📁 Мастер-сессия: {'✅ есть' if session_exists else '❌ нет'} ({session_size} байт)\n"
            f"📊 Всего каналов: {len(channels)}\n"
            f"🔄 В процессе авторизации: {'✅ да' if in_auth else '❌ нет'}\n"
            f"📌 Состояние: {auth_state if auth_state else '-'}\n"
            f"⏹️ Процесс остановлен: {'✅ да' if is_stopping else '❌ нет'}\n"
            f"🆔 User ID: {user_id}"
        )
    else:
        is_stopping = user_id in stop_flags and stop_flags[user_id]
        channels = load_channels()
        debug_text = (
            f"🔍 Отладка\n\n"
            f"📊 Всего каналов: {len(channels)}\n"
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

@dp.message_handler(lambda message: message.text == "📋 Список каналов")
async def show_channels(message: types.Message):
    """Показывает общий список сохраненных каналов"""
    user_id = message.from_user.id
    
    channels = load_channels()
    
    if not channels:
        await message.reply(
            "📭 Пока нет добавленных каналов\n\n"
            "Нажми '➕ Добавить канал' и отправь ссылку на Telegram-канал",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    # Создаем клавиатуру с кнопками для каждого канала
    keyboard = InlineKeyboardMarkup(row_width=1)
    for i, channel in enumerate(channels):
        # Кнопка для перехода на канал
        channel_button = InlineKeyboardButton(
            f"📢 {channel['name']}", 
            url=channel['url']
        )
        # Кнопка для удаления канала (только для админа)
        if is_admin(user_id):
            delete_button = InlineKeyboardButton(
                f"❌ Удалить", 
                callback_data=f"delete_{i}"
            )
            keyboard.row(channel_button, delete_button)
        else:
            keyboard.add(channel_button)
    
    # Кнопка удаления всех каналов (только для админа)
    if is_admin(user_id):
        keyboard.add(InlineKeyboardButton("❌ Удалить все", callback_data="delete_all"))
    
    keyboard.add(InlineKeyboardButton("◀️ Назад в главное меню", callback_data="back_to_main"))
    
    text = f"📋 Общий список каналов:\n\n"
    for i, channel in enumerate(channels, 1):
        text += f"{i}. {channel['name']}\n"
    
    text += f"\nВсего каналов: {len(channels)}\n\n"
    text += "Нажми на название канала чтобы перейти\n"
    
    if is_admin(user_id):
        text += "Нажми ❌ Удалить чтобы удалить канал"
    else:
        text += "Только администратор может удалять каналы"
    
    await message.reply(text, reply_markup=keyboard)

@dp.message_handler(lambda message: message.text == "➕ Добавить канал")
async def add_channel_prompt(message: types.Message):
    """Запрашивает ссылку на канал"""
    user_id = message.from_user.id
    
    await message.reply(
        "🔗 Отправь ссылку на Telegram-канал\n\n"
        "Примеры:\n"
        "• https://t.me/durov\n"
        "• @durov\n"
        "• durov\n\n"
        "✅ Канал добавится в общий список для всех пользователей!\n\n"
        "Я сам определю название канала!"
    )
    
    user_data[user_id] = {'state': 'waiting_channel_link'}

@dp.message_handler(lambda message: message.text == "📤 Импорт каналов")
async def import_channels_prompt(message: types.Message):
    """Предлагает выбрать способ импорта"""
    user_id = message.from_user.id
    
    await message.reply(
        "📤 Выберите способ импорта каналов:\n\n"
        "• 📥 Импорт из Excel - загрузите Excel файл с каналами\n"
        "• 📥 Импорт из TXT - загрузите текстовый файл с каналами\n\n"
        "Формат файла:\n"
        "Название, ссылка\n"
        "Пример:\n"
        "Новости, https://t.me/durov\n"
        "Технологии, @techchannel",
        reply_markup=get_channels_management_keyboard()
    )

@dp.message_handler(lambda message: message.text == "📥 Экспорт каналов")
async def export_channels(message: types.Message):
    """Экспортирует каналы в Excel"""
    user_id = message.from_user.id
    
    status_msg = await message.reply("🔄 Создаю Excel файл...")
    
    try:
        filepath = export_channels_to_excel()
        
        if filepath and os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                await bot.send_document(
                    user_id,
                    f,
                    caption="📊 Список каналов в формате Excel"
                )
            
            # Удаляем временный файл
            os.remove(filepath)
            await status_msg.delete()
        else:
            await status_msg.edit_text("❌ Ошибка при создании Excel файла")
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка: {str(e)}")

@dp.message_handler(lambda message: message.text == "📥 Импорт из Excel")
async def import_excel_prompt(message: types.Message):
    """Запрашивает Excel файл для импорта"""
    user_id = message.from_user.id
    
    await message.reply(
        "📥 Отправьте Excel файл (.xlsx) с каналами\n\n"
        "Формат файла:\n"
        "• Первый столбец - название канала\n"
        "• Второй столбец - ссылка на канал\n\n"
        "Или нажмите '◀️ Назад в меню' для возврата",
        reply_markup=get_channels_management_keyboard()
    )
    
    user_data[user_id] = {'state': 'waiting_excel_file'}

@dp.message_handler(lambda message: message.text == "📥 Импорт из TXT")
async def import_txt_prompt(message: types.Message):
    """Запрашивает текстовый файл для импорта"""
    user_id = message.from_user.id
    
    await message.reply(
        "📥 Отправьте текстовый файл (.txt) с каналами\n\n"
        "Формат файла:\n"
        "Название, ссылка\n"
        "Каждая строка - один канал\n\n"
        "Пример:\n"
        "Новости, https://t.me/durov\n"
        "Технологии, @techchannel\n\n"
        "Или нажмите '◀️ Назад в меню' для возврата",
        reply_markup=get_channels_management_keyboard()
    )
    
    user_data[user_id] = {'state': 'waiting_txt_file'}

@dp.message_handler(content_types=ContentType.DOCUMENT)
async def handle_document(message: types.Message):
    """Обрабатывает загруженные документы"""
    user_id = message.from_user.id
    
    if user_id not in user_data:
        await message.reply(
            "Сначала выберите действие в меню",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    state = user_data[user_id].get('state')
    
    if state not in ['waiting_excel_file', 'waiting_txt_file']:
        return
    
    document = message.document
    file_name = document.file_name
    file_ext = os.path.splitext(file_name)[1].lower()
    
    # Проверяем расширение файла
    if state == 'waiting_excel_file' and file_ext not in ['.xlsx', '.xls']:
        await message.reply(
            "❌ Пожалуйста, отправьте Excel файл (.xlsx или .xls)",
            reply_markup=get_channels_management_keyboard()
        )
        return
    
    if state == 'waiting_txt_file' and file_ext != '.txt':
        await message.reply(
            "❌ Пожалуйста, отправьте текстовый файл (.txt)",
            reply_markup=get_channels_management_keyboard()
        )
        return
    
    status_msg = await message.reply("🔄 Загружаю и обрабатываю файл...")
    
    try:
        # Скачиваем файл
        file_path = os.path.join(UPLOADS_DIR, f"{user_id}_{file_name}")
        await document.download(destination_file=file_path)
        
        # Обрабатываем файл
        if state == 'waiting_excel_file':
            result = import_channels_from_excel(file_path)
        else:
            result = import_channels_from_txt(file_path)
        
        # Удаляем временный файл
        safe_remove_file(file_path)
        
        # Формируем ответ
        if result['success']:
            response = f"✅ Импорт завершен!\n\n"
            response += f"📊 Добавлено новых каналов: {result['added']}\n"
            response += f"📈 Всего каналов в базе: {result['total']}\n"
            
            if result['duplicates']:
                response += f"\n⚠️ Найдены дубликаты ({len(result['duplicates'])}):\n"
                for dup in result['duplicates'][:5]:  # Показываем первые 5
                    response += f"• {dup}\n"
                if len(result['duplicates']) > 5:
                    response += f"... и еще {len(result['duplicates']) - 5}\n"
            
            if result['invalid']:
                response += f"\n❌ Некорректные ссылки ({len(result['invalid'])}):\n"
                for inv in result['invalid'][:5]:  # Показываем первые 5
                    response += f"• {inv}\n"
                if len(result['invalid']) > 5:
                    response += f"... и еще {len(result['invalid']) - 5}\n"
        else:
            response = f"❌ Ошибка импорта: {result['error']}"
        
        await status_msg.delete()
        await message.reply(response, reply_markup=get_main_keyboard(user_id))
        
        # Очищаем состояние
        del user_data[user_id]
        
    except Exception as e:
        await status_msg.delete()
        await message.reply(f"❌ Ошибка при обработке файла: {str(e)}")
        safe_remove_file(file_path)

@dp.message_handler(lambda message: message.text == "🔍 Поиск")
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
    """Собрать все посты без фильтра (только для админа)"""
    user_id = message.from_user.id
    
    # Проверяем, админ ли пользователь
    if not is_admin(user_id):
        await message.reply(
            "❌ Эта функция доступна только администратору.\n"
            "Используйте '🔍 Поиск' для поиска по ключевым словам.",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    global MASTER_SESSION
    if not MASTER_SESSION:
        MASTER_SESSION = load_master_session()
    
    if not MASTER_SESSION:
        await message.reply(
            "🔄 Мастер-сессия не найдена. Начинаю процесс авторизации...",
            reply_markup=get_auth_keyboard()
        )
        await start_authorization(user_id, message)
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
    
    channels = load_channels()
    
    help_text = (
        "❓ Помощь\n\n"
        "📋 Список каналов - посмотреть общий список каналов\n"
        "➕ Добавить канал - добавить новый канал в общий список\n"
        "📤 Импорт каналов - загрузить каналы из файла\n"
        "📥 Экспорт каналов - сохранить каналы в Excel\n"
        "🔍 Поиск - найти посты по ключевым словам\n"
        "⏹️ Стоп - остановить формирование отчета\n\n"
        f"📊 Всего каналов в базе: {len(channels)}\n\n"
        "После выбора периода появится выбор:\n"
        "🖼️ С картинками - сохранить изображения в отчет\n"
        "📝 Только текст - только текст, без картинок\n\n"
        "Команды:\n"
        "/debug - диагностика\n"
        "/checksession - проверка сессии"
    )
    
    if is_admin(user_id):
        help_text += (
            "\n\n👑 Команды администратора:\n"
            "🔄 Собрать всё - получить все посты без фильтра\n"
            "🚪 Разлогиниться - выйти из аккаунта\n"
            "/reset - сброс авторизации"
        )
    
    await message.reply(help_text, reply_markup=get_main_keyboard(user_id))

@dp.message_handler(lambda message: message.text == "⏹️ Стоп")
async def stop_process(message: types.Message):
    """Останавливает текущий процесс сбора"""
    user_id = message.from_user.id
    
    if user_id in stop_flags:
        stop_flags[user_id] = True
        await message.reply(
            "⏹️ Отправлен сигнал остановки...",
            reply_markup=get_main_keyboard(user_id)
        )
    else:
        await message.reply(
            "Нет активного процесса для остановки.",
            reply_markup=get_main_keyboard(user_id)
        )

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

@dp.message_handler(lambda message: message.text == "🚪 Разлогиниться")
async def logout(message: types.Message):
    """Выход из аккаунта (только для админа)"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.reply(
            "❌ Эта функция доступна только администратору.",
            reply_markup=get_main_keyboard(user_id)
        )
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
    
    # Очищаем данные авторизации
    if user_id in auth_data:
        del auth_data[user_id]
    
    await message.reply(
        "🚪 Вы вышли из аккаунта.\n\n"
        "При следующем использовании нужно будет авторизоваться заново.",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.text == "◀️ Назад в меню")
async def back_to_menu(message: types.Message):
    """Возврат в главное меню"""
    user_id = message.from_user.id
    
    # Очищаем состояние пользователя
    if user_id in user_data:
        del user_data[user_id]
    
    await message.reply(
        "Главное меню:",
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
    
    if 'keywords' in user_data[user_id] and user_data[user_id]['keywords'] == 'все':
        # Для "Собрать всё" сразу запускаем сбор
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
        # Для поиска запрашиваем ключевые слова
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
            f"Нажми '🔍 Поиск' или '🔄 Собрать всё'",
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
            f"Нажми '🔍 Поиск' или '🔄 Собрать всё'",
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
    """Обрабатывает добавление канала по ссылке (канал добавляется в общий список)"""
    try:
        user_id = message.from_user.id
        link = message.text.strip()
        
        # Нормализуем ссылку
        link = normalize_channel_url(link)
        
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
        
        # Загружаем общий список каналов
        channels = load_channels()
        
        # Проверяем, нет ли уже такого канала
        for ch in channels:
            if ch['url'] == link or ch['name'].lower() == channel_name.lower():
                await message.reply(
                    f"❌ Канал {channel_name} уже есть в общем списке!\n"
                    f"Ссылка: {ch['url']}",
                    reply_markup=get_main_keyboard(user_id)
                )
                del user_data[user_id]
                return
        
        # Добавляем канал в общий список
        channels.append({'name': channel_name, 'url': link})
        save_channels(channels)
        
        del user_data[user_id]
        
        await message.reply(
            f"✅ Канал успешно добавлен в общий список!\n\n"
            f"📌 Название: {channel_name}\n"
            f"🔗 Ссылка: {link}\n\n"
            f"Теперь этот канал доступен всем пользователям бота.\n"
            f"Всего каналов в базе: {len(channels)}",
            reply_markup=get_main_keyboard(user_id)
        )
        
    except Exception as e:
        await message.reply(f"❌ Ошибка: {str(e)}")

@dp.message_handler(lambda message: message.from_user.id in user_data and user_data[message.from_user.id].get('state') == 'waiting_keywords')
async def process_keywords(message: types.Message):
    """Обрабатывает ключевые слова и запускает сбор"""
    keywords = message.text.strip()
    user_id = message.from_user.id
    
    # Загружаем общий список каналов
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
    """Обрабатывает удаление канала (только для админа)"""
    await bot.answer_callback_query(callback_query.id)
    
    user_id = callback_query.from_user.id
    
    # Проверяем, админ ли пользователь
    if not is_admin(user_id):
        await bot.send_message(
            user_id,
            "❌ Удаление каналов доступно только администратору.",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    data = callback_query.data
    channels = load_channels()
    
    if data == "delete_all":
        save_channels([])
        await bot.send_message(
            user_id,
            "🗑 Все каналы удалены из общего списка",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    index = int(data.split('_')[1])
    if index < len(channels):
        deleted = channels.pop(index)
        save_channels(channels)
        
        await bot.send_message(
            user_id,
            f"🗑 Канал удален из общего списка\n\n"
            f"Название: {deleted['name']}\n"
            f"Ссылка: {deleted['url']}\n\n"
            f"Осталось каналов: {len(channels)}",
            reply_markup=get_main_keyboard(user_id)
        )

@dp.callback_query_handler(lambda c: c.data == 'back_to_main')
async def process_back_callback(callback_query: types.CallbackQuery):
    """Возврат в главное меню"""
    await bot.answer_callback_query(callback_query.id)
    
    user_id = callback_query.from_user.id
    
    # Очищаем состояние пользователя
    if user_id in user_data:
        del user_data[user_id]
    
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
        channels_with_posts = 0  # Счетчик каналов, в которых найдены посты
        
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
                # Нормализуем URL перед использованием
                channel_url = normalize_channel_url(channel['url'])
                entity = await client.get_entity(channel_url)
                
                posts_count = 0
                channel_posts = []  # Временное хранилище для постов канала
                
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
                        # Сохраняем пост во временное хранилище
                        channel_posts.append(message)
                        posts_count += 1
                        total_posts += 1
                        
                        if posts_count % 20 == 0:
                            await bot.send_message(user_id, f"📊 Найдено {posts_count} постов в канале {channel['name']}...")
                
                # Если есть посты, добавляем информацию о канале и сами посты
                if posts_count > 0:
                    channels_with_posts += 1
                    doc.add_heading(f"Канал: {channel['name']}", level=1)
                    doc.add_paragraph(f"Ссылка: {channel_url}")
                    doc.add_paragraph()
                    
                    # Добавляем все сохраненные посты
                    for message in channel_posts:
                        p = doc.add_paragraph()
                        
                        display_date = message.date
                        if display_date:
                            if display_date.tzinfo is not None:
                                display_date = display_date.astimezone()
                            p.add_run(f"📅 {display_date.strftime('%d.%m.%Y %H:%M')}\n").bold = True
                        
                        if message.text:
                            text = message.text
                            if len(text) > 5000:
                                text = text[:5000] + "..."
                            p.add_run(text)
                        
                        # Добавляем изображения, если нужно
                        if save_images and message.media:
                            if hasattr(message.media, 'photo'):
                                image_path = await download_media(message, user_id, client)
                                if image_path:
                                    doc.add_paragraph()
                                    add_image_to_doc(doc, image_path)
                            
                            if hasattr(message.media, 'document') and message.media.document:
                                if hasattr(message.media.document, 'mime_type'):
                                    mime = message.media.document.mime_type
                                    if 'image' in mime:
                                        image_path = await download_media(message, user_id, client)
                                        if image_path:
                                            doc.add_paragraph()
                                            add_image_to_doc(doc, image_path)
                        
                        # Добавляем активную ссылку на пост
                        if message.id:
                            channel_username = channel_url.split('/')[-1]
                            link_text = f"🔗 Ссылка на пост"
                            link_url = f"https://t.me/{channel_username}/{message.id}"
                            
                            link_paragraph = doc.add_paragraph()
                            add_hyperlink(link_paragraph, link_text, link_url)
                        
                        doc.add_paragraph()
                    
                    doc.add_paragraph(f"✅ Найдено постов: {posts_count}")
                    doc.add_page_break()
                else:
                    await bot.send_message(user_id, f"ℹ️ В канале {channel['name']} не найдено постов за выбранный период")
                
                if stop_flags.get(user_id, False):
                    stopped_early = True
                    await bot.send_message(user_id, f"⏹️ Анализ канала {channel['name']} прерван.")
                    break
                
            except FloodWaitError as e:
                wait_time = e.seconds
                await bot.send_message(
                    user_id, 
                    f"⚠️ Превышен лимит запросов. Жду {wait_time} секунд..."
                )
                await asyncio.sleep(wait_time)
                continue
                
            except Exception as e:
                error_msg = str(e)
                # Показываем нормализованный URL в ошибке
                channel_url = normalize_channel_url(channel['url'])
                await bot.send_message(user_id, f"⚠️ Ошибка с каналом {channel['name']}: {error_msg[:200]}")
                doc.add_paragraph(f"❌ Ошибка доступа к каналу: {channel['name']} ({channel_url})")
                doc.add_page_break()
        
        doc.add_heading('Итоговая статистика', level=1)
        doc.add_paragraph(f"Всего обработано каналов: {processed_channels}/{len(channels)}")
        doc.add_paragraph(f"Каналов с найденными постами: {channels_with_posts}")
        doc.add_paragraph(f"Всего найдено постов: {total_posts}")
        doc.add_paragraph(f"Период: последние {period_display}")
        doc.add_paragraph(f"Начало периода: {start_time.strftime('%d.%m.%Y %H:%M')}")
        doc.add_paragraph(f"Формат отчета: {'с картинками' if save_images else 'только текст'}")
        if stopped_early:
            doc.add_paragraph("⚠️ Отчет был остановлен досрочно по запросу пользователя")
        
        # Проверяем, есть ли вообще посты
        if total_posts == 0:
            await bot.send_message(
                user_id, 
                f"📭 Поиск завершен\n\n"
                f"За период {period_display} не найдено постов, соответствующих запросу: {keywords}\n"
                f"Проверено каналов: {len(channels)}",
                reply_markup=get_main_keyboard(user_id)
            )
            
            if user_id in user_data:
                del user_data[user_id]
            
            await client.disconnect()
            return
        
        output_file = f"report_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_file)
        
        with open(output_file, 'rb') as f:
            caption = f"✅ Отчет готов!\n\n"
            caption += f"📊 Найдено постов: {total_posts}\n"
            caption += f"📁 Каналов с постами: {channels_with_posts}/{processed_channels}\n"
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
    
    # Загружаем общий список каналов
    channels = load_channels()
    
    print("🤖 Бот запускается...")
    print(f"👑 Администратор ID: {ADMIN_ID}")
    print(f"📊 Всего каналов в базе: {len(channels)}")
    print(f"📁 Папки: {SESSIONS_DIR}, {IMAGES_DIR}, {UPLOADS_DIR}")
    
    # Показываем список каналов для проверки
    if channels:
        print("\n📋 Список каналов:")
        for i, ch in enumerate(channels, 1):
            print(f"  {i}. {ch['name']} - {ch['url']}")
    else:
        print("\n📭 Список каналов пуст")
    
    if MASTER_SESSION:
        print("✅ Мастер-сессия загружена")
    else:
        print("⚠️ Мастер-сессия не найдена. Администратору нужно авторизоваться при первом использовании.")
    
    # Запускаем бота
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)
