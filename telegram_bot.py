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
import aiohttp
from bs4 import BeautifulSoup
import requests
from urllib.parse import quote_plus, urlparse
import html
import xml.etree.ElementTree as ET

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
from telethon.tl.functions.messages import SearchGlobalRequest
from telethon.tl.types import InputMessagesFilterEmpty, InputMessagesFilterPhotos, InputMessagesFilterDocument, InputMessagesFilterVideo

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

# Часовой пояс UTC+10
UTC_PLUS_10 = timezone(timedelta(hours=10))

# Максимальный период в днях
MAX_PERIOD_DAYS = 30
MAX_PERIOD_HOURS = MAX_PERIOD_DAYS * 24

# Настройки для поиска в интернете
MAX_WEB_RESULTS = 50  # Максимальное количество результатов из интернета
SEARCH_ENGINES = ['google', 'bing']  # Доступные поисковики
MAX_SEARCH_TIME = 60  # Максимальное время поиска в секундах

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

# ========== ФУНКЦИИ ДЛЯ ПОИСКА В ИНТЕРНЕТЕ ==========
async def search_google(query, num_results=20):
    """Поиск в Google"""
    try:
        results = []
        search_query = quote_plus(query)
        url = f"https://www.google.com/search?q={search_query}&num={num_results}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Поиск результатов
                    for g in soup.find_all('div', class_='g'):
                        title_elem = g.find('h3')
                        link_elem = g.find('a')
                        desc_elem = g.find('div', class_='IsZvec')
                        
                        if title_elem and link_elem:
                            title = title_elem.get_text()
                            link = link_elem.get('href')
                            description = desc_elem.get_text() if desc_elem else ''
                            
                            # Очищаем ссылку
                            if link.startswith('/url?q='):
                                link = link.split('/url?q=')[1].split('&')[0]
                            
                            if link.startswith('http'):
                                results.append({
                                    'title': title,
                                    'link': link,
                                    'description': description,
                                    'source': 'Google'
                                })
                    
                    return results[:num_results]
    except Exception as e:
        print(f"Ошибка поиска в Google: {e}")
        return []

async def search_bing(query, num_results=20):
    """Поиск в Bing"""
    try:
        results = []
        search_query = quote_plus(query)
        url = f"https://www.bing.com/search?q={search_query}&count={num_results}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Поиск результатов
                    for li in soup.find_all('li', class_='b_algo'):
                        title_elem = li.find('h2')
                        link_elem = li.find('a')
                        desc_elem = li.find('p')
                        
                        if title_elem and link_elem:
                            title = title_elem.get_text()
                            link = link_elem.get('href')
                            description = desc_elem.get_text() if desc_elem else ''
                            
                            if link.startswith('http'):
                                results.append({
                                    'title': title,
                                    'link': link,
                                    'description': description,
                                    'source': 'Bing'
                                })
                    
                    return results[:num_results]
    except Exception as e:
        print(f"Ошибка поиска в Bing: {e}")
        return []

async def search_telegram_global(query, client, limit=100):
    """Глобальный поиск по всему Telegram"""
    try:
        results = []
        
        # Используем SearchGlobalRequest для глобального поиска
        result = await client(SearchGlobalRequest(
            q=query,
            filter=InputMessagesFilterEmpty(),
            min_date=None,
            max_date=None,
            offset_rate=0,
            offset_peer=None,
            offset_id=0,
            limit=limit
        ))
        
        if hasattr(result, 'messages'):
            for msg in result.messages:
                if hasattr(msg, 'message') and msg.message:
                    # Получаем информацию о чате
                    try:
                        chat = await client.get_entity(msg.peer_id)
                        chat_title = getattr(chat, 'title', 'Неизвестный чат')
                        chat_username = getattr(chat, 'username', None)
                    except:
                        chat_title = 'Неизвестный чат'
                        chat_username = None
                    
                    results.append({
                        'message': msg.message,
                        'date': msg.date,
                        'chat_title': chat_title,
                        'chat_username': chat_username,
                        'message_id': msg.id,
                        'source': 'Telegram Global'
                    })
        
        return results
    except Exception as e:
        print(f"Ошибка глобального поиска в Telegram: {e}")
        return []

async def search_telegram_channels(query, channels, client, period_hours, save_images, user_id):
    """Поиск по заданным каналам Telegram"""
    results = []
    start_time = datetime.now().astimezone() - timedelta(hours=period_hours)
    
    for channel in channels:
        if stop_flags.get(user_id, False):
            break
        
        try:
            channel_url = normalize_channel_url(channel['url'])
            entity = await client.get_entity(channel_url)
            
            async for message in client.iter_messages(entity, offset_date=datetime.now(), reverse=False):
                if stop_flags.get(user_id, False):
                    break
                
                if message.date:
                    msg_date = message.date
                    if msg_date.tzinfo is None:
                        msg_date = msg_date.replace(tzinfo=timezone.utc)
                    if msg_date < start_time:
                        break
                
                if message.text and query.lower() in message.text.lower():
                    results.append({
                        'message': message,
                        'channel_name': channel['name'],
                        'channel_url': channel_url,
                        'date': message.date,
                        'source': 'Telegram Channel'
                    })
        except Exception as e:
            print(f"Ошибка при поиске в канале {channel['name']}: {e}")
            continue
    
    return results

# ========== ФУНКЦИИ ДЛЯ ПРОВЕРКИ АДМИНА ==========
def is_admin(user_id):
    """Проверяет, является ли пользователь администратором"""
    return user_id == ADMIN_ID

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С ПЕРИОДАМИ ==========
def parse_period(text):
    """Парсит текстовое представление периода и возвращает количество часов"""
    text = text.lower().strip()
    
    # Убираем возможные точки в конце
    text = text.rstrip('.')
    
    # Паттерны для русских единиц измерения
    patterns = [
        (r'^(\d+)\s*(?:час|часа|часов|ч)$', 1),  # часы
        (r'^(\d+)\s*(?:минут|минута|минуты|мин|м)$', 1/60),  # минуты
        (r'^(\d+)\s*(?:день|дня|дней|д|сут|суток)$', 24),  # дни
    ]
    
    for pattern, multiplier in patterns:
        match = re.match(pattern, text)
        if match:
            value = int(match.group(1))
            return value * multiplier
    
    # Если просто число - считаем что это часы
    match = re.match(r'^(\d+)$', text)
    if match:
        return int(match.group(1))
    
    return None

def format_period(period_hours):
    """Форматирует количество часов в читаемый текст"""
    if period_hours < 1:
        minutes = int(period_hours * 60)
        if minutes == 1:
            return "1 минута"
        elif minutes < 5:
            return f"{minutes} минуты"
        else:
            return f"{minutes} минут"
    elif period_hours < 24:
        if period_hours == 1:
            return "1 час"
        elif period_hours < 5:
            return f"{int(period_hours)} часа"
        else:
            return f"{int(period_hours)} часов"
    else:
        days = period_hours / 24
        if days == 1:
            return "1 день"
        elif days < 5:
            return f"{int(days)} дня"
        else:
            return f"{int(days)} дней"

def format_datetime_utc10(dt):
    """Форматирует дату с часовым поясом UTC+10"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Переводим в UTC+10
    dt_utc10 = dt.astimezone(UTC_PLUS_10)
    return dt_utc10.strftime('%d.%m.%Y %H:%M')

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(user_id):
    """Главная клавиатура с кнопками"""
    
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("📋 Список каналов"),
        KeyboardButton("➕ Добавить канал"),
        KeyboardButton("📤 Импорт каналов"),
        KeyboardButton("📥 Экспорт каналов"),
        KeyboardButton("🔍 Расширенный поиск"),
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

def get_search_type_keyboard():
    """Клавиатура для выбора типа поиска"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("📱 По каналам (из базы)"),
        KeyboardButton("🌍 По всему Telegram"),
        KeyboardButton("🔎 Поиск в интернете"),
        KeyboardButton("⚡ Комбинированный поиск"),
        KeyboardButton("◀️ Назад в меню")
    )
    return keyboard

def get_web_search_keyboard():
    """Клавиатура для выбора поисковой системы"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🔍 Google"),
        KeyboardButton("🔎 Bing"),
        KeyboardButton("🌐 Все поисковики"),
        KeyboardButton("◀️ Назад")
    )
    return keyboard

def get_period_keyboard():
    """Клавиатура для выбора периода"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🕐 1 час"),
        KeyboardButton("🕒 3 часа"),
        KeyboardButton("🕖 7 часов"),
        KeyboardButton("📅 24 часа"),
        KeyboardButton("📆 3 дня"),
        KeyboardButton("📆 7 дней"),
        KeyboardButton("⌨️ Свой период"),
        KeyboardButton("◀️ Назад")
    )
    return keyboard

def get_custom_period_keyboard():
    """Клавиатура для ввода своего периода"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    keyboard.add(
        KeyboardButton("◀️ Назад")
    )
    return keyboard

def get_image_option_keyboard():
    """Клавиатура для выбора - сохранять изображения или нет"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("🖼️ С картинками"),
        KeyboardButton("📝 Только текст"),
        KeyboardButton("◀️ Назад")
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
        "Я бот для сбора информации из Telegram-каналов и интернета.\n\n"
        "📊 Общий список каналов доступен всем пользователям\n"
        "✏️ Все пользователи могут добавлять каналы\n\n"
        "🔍 Расширенный поиск:\n"
        "• 📱 По каналам из базы - поиск только по вашим каналам\n"
        "• 🌍 По всему Telegram - глобальный поиск по всем каналам\n"
        "• 🔎 Поиск в интернете - поиск в Google и Bing\n"
        "• ⚡ Комбинированный - сразу по всем источникам\n\n"
        "Как пользоваться:\n"
        "1️⃣ Нажми '➕ Добавить канал' и отправь ссылку на канал\n"
        "2️⃣ Канал добавится в общий список для всех\n"
        "3️⃣ Нажми '🔍 Расширенный поиск' и выбери тип поиска\n"
        "4️⃣ Введи ключевые слова\n"
        "5️⃣ Выбери период времени (можно ввести свой)\n"
        "6️⃣ Выбери формат отчета\n\n"
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
            "Нажми '🔍 Расширенный поиск' для авторизации."
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

@dp.message_handler(lambda message: message.text == "🔍 Расширенный поиск")
async def extended_search(message: types.Message):
    """Расширенный поиск с выбором источника"""
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
                "❌ Мастер-сессия не найдена. Обратитесь к администратору.\n"
                "Доступен только поиск в интернете.",
                reply_markup=get_search_type_keyboard()
            )
            user_data[user_id] = {'state': 'waiting_search_type'}
        return
    
    await message.reply(
        "🔍 Выберите источник поиска:\n\n"
        "• 📱 По каналам (из базы) - поиск только по вашим каналам\n"
        "• 🌍 По всему Telegram - глобальный поиск по всем каналам\n"
        "• 🔎 Поиск в интернете - поиск в Google и Bing\n"
        "• ⚡ Комбинированный поиск - сразу по всем источникам",
        reply_markup=get_search_type_keyboard()
    )
    
    user_data[user_id] = {'state': 'waiting_search_type'}

@dp.message_handler(lambda message: message.text == "📱 По каналам (из базы)")
async def search_channels_only(message: types.Message):
    """Поиск только по каналам из базы"""
    user_id = message.from_user.id
    
    channels = load_channels()
    
    if not channels:
        await message.reply(
            "❌ Сначала добавьте каналы через '➕ Добавить канал'",
            reply_markup=get_main_keyboard(user_id)
        )
        return
    
    user_data[user_id] = {
        'state': 'waiting_keywords',
        'search_type': 'channels',
        'channels': channels
    }
    
    await message.reply(
        "🔍 Введите ключевые слова для поиска по каналам:",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.text == "🌍 По всему Telegram")
async def search_global_telegram(message: types.Message):
    """Глобальный поиск по всему Telegram"""
    user_id = message.from_user.id
    
    global MASTER_SESSION
    if not MASTER_SESSION:
        if is_admin(user_id):
            await message.reply(
                "🔄 Мастер-сессия не найдена. Начинаю процесс авторизации...",
                reply_markup=get_auth_keyboard()
            )
            await start_authorization(user_id, message)
        else:
            await message.reply(
                "❌ Для глобального поиска в Telegram нужна мастер-сессия.\n"
                "Обратитесь к администратору.",
                reply_markup=get_main_keyboard(user_id)
            )
        return
    
    user_data[user_id] = {
        'state': 'waiting_keywords',
        'search_type': 'global_telegram'
    }
    
    await message.reply(
        "🔍 Введите ключевые слова для глобального поиска по Telegram:",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.text == "🔎 Поиск в интернете")
async def search_web(message: types.Message):
    """Поиск в интернете"""
    user_id = message.from_user.id
    
    await message.reply(
        "🌐 Выберите поисковую систему:\n\n"
        "• 🔍 Google\n"
        "• 🔎 Bing\n"
        "• 🌐 Все поисковики (комбинированный результат)",
        reply_markup=get_web_search_keyboard()
    )
    
    user_data[user_id] = {'state': 'waiting_web_search_type'}

@dp.message_handler(lambda message: message.text == "⚡ Комбинированный поиск")
async def search_combined(message: types.Message):
    """Комбинированный поиск по всем источникам"""
    user_id = message.from_user.id
    
    global MASTER_SESSION
    if not MASTER_SESSION:
        if is_admin(user_id):
            await message.reply(
                "🔄 Мастер-сессия не найдена. Начинаю процесс авторизации...",
                reply_markup=get_auth_keyboard()
            )
            await start_authorization(user_id, message)
        else:
            await message.reply(
                "❌ Для комбинированного поиска нужна мастер-сессия.\n"
                "Будет выполнен только поиск в интернете.",
                reply_markup=get_main_keyboard(user_id)
            )
            user_data[user_id] = {
                'state': 'waiting_keywords',
                'search_type': 'web_only'
            }
            await message.reply("🔍 Введите ключевые слова для поиска в интернете:")
        return
    
    channels = load_channels()
    
    user_data[user_id] = {
        'state': 'waiting_keywords',
        'search_type': 'combined',
        'channels': channels
    }
    
    await message.reply(
        "🔍 Введите ключевые слова для комбинированного поиска\n"
        "(по каналам, глобальный Telegram и интернет):",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.text == "🔍 Google")
async def search_google_only(message: types.Message):
    """Поиск только в Google"""
    user_id = message.from_user.id
    
    user_data[user_id] = {
        'state': 'waiting_keywords',
        'search_type': 'google'
    }
    
    await message.reply(
        "🔍 Введите ключевые слова для поиска в Google:",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.text == "🔎 Bing")
async def search_bing_only(message: types.Message):
    """Поиск только в Bing"""
    user_id = message.from_user.id
    
    user_data[user_id] = {
        'state': 'waiting_keywords',
        'search_type': 'bing'
    }
    
    await message.reply(
        "🔎 Введите ключевые слова для поиска в Bing:",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.text == "🌐 Все поисковики")
async def search_all_engines(message: types.Message):
    """Поиск во всех поисковиках"""
    user_id = message.from_user.id
    
    user_data[user_id] = {
        'state': 'waiting_keywords',
        'search_type': 'all_engines'
    }
    
    await message.reply(
        "🌐 Введите ключевые слова для поиска во всех поисковиках:",
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.text == "🔄 Собрать всё")
async def collect_all(message: types.Message):
    """Собрать все посты без фильтра (только для админа)"""
    user_id = message.from_user.id
    
    # Проверяем, админ ли пользователь
    if not is_admin(user_id):
        await message.reply(
            "❌ Эта функция доступна только администратору.\n"
            "Используйте '🔍 Расширенный поиск' для поиска.",
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
        f"Максимальный период: {MAX_PERIOD_DAYS} дней\n"
        "За какой период собирать все посты?",
        reply_markup=get_period_keyboard()
    )
    
    user_data[user_id] = {
        'state': 'waiting_period',
        'keywords': 'все',
        'search_type': 'channels',
        'channels': channels
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
        "🔍 Расширенный поиск - поиск с выбором источника\n"
        "⏹️ Стоп - остановить формирование отчета\n\n"
        f"📊 Всего каналов в базе: {len(channels)}\n\n"
        "🔍 Типы поиска:\n"
        "• 📱 По каналам - поиск только по добавленным каналам\n"
        "• 🌍 По всему Telegram - глобальный поиск по всем каналам\n"
        "• 🔎 Поиск в интернете - поиск в Google и Bing\n"
        "• ⚡ Комбинированный - сразу по всем источникам\n\n"
        "После выбора периода появится выбор:\n"
        "🖼️ С картинками - сохранить изображения в отчет\n"
        "📝 Только текст - только текст, без картинок\n\n"
        "⌨️ Свой период - можно ввести любой период, например:\n"
        "• 30 минут\n"
        "• 2 часа\n"
        "• 5 дней\n"
        f"• Максимум: {MAX_PERIOD_DAYS} дней\n\n"
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

@dp.message_handler(lambda message: message.text == "◀️ Назад")
async def back_to_previous(message: types.Message):
    """Возврат к предыдущему меню"""
    user_id = message.from_user.id
    
    if user_id in user_data:
        current_state = user_data[user_id].get('state')
        
        if current_state == 'waiting_search_type':
            del user_data[user_id]
            await message.reply(
                "Главное меню:",
                reply_markup=get_main_keyboard(user_id)
            )
        elif current_state == 'waiting_web_search_type':
            user_data[user_id]['state'] = 'waiting_search_type'
            await message.reply(
                "🔍 Выберите источник поиска:",
                reply_markup=get_search_type_keyboard()
            )
        elif current_state in ['waiting_keywords', 'waiting_period']:
            del user_data[user_id]
            await message.reply(
                "Главное меню:",
                reply_markup=get_main_keyboard(user_id)
            )
        else:
            del user_data[user_id]
            await message.reply(
                "Главное меню:",
                reply_markup=get_main_keyboard(user_id)
            )
    else:
        await message.reply(
            "Главное меню:",
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
    elif period_text == "⌨️ Свой период":
        await message.reply(
            f"⌨️ Введите свой период\n\n"
            f"Примеры:\n"
            f"• 30 минут\n"
            f"• 2 часа\n"
            f"• 5 дней\n"
            f"• 90 мин\n"
            f"• 24\n\n"
            f"Максимальный период - {MAX_PERIOD_DAYS} дней\n"
            f"(можно писать слитно: 30минут, 2часа, 5дней)",
            reply_markup=get_custom_period_keyboard()
        )
        user_data[user_id]['state'] = 'waiting_custom_period'
        return
    elif period_text == "◀️ Назад":
        if 'search_type' in user_data[user_id]:
            await message.reply(
                "🔍 Выберите источник поиска:",
                reply_markup=get_search_type_keyboard()
            )
            user_data[user_id]['state'] = 'waiting_search_type'
        else:
            await message.reply(
                "Главное меню:",
                reply_markup=get_main_keyboard(user_id)
            )
            del user_data[user_id]
        return
    else:
        await message.reply(
            "❌ Пожалуйста, выбери период из кнопок",
            reply_markup=get_period_keyboard()
        )
        return
    
    user_data[user_id]['period_hours'] = period_hours
    user_data[user_id]['period_text'] = period_text
    
    await message.reply(
        "🖼️ Выбери формат отчета:\n\n"
        "• С картинками - будут сохранены все изображения\n"
        "• Только текст - только текст, без картинок (быстрее)",
        reply_markup=get_image_option_keyboard()
    )
    
    user_data[user_id]['state'] = 'waiting_image_option'

@dp.message_handler(lambda message: message.from_user.id in user_data and user_data[message.from_user.id].get('state') == 'waiting_custom_period')
async def process_custom_period(message: types.Message):
    """Обработка ввода своего периода"""
    user_id = message.from_user.id
    period_text = message.text
    
    if period_text == "◀️ Назад":
        await message.reply(
            "⏱ Выбери период времени",
            reply_markup=get_period_keyboard()
        )
        user_data[user_id]['state'] = 'waiting_period'
        return
    
    period_hours = parse_period(period_text)
    
    if period_hours is None or period_hours <= 0:
        await message.reply(
            "❌ Не удалось распознать период.\n\n"
            "Примеры правильного ввода:\n"
            "• 30 минут\n"
            "• 2 часа\n"
            "• 5 дней\n"
            "• 90 мин\n"
            "• 24\n\n"
            f"Максимальный период - {MAX_PERIOD_DAYS} дней\n"
            "Попробуйте еще раз:",
            reply_markup=get_custom_period_keyboard()
        )
        return
    
    if period_hours > MAX_PERIOD_HOURS:
        await message.reply(
            f"❌ Период не может превышать {MAX_PERIOD_DAYS} дней.\n"
            f"Введите меньший период:",
            reply_markup=get_custom_period_keyboard()
        )
        return
    
    user_data[user_id]['period_hours'] = period_hours
    user_data[user_id]['period_text'] = period_text
    
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
    
    if option == "◀️ Назад":
        await message.reply(
            "⏱ Выбери период времени",
            reply_markup=get_period_keyboard()
        )
        user_data[user_id]['state'] = 'waiting_period'
        return
    elif option not in ["🖼️ С картинками", "📝 Только текст"]:
        await message.reply(
            "❌ Пожалуйста, выбери вариант из кнопок",
            reply_markup=get_image_option_keyboard()
        )
        return
    
    save_images = (option == "🖼️ С картинками")
    user_data[user_id]['save_images'] = save_images
    
    # Запускаем сбор в зависимости от типа поиска
    search_type = user_data[user_id].get('search_type', 'channels')
    
    if search_type == 'channels':
        await message.reply(
            f"🔍 Начинаю поиск по каналам\n"
            f"Период: {user_data[user_id]['period_text']}\n"
            f"Формат: {'с картинками' if save_images else 'только текст'}\n\n"
            f"⏳ Это займет некоторое время...\n"
            f"Чтобы остановить, нажми '⏹️ Стоп'",
            reply_markup=get_main_keyboard(user_id)
        )
        await collect_from_channels(user_id)
    
    elif search_type == 'global_telegram':
        await message.reply(
            f"🌍 Начинаю глобальный поиск по Telegram\n"
            f"Период: {user_data[user_id]['period_text']}\n"
            f"Формат: {'с картинками' if save_images else 'только текст'}\n\n"
            f"⏳ Это займет некоторое время...\n"
            f"Чтобы остановить, нажми '⏹️ Стоп'",
            reply_markup=get_main_keyboard(user_id)
        )
        await collect_global_telegram(user_id)
    
    elif search_type in ['google', 'bing', 'all_engines']:
        await message.reply(
            f"🔎 Начинаю поиск в интернете\n"
            f"Поисковик: {search_type}\n"
            f"Формат: {'с картинками' if save_images else 'только текст'}\n\n"
            f"⏳ Это займет некоторое время...\n"
            f"Чтобы остановить, нажми '⏹️ Стоп'",
            reply_markup=get_main_keyboard(user_id)
        )
        await collect_from_web(user_id)
    
    elif search_type == 'combined':
        await message.reply(
            f"⚡ Начинаю комбинированный поиск\n"
            f"• По каналам из базы\n"
            f"• Глобальный поиск по Telegram\n"
            f"• Поиск в интернете\n"
            f"Период: {user_data[user_id]['period_text']}\n"
            f"Формат: {'с картинками' if save_images else 'только текст'}\n\n"
            f"⏳ Это займет некоторое время...\n"
            f"Чтобы остановить, нажми '⏹️ Стоп'",
            reply_markup=get_main_keyboard(user_id)
        )
        await collect_combined(user_id)
    
    elif search_type == 'web_only':
        await message.reply(
            f"🔎 Начинаю поиск в интернете\n"
            f"Формат: {'с картинками' if save_images else 'только текст'}\n\n"
            f"⏳ Это займет некоторое время...\n"
            f"Чтобы остановить, нажми '⏹️ Стоп'",
            reply_markup=get_main_keyboard(user_id)
        )
        await collect_from_web(user_id)

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
            f"Нажми '🔍 Расширенный поиск' для начала работы.",
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
            f"Нажми '🔍 Расширенный поиск' для начала работы.",
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
    """Обрабатывает ключевые слова и запускает выбор периода"""
    keywords = message.text.strip()
    user_id = message.from_user.id
    
    user_data[user_id]['keywords'] = keywords
    
    await message.reply(
        "⏱ Выбери период времени\n\n"
        f"Максимальный период: {MAX_PERIOD_DAYS} дней\n"
        "За какой период собирать информацию?",
        reply_markup=get_period_keyboard()
    )
    
    user_data[user_id]['state'] = 'waiting_period'

# ========== ФУНКЦИИ СБОРА ДАННЫХ ==========
async def collect_from_channels(user_id):
    """Сбор данных из каналов базы"""
    client = None
    try:
        stop_flags[user_id] = False
        
        keywords = user_data[user_id]['keywords']
        channels = user_data[user_id].get('channels', load_channels())
        period_hours = user_data[user_id].get('period_hours', 168)
        save_images = user_data[user_id].get('save_images', True)
        
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
        
        await bot.send_message(user_id, f"✅ Подключение установлено! Начинаю сбор...")
        
        doc = Document()
        
        title = doc.add_heading('Отчёт по Telegram-каналам', 0)
        title.alignment = 1
        
        now_utc10 = datetime.now(UTC_PLUS_10)
        doc.add_paragraph(f"Дата формирования: {now_utc10.strftime('%d.%m.%Y %H:%M')} (UTC+10)")
        doc.add_paragraph(f"Тип поиска: По каналам из базы")
        doc.add_paragraph(f"Ключевые слова: {keywords}")
        doc.add_paragraph(f"Период: {user_data[user_id].get('period_text', format_period(period_hours))}")
        doc.add_paragraph()
        
        total_posts = 0
        processed_channels = 0
        stopped_early = False
        channels_with_posts = 0
        
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
                channel_url = normalize_channel_url(channel['url'])
                entity = await client.get_entity(channel_url)
                
                posts_count = 0
                channel_posts = []
                
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
                    
                    if keywords.lower() == 'все' or (message.text and keywords.lower() in message.text.lower()):
                        channel_posts.append(message)
                        posts_count += 1
                        total_posts += 1
                        
                        if posts_count % 20 == 0:
                            await bot.send_message(user_id, f"📊 Найдено {posts_count} постов в канале {channel['name']}...")
                
                if posts_count > 0:
                    channels_with_posts += 1
                    doc.add_heading(f"Канал: {channel['name']}", level=1)
                    doc.add_paragraph(f"Ссылка: {channel_url}")
                    doc.add_paragraph()
                    
                    for message in channel_posts:
                        p = doc.add_paragraph()
                        
                        display_date = format_datetime_utc10(message.date)
                        p.add_run(f"📅 {display_date}\n").bold = True
                        
                        if message.text:
                            text = message.text
                            if len(text) > 5000:
                                text = text[:5000] + "..."
                            p.add_run(text)
                        
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
                        
                        if message.id:
                            channel_username = channel_url.split('/')[-1]
                            link_text = f"🔗 Ссылка на пост"
                            link_url = f"https://t.me/{channel_username}/{message.id}"
                            
                            link_paragraph = doc.add_paragraph()
                            add_hyperlink(link_paragraph, link_text, link_url)
                        
                        doc.add_paragraph()
                    
                    doc.add_paragraph(f"✅ Найдено постов: {posts_count}")
                    doc.add_page_break()
                
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
                await bot.send_message(user_id, f"⚠️ Ошибка с каналом {channel['name']}: {error_msg[:200]}")
                doc.add_paragraph(f"❌ Ошибка доступа к каналу: {channel['name']}")
                doc.add_page_break()
        
        doc.add_heading('Итоговая статистика', level=1)
        doc.add_paragraph(f"Всего обработано каналов: {processed_channels}/{len(channels)}")
        doc.add_paragraph(f"Каналов с найденными постами: {channels_with_posts}")
        doc.add_paragraph(f"Всего найдено постов: {total_posts}")
        doc.add_paragraph(f"Период: {user_data[user_id].get('period_text', format_period(period_hours))}")
        doc.add_paragraph(f"Формат отчета: {'с картинками' if save_images else 'только текст'}")
        if stopped_early:
            doc.add_paragraph("⚠️ Отчет был остановлен досрочно по запросу пользователя")
        
        if total_posts == 0:
            await bot.send_message(
                user_id, 
                f"📭 Поиск завершен\n\n"
                f"За период {user_data[user_id].get('period_text', format_period(period_hours))} не найдено постов, соответствующих запросу: {keywords}\n"
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
            caption += f"⏱ Период: {user_data[user_id].get('period_text', format_period(period_hours))}\n"
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

async def collect_global_telegram(user_id):
    """Глобальный поиск по всему Telegram"""
    client = None
    try:
        stop_flags[user_id] = False
        
        keywords = user_data[user_id]['keywords']
        period_hours = user_data[user_id].get('period_hours', 168)
        save_images = user_data[user_id].get('save_images', True)
        
        await bot.send_message(user_id, "🔄 Подключаюсь к Telegram для глобального поиска...")
        
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
        
        await bot.send_message(user_id, f"✅ Подключение установлено! Выполняю глобальный поиск...")
        
        doc = Document()
        
        title = doc.add_heading('Отчёт по глобальному поиску в Telegram', 0)
        title.alignment = 1
        
        now_utc10 = datetime.now(UTC_PLUS_10)
        doc.add_paragraph(f"Дата формирования: {now_utc10.strftime('%d.%m.%Y %H:%M')} (UTC+10)")
        doc.add_paragraph(f"Тип поиска: Глобальный по всему Telegram")
        doc.add_paragraph(f"Ключевые слова: {keywords}")
        doc.add_paragraph(f"Период: {user_data[user_id].get('period_text', format_period(period_hours))}")
        doc.add_paragraph()
        
        start_time = datetime.now().astimezone() - timedelta(hours=period_hours)
        
        # Выполняем глобальный поиск
        await bot.send_message(user_id, "🌍 Ищу сообщения по всему Telegram...")
        
        try:
            result = await client(SearchGlobalRequest(
                q=keywords,
                filter=InputMessagesFilterEmpty(),
                min_date=start_time,
                max_date=datetime.now().astimezone(),
                offset_rate=0,
                offset_peer=None,
                offset_id=0,
                limit=100
            ))
            
            total_posts = 0
            
            if hasattr(result, 'messages') and result.messages:
                for msg in result.messages:
                    if stop_flags.get(user_id, False):
                        break
                    
                    if hasattr(msg, 'message') and msg.message:
                        total_posts += 1
                        
                        # Получаем информацию о чате
                        try:
                            chat = await client.get_entity(msg.peer_id)
                            chat_title = getattr(chat, 'title', 'Неизвестный чат')
                            chat_username = getattr(chat, 'username', None)
                        except:
                            chat_title = 'Неизвестный чат'
                            chat_username = None
                        
                        doc.add_heading(f"Чат: {chat_title}", level=2)
                        if chat_username:
                            doc.add_paragraph(f"Ссылка: https://t.me/{chat_username}")
                        
                        p = doc.add_paragraph()
                        display_date = format_datetime_utc10(msg.date)
                        p.add_run(f"📅 {display_date}\n").bold = True
                        
                        if msg.message:
                            text = msg.message
                            if len(text) > 5000:
                                text = text[:5000] + "..."
                            p.add_run(text)
                        
                        if save_images and msg.media:
                            if hasattr(msg.media, 'photo'):
                                image_path = await download_media(msg, user_id, client)
                                if image_path:
                                    doc.add_paragraph()
                                    add_image_to_doc(doc, image_path)
                        
                        if chat_username and msg.id:
                            link_text = f"🔗 Ссылка на сообщение"
                            link_url = f"https://t.me/{chat_username}/{msg.id}"
                            link_paragraph = doc.add_paragraph()
                            add_hyperlink(link_paragraph, link_text, link_url)
                        
                        doc.add_paragraph()
                        
                        if total_posts % 10 == 0:
                            await bot.send_message(user_id, f"📊 Найдено {total_posts} сообщений...")
            
            doc.add_heading('Итоговая статистика', level=1)
            doc.add_paragraph(f"Всего найдено сообщений: {total_posts}")
            doc.add_paragraph(f"Период: {user_data[user_id].get('period_text', format_period(period_hours))}")
            doc.add_paragraph(f"Формат отчета: {'с картинками' if save_images else 'только текст'}")
            
            if total_posts == 0:
                await bot.send_message(
                    user_id, 
                    f"📭 Глобальный поиск завершен\n\n"
                    f"За период {user_data[user_id].get('period_text', format_period(period_hours))} не найдено сообщений, соответствующих запросу: {keywords}",
                    reply_markup=get_main_keyboard(user_id)
                )
                
                if user_id in user_data:
                    del user_data[user_id]
                
                await client.disconnect()
                return
            
            output_file = f"global_report_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
            doc.save(output_file)
            
            with open(output_file, 'rb') as f:
                caption = f"✅ Глобальный поиск завершен!\n\n"
                caption += f"📊 Найдено сообщений: {total_posts}\n"
                caption += f"🔍 Ключевые слова: {keywords}\n"
                caption += f"⏱ Период: {user_data[user_id].get('period_text', format_period(period_hours))}\n"
                caption += f"🖼️ Формат: {'с картинками' if save_images else 'только текст'}\n"
                
                await bot.send_document(
                    user_id, 
                    f, 
                    caption=caption
                )
            
            if os.path.exists(output_file):
                os.remove(output_file)
            
        except Exception as e:
            await bot.send_message(user_id, f"❌ Ошибка при глобальном поиске: {str(e)}")
        
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

async def collect_from_web(user_id):
    """Сбор данных из интернета"""
    try:
        stop_flags[user_id] = False
        
        keywords = user_data[user_id]['keywords']
        search_type = user_data[user_id].get('search_type', 'google')
        save_images = user_data[user_id].get('save_images', True)
        
        await bot.send_message(user_id, "🌐 Начинаю поиск в интернете...")
        
        doc = Document()
        
        title = doc.add_heading('Отчёт по поиску в интернете', 0)
        title.alignment = 1
        
        now_utc10 = datetime.now(UTC_PLUS_10)
        doc.add_paragraph(f"Дата формирования: {now_utc10.strftime('%d.%m.%Y %H:%M')} (UTC+10)")
        doc.add_paragraph(f"Тип поиска: {search_type}")
        doc.add_paragraph(f"Ключевые слова: {keywords}")
        doc.add_paragraph()
        
        total_results = 0
        
        # Поиск в Google
        if search_type in ['google', 'all_engines']:
            await bot.send_message(user_id, "🔍 Ищу в Google...")
            google_results = await search_google(keywords, 20)
            
            if google_results:
                doc.add_heading('Результаты Google', level=1)
                for result in google_results:
                    if stop_flags.get(user_id, False):
                        break
                    
                    doc.add_heading(result['title'], level=2)
                    doc.add_paragraph(f"Источник: {result['source']}")
                    
                    p = doc.add_paragraph()
                    p.add_run("Ссылка: ").bold = True
                    add_hyperlink(p, result['link'], result['link'])
                    
                    if result['description']:
                        doc.add_paragraph(result['description'])
                    
                    doc.add_paragraph()
                    total_results += 1
                
                doc.add_page_break()
        
        # Поиск в Bing
        if search_type in ['bing', 'all_engines'] and not stop_flags.get(user_id, False):
            await bot.send_message(user_id, "🔎 Ищу в Bing...")
            bing_results = await search_bing(keywords, 20)
            
            if bing_results:
                doc.add_heading('Результаты Bing', level=1)
                for result in bing_results:
                    if stop_flags.get(user_id, False):
                        break
                    
                    doc.add_heading(result['title'], level=2)
                    doc.add_paragraph(f"Источник: {result['source']}")
                    
                    p = doc.add_paragraph()
                    p.add_run("Ссылка: ").bold = True
                    add_hyperlink(p, result['link'], result['link'])
                    
                    if result['description']:
                        doc.add_paragraph(result['description'])
                    
                    doc.add_paragraph()
                    total_results += 1
        
        doc.add_heading('Итоговая статистика', level=1)
        doc.add_paragraph(f"Всего найдено результатов: {total_results}")
        doc.add_paragraph(f"Ключевые слова: {keywords}")
        doc.add_paragraph(f"Поисковые системы: {search_type}")
        
        if total_results == 0:
            await bot.send_message(
                user_id, 
                f"📭 Поиск в интернете завершен\n\n"
                f"Не найдено результатов для запроса: {keywords}",
                reply_markup=get_main_keyboard(user_id)
            )
            
            if user_id in user_data:
                del user_data[user_id]
            
            return
        
        output_file = f"web_report_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_file)
        
        with open(output_file, 'rb') as f:
            caption = f"✅ Поиск в интернете завершен!\n\n"
            caption += f"📊 Найдено результатов: {total_results}\n"
            caption += f"🔍 Ключевые слова: {keywords}\n"
            caption += f"🌐 Поисковики: {search_type}\n"
            
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

async def collect_combined(user_id):
    """Комбинированный сбор данных из всех источников"""
    try:
        stop_flags[user_id] = False
        
        keywords = user_data[user_id]['keywords']
        period_hours = user_data[user_id].get('period_hours', 168)
        save_images = user_data[user_id].get('save_images', True)
        
        await bot.send_message(user_id, "⚡ Начинаю комбинированный поиск...")
        
        doc = Document()
        
        title = doc.add_heading('Комбинированный отчёт', 0)
        title.alignment = 1
        
        now_utc10 = datetime.now(UTC_PLUS_10)
        doc.add_paragraph(f"Дата формирования: {now_utc10.strftime('%d.%m.%Y %H:%M')} (UTC+10)")
        doc.add_paragraph(f"Тип поиска: Комбинированный")
        doc.add_paragraph(f"Ключевые слова: {keywords}")
        doc.add_paragraph(f"Период: {user_data[user_id].get('period_text', format_period(period_hours))}")
        doc.add_paragraph()
        
        total_results = 0
        
        # Поиск в интернете
        if not stop_flags.get(user_id, False):
            await bot.send_message(user_id, "🌐 Ищу в интернете...")
            doc.add_heading('РЕЗУЛЬТАТЫ ИЗ ИНТЕРНЕТА', level=1)
            
            google_results = await search_google(keywords, 10)
            if google_results:
                doc.add_heading('Google', level=2)
                for result in google_results:
                    if stop_flags.get(user_id, False):
                        break
                    
                    doc.add_heading(result['title'], level=3)
                    p = doc.add_paragraph()
                    p.add_run("Ссылка: ").bold = True
                    add_hyperlink(p, result['link'], result['link'])
                    
                    if result['description']:
                        doc.add_paragraph(result['description'])
                    
                    doc.add_paragraph()
                    total_results += 1
            
            bing_results = await search_bing(keywords, 10)
            if bing_results and not stop_flags.get(user_id, False):
                doc.add_heading('Bing', level=2)
                for result in bing_results:
                    if stop_flags.get(user_id, False):
                        break
                    
                    doc.add_heading(result['title'], level=3)
                    p = doc.add_paragraph()
                    p.add_run("Ссылка: ").bold = True
                    add_hyperlink(p, result['link'], result['link'])
                    
                    if result['description']:
                        doc.add_paragraph(result['description'])
                    
                    doc.add_paragraph()
                    total_results += 1
            
            doc.add_page_break()
        
        # Глобальный поиск по Telegram
        if not stop_flags.get(user_id, False) and MASTER_SESSION:
            await bot.send_message(user_id, "🌍 Ищу по всему Telegram...")
            doc.add_heading('РЕЗУЛЬТАТЫ ИЗ TELEGRAM (ГЛОБАЛЬНЫЙ ПОИСК)', level=1)
            
            client = TelegramClient(StringSession(MASTER_SESSION), API_ID, API_HASH)
            await client.connect()
            
            if await client.is_user_authorized():
                start_time = datetime.now().astimezone() - timedelta(hours=period_hours)
                
                try:
                    result = await client(SearchGlobalRequest(
                        q=keywords,
                        filter=InputMessagesFilterEmpty(),
                        min_date=start_time,
                        max_date=datetime.now().astimezone(),
                        offset_rate=0,
                        offset_peer=None,
                        offset_id=0,
                        limit=50
                    ))
                    
                    telegram_count = 0
                    if hasattr(result, 'messages') and result.messages:
                        for msg in result.messages[:20]:  # Ограничиваем до 20 сообщений
                            if stop_flags.get(user_id, False):
                                break
                            
                            if hasattr(msg, 'message') and msg.message:
                                telegram_count += 1
                                
                                try:
                                    chat = await client.get_entity(msg.peer_id)
                                    chat_title = getattr(chat, 'title', 'Неизвестный чат')
                                    chat_username = getattr(chat, 'username', None)
                                except:
                                    chat_title = 'Неизвестный чат'
                                    chat_username = None
                                
                                doc.add_heading(f"Чат: {chat_title}", level=2)
                                if chat_username:
                                    doc.add_paragraph(f"Ссылка: https://t.me/{chat_username}")
                                
                                p = doc.add_paragraph()
                                display_date = format_datetime_utc10(msg.date)
                                p.add_run(f"📅 {display_date}\n").bold = True
                                
                                if msg.message:
                                    text = msg.message
                                    if len(text) > 1000:
                                        text = text[:1000] + "..."
                                    p.add_run(text)
                                
                                if chat_username and msg.id:
                                    link_text = f"🔗 Ссылка на сообщение"
                                    link_url = f"https://t.me/{chat_username}/{msg.id}"
                                    link_paragraph = doc.add_paragraph()
                                    add_hyperlink(link_paragraph, link_text, link_url)
                                
                                doc.add_paragraph()
                    
                    doc.add_paragraph(f"✅ Найдено сообщений в Telegram: {telegram_count}")
                    total_results += telegram_count
                    
                except Exception as e:
                    doc.add_paragraph(f"❌ Ошибка при глобальном поиске: {str(e)}")
            
            await client.disconnect()
            doc.add_page_break()
        
        # Поиск по каналам из базы
        if not stop_flags.get(user_id, False) and MASTER_SESSION:
            await bot.send_message(user_id, "📱 Ищу по каналам из базы...")
            doc.add_heading('РЕЗУЛЬТАТЫ ИЗ КАНАЛОВ БАЗЫ', level=1)
            
            channels = load_channels()
            if channels:
                client = TelegramClient(StringSession(MASTER_SESSION), API_ID, API_HASH)
                await client.connect()
                
                if await client.is_user_authorized():
                    start_time = datetime.now().astimezone() - timedelta(hours=period_hours)
                    channels_count = 0
                    
                    for channel in channels[:5]:  # Ограничиваем до 5 каналов
                        if stop_flags.get(user_id, False):
                            break
                        
                        try:
                            channel_url = normalize_channel_url(channel['url'])
                            entity = await client.get_entity(channel_url)
                            
                            channel_posts = 0
                            async for message in client.iter_messages(entity, limit=10, offset_date=datetime.now()):
                                if stop_flags.get(user_id, False):
                                    break
                                
                                if message.date and message.date >= start_time and message.text and keywords.lower() in message.text.lower():
                                    if channel_posts == 0:
                                        doc.add_heading(f"Канал: {channel['name']}", level=2)
                                        doc.add_paragraph(f"Ссылка: {channel_url}")
                                    
                                    channel_posts += 1
                                    
                                    p = doc.add_paragraph()
                                    display_date = format_datetime_utc10(message.date)
                                    p.add_run(f"📅 {display_date}\n").bold = True
                                    
                                    text = message.text
                                    if len(text) > 1000:
                                        text = text[:1000] + "..."
                                    p.add_run(text)
                                    
                                    if message.id:
                                        channel_username = channel_url.split('/')[-1]
                                        link_url = f"https://t.me/{channel_username}/{message.id}"
                                        link_paragraph = doc.add_paragraph()
                                        add_hyperlink(link_paragraph, "🔗 Ссылка на пост", link_url)
                                    
                                    doc.add_paragraph()
                            
                            if channel_posts > 0:
                                doc.add_paragraph(f"✅ Найдено постов в канале: {channel_posts}")
                                channels_count += 1
                                total_results += channel_posts
                            
                        except Exception as e:
                            continue
                    
                    doc.add_paragraph(f"📊 Всего каналов с результатами: {channels_count}")
                
                await client.disconnect()
        
        doc.add_heading('ИТОГОВАЯ СТАТИСТИКА', level=1)
        doc.add_paragraph(f"Всего найдено результатов: {total_results}")
        doc.add_paragraph(f"Ключевые слова: {keywords}")
        doc.add_paragraph(f"Период: {user_data[user_id].get('period_text', format_period(period_hours))}")
        doc.add_paragraph(f"Формат отчета: {'с картинками' if save_images else 'только текст'}")
        
        if total_results == 0:
            await bot.send_message(
                user_id, 
                f"📭 Комбинированный поиск завершен\n\n"
                f"Не найдено результатов для запроса: {keywords}",
                reply_markup=get_main_keyboard(user_id)
            )
            
            if user_id in user_data:
                del user_data[user_id]
            
            return
        
        output_file = f"combined_report_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
        doc.save(output_file)
        
        with open(output_file, 'rb') as f:
            caption = f"✅ Комбинированный поиск завершен!\n\n"
            caption += f"📊 Всего найдено результатов: {total_results}\n"
            caption += f"🔍 Ключевые слова: {keywords}\n"
            caption += f"⏱ Период: {user_data[user_id].get('period_text', format_period(period_hours))}\n"
            caption += f"🖼️ Формат: {'с картинками' if save_images else 'только текст'}\n"
            
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
    print(f"🌏 Часовой пояс: UTC+10")
    print(f"⏱ Максимальный период: {MAX_PERIOD_DAYS} дней")
    print(f"🔍 Типы поиска: Каналы, Глобальный Telegram, Интернет, Комбинированный")
    
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
