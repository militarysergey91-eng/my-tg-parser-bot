import asyncio
import logging
from datetime import datetime, timedelta, timezone
import os
import json
import re
import time
import sys
import signal
import random

from PIL import Image
import pandas as pd
import openpyxl
import aiohttp
from bs4 import BeautifulSoup
import requests
from urllib.parse import quote_plus

# Для перевода
from googletrans import Translator, LANGUAGES
import langdetect

from aiogram import Bot, Dispatcher, types
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
API_TOKEN = '8029293386:AAG-Hih0eVun77YYcY8zf6PyDjQERmQCx9w'
API_ID = 38892524
API_HASH = 'd71ef1a657ab20d2a47a52130626c939'
ADMIN_ID = 5224743551

CHANNELS_FILE = 'saved_channels.json'
SESSIONS_DIR = 'telegram_sessions'
IMAGES_DIR = 'temp_images'
UPLOADS_DIR = 'uploads'

UTC_PLUS_10 = timezone(timedelta(hours=10))
MAX_PERIOD_DAYS = 30
MAX_PERIOD_HOURS = MAX_PERIOD_DAYS * 24

os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

# ========== ИНИЦИАЛИЗАЦИЯ ==========
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
logging.basicConfig(level=logging.INFO)

user_data = {}
auth_data = {}
stop_flags = {}
MASTER_SESSION = None

# ========== ПЕРЕВОД НА РУССКИЙ (ВСЕГДА) ==========
class TranslatorManager:
    def __init__(self):
        self.translator = Translator()
        
    def detect_language(self, text):
        try:
            if not text or len(text) < 10:
                return 'unknown'
            return langdetect.detect(text)
        except:
            return 'unknown'
    
    def get_language_name(self, code):
        return LANGUAGES.get(code, code)
    
    def translate_to_russian(self, text):
        """Переводит текст на русский язык (синхронно)"""
        try:
            if not text or len(text) < 10:
                return {'translated': text, 'was_translated': False, 'src_lang': 'unknown'}
            
            # Определяем язык
            src = self.detect_language(text)
            
            # Если текст уже на русском или не удалось определить, не переводим
            if src == 'ru' or src == 'unknown':
                return {'translated': text, 'was_translated': False, 'src_lang': src}
            
            # Переводим (без await!)
            result = self.translator.translate(text[:5000], dest='ru', src=src)
            
            if result and result.text:
                return {
                    'translated': result.text,
                    'src_lang': src,
                    'src_name': self.get_language_name(src),
                    'was_translated': True
                }
            else:
                return {'translated': text, 'was_translated': False, 'src_lang': src}
                
        except Exception as e:
            print(f"Ошибка перевода: {e}")
            return {'translated': text, 'was_translated': False, 'src_lang': 'unknown'}

translator = TranslatorManager()

# ========== РАБОТА С ФАЙЛАМИ ==========
def load_channels():
    if os.path.exists(CHANNELS_FILE):
        with open(CHANNELS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_channels(channels):
    with open(CHANNELS_FILE, 'w', encoding='utf-8') as f:
        json.dump(channels, f, ensure_ascii=False, indent=2)

def normalize_channel_url(url):
    url = url.replace('@', '')
    if 't.me/' in url:
        username = url.split('t.me/')[-1].strip('/').split('/')[0]
        return f"https://t.me/{username}"
    if '/' not in url and not url.startswith('http'):
        return f"https://t.me/{url}"
    return url

def extract_channel_name(url):
    url = normalize_channel_url(url)
    match = re.search(r't\.me/([a-zA-Z0-9_]+)', url)
    return match.group(1) if match else url.split('/')[-1]

# ========== ФУНКЦИИ ИМПОРТА/ЭКСПОРТА ==========
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
        
        # Проходим по всем строкам
        for index, row in df.iterrows():
            # Ищем столбцы с названием и ссылкой
            name_col = None
            url_col = None
            
            # Автоматически определяем столбцы
            for col in df.columns:
                col_lower = str(col).lower()
                if 'название' in col_lower or 'name' in col_lower or 'канал' in col_lower:
                    name_col = col
                if 'ссылка' in col_lower or 'url' in col_lower or 'link' in col_lower:
                    url_col = col
            
            # Если нашли нужные столбцы
            if name_col and url_col:
                name = str(row[name_col]).strip()
                url = str(row[url_col]).strip()
                
                # Проверяем ссылку
                if url and ('t.me/' in url or '@' in url):
                    url = normalize_channel_url(url)
                    
                    # Проверяем дубликаты
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
        return {'success': False, 'error': str(e)}

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
            
            # Проверяем ссылку
            if url and ('t.me/' in url or '@' in url):
                url = normalize_channel_url(url)
                
                # Проверяем дубликаты
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
        return {'success': False, 'error': str(e)}

def save_session_string(session_string):
    global MASTER_SESSION
    try:
        with open(os.path.join(SESSIONS_DIR, 'master_session.txt'), 'w', encoding='utf-8') as f:
            f.write(session_string)
        MASTER_SESSION = session_string
        return True
    except:
        return False

def load_master_session():
    global MASTER_SESSION
    try:
        path = os.path.join(SESSIONS_DIR, 'master_session.txt')
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                MASTER_SESSION = f.read().strip()
                return MASTER_SESSION
    except:
        pass
    return None

def remove_master_session():
    global MASTER_SESSION
    try:
        path = os.path.join(SESSIONS_DIR, 'master_session.txt')
        if os.path.exists(path):
            os.remove(path)
        MASTER_SESSION = None
    except:
        pass

def cleanup_temp_files(user_id):
    try:
        for f in os.listdir(IMAGES_DIR):
            if f.startswith(f"img_{user_id}_"):
                os.remove(os.path.join(IMAGES_DIR, f))
    except:
        pass

async def download_media(message, user_id, client):
    try:
        if message.media:
            timestamp = datetime.now().timestamp()
            filename = f"img_{user_id}_{timestamp}.jpg"
            path = os.path.join(IMAGES_DIR, filename)
            return await message.download_media(file=path)
    except:
        pass
    return None

def add_image_to_doc(doc, path):
    try:
        doc.add_picture(path, width=Cm(10))
        return True
    except:
        return False

def add_hyperlink(paragraph, text, url):
    part = paragraph.part
    hyperlink = OxmlElement('w:hyperlink')
    hyperlink.set(qn('r:id'), part.relate_to(url, 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink', is_external=True))
    r = OxmlElement('w:r')
    t = OxmlElement('w:t')
    t.text = text
    r.append(t)
    hyperlink.append(r)
    paragraph._p.append(hyperlink)

def is_admin(user_id):
    return user_id == ADMIN_ID

def parse_period(text):
    text = text.lower().strip()
    patterns = [
        (r'(\d+)\s*(час|часа|часов|ч)', 1),
        (r'(\d+)\s*(минут|минута|минуты|мин|м)', 1/60),
        (r'(\d+)\s*(день|дня|дней|д|сут|суток)', 24),
    ]
    for pattern, mult in patterns:
        m = re.search(pattern, text)
        if m:
            return int(m.group(1)) * mult
    m = re.search(r'(\d+)', text)
    return int(m.group(1)) if m else None

def format_period(hours):
    if hours < 1:
        return f"{int(hours*60)} мин"
    if hours < 24:
        return f"{int(hours)} ч"
    return f"{int(hours/24)} дн"

def format_datetime_utc10(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(UTC_PLUS_10).strftime('%d.%m.%Y %H:%M')

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(user_id):
    """Главная клавиатура - разная для админа и обычных пользователей"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    if is_admin(user_id):
        # Для админа - полный функционал
        kb.add(
            KeyboardButton("📋 Список каналов"),
            KeyboardButton("➕ Добавить канал"),
            KeyboardButton("📥 Импорт каналов"),
            KeyboardButton("📤 Экспорт каналов"),
            KeyboardButton("🔍 Поиск"),
            KeyboardButton("❓ Помощь"),
            KeyboardButton("⏹️ Стоп"),
            KeyboardButton("🔄 Собрать всё"),
            KeyboardButton("🚪 Выйти")
        )
    else:
        # Для обычных пользователей - только просмотр и поиск
        kb.add(
            KeyboardButton("📋 Список каналов"),
            KeyboardButton("🔍 Поиск"),
            KeyboardButton("❓ Помощь"),
            KeyboardButton("⏹️ Стоп")
        )
    return kb

def get_channels_menu_keyboard(user_id):
    """Клавиатура для меню каналов"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    if is_admin(user_id):
        # Для админа - полное управление
        kb.add(
            KeyboardButton("➕ Добавить канал"),
            KeyboardButton("📥 Импорт каналов"),
            KeyboardButton("📤 Экспорт каналов"),
            KeyboardButton("◀️ Назад в меню")
        )
    else:
        # Для обычных пользователей - только просмотр
        kb.add(
            KeyboardButton("◀️ Назад в меню")
        )
    return kb

def get_import_export_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        KeyboardButton("📥 Импорт из Excel"),
        KeyboardButton("📥 Импорт из TXT"),
        KeyboardButton("📤 Экспорт в Excel"),
        KeyboardButton("◀️ Назад")
    )
    return kb

def get_period_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        KeyboardButton("🕐 1 час"),
        KeyboardButton("🕒 3 часа"),
        KeyboardButton("🕖 7 часов"),
        KeyboardButton("📅 24 часа"),
        KeyboardButton("📆 3 дня"),
        KeyboardButton("📆 7 дней"),
        KeyboardButton("⌨️ Свой"),
        KeyboardButton("◀️ Назад")
    )
    return kb

def get_custom_period_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    kb.add(KeyboardButton("◀️ Назад"))
    return kb

def get_image_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        KeyboardButton("🖼️ С картинками"),
        KeyboardButton("📝 Только текст"),
        KeyboardButton("◀️ Назад")
    )
    return kb

def get_auth_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    kb.add(KeyboardButton("❌ Отмена"), KeyboardButton("◀️ Назад в меню"))
    return kb

# ========== КОМАНДЫ ==========
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    user_id = message.from_user.id
    channels = load_channels()
    
    if is_admin(user_id):
        text = (
            "👋 Привет, Админ!\n\n"
            "🔍 Я бот для поиска по Telegram группам и каналам\n\n"
            "📌 В моей базе сейчас: {len(channels)} групп/каналов\n\n"
            "⚡️ Мои возможности:\n"
            "• Поиск по добавленным группам\n"
            "• Автоматический перевод на русский\n"
            "• Управление списком групп (добавление, удаление)\n"
            "• Импорт/экспорт списка групп из Excel/TXT\n\n"
            "👇 Используй кнопки внизу экрана"
        )
    else:
        text = (
            "👋 Привет!\n\n"
            "🔍 Я бот для поиска по Telegram группам и каналам\n\n"
            "📌 В моей базе сейчас: {len(channels)} групп/каналов\n\n"
            "⚡️ Мои возможности:\n"
            "• Просмотр списка доступных групп\n"
            "• Поиск по группам\n"
            "• Автоматический перевод на русский\n\n"
            "👇 Используй кнопки внизу экрана"
        )
    
    if not MASTER_SESSION and is_admin(user_id):
        text += "\n\n⚠️ Нажми 🔍 Поиск для авторизации"
    
    await message.reply(text, reply_markup=get_main_keyboard(user_id))

@dp.message_handler(lambda m: m.text == "📋 Список каналов")
async def show_channels(m: types.Message):
    user_id = m.from_user.id
    channels = load_channels()
    
    if not channels:
        await m.reply("📭 Нет добавленных групп/каналов", reply_markup=get_channels_menu_keyboard(user_id))
        return
    
    # Создаем клавиатуру со списком каналов
    kb = InlineKeyboardMarkup(row_width=1)
    
    for i, ch in enumerate(channels):
        # Кнопка с названием канала (открывает ссылку)
        btn = InlineKeyboardButton(f"📢 {ch['name']}", url=ch['url'])
        
        if is_admin(user_id):
            # Для админа добавляем кнопку удаления
            del_btn = InlineKeyboardButton("❌", callback_data=f"del_{i}")
            kb.row(btn, del_btn)
        else:
            # Для обычных пользователей только кнопка с ссылкой
            kb.add(btn)
    
    if is_admin(user_id):
        kb.add(InlineKeyboardButton("❌ Удалить все", callback_data="del_all"))
    
    kb.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    
    # Формируем текст сообщения
    text = "📋 Список групп и каналов:\n\n"
    for i, ch in enumerate(channels, 1):
        text += f"{i}. {ch['name']}\n"
    
    text += f"\nВсего групп/каналов: {len(channels)}"
    
    if not is_admin(user_id):
        text += "\n\nℹ️ Нажми на название чтобы перейти"
    
    await m.reply(text, reply_markup=kb)

@dp.message_handler(lambda m: m.text == "➕ Добавить канал")
async def add_channel_prompt(m: types.Message):
    user_id = m.from_user.id
    
    if not is_admin(user_id):
        await m.reply("❌ Только администратор может добавлять группы/каналы")
        return
    
    await m.reply("🔗 Отправь ссылку на группу или канал\nПример: @durov или https://t.me/durov")
    user_data[user_id] = {'state': 'waiting_channel'}

@dp.message_handler(lambda m: m.text == "📥 Импорт каналов")
async def import_menu(m: types.Message):
    user_id = m.from_user.id
    
    if not is_admin(user_id):
        await m.reply("❌ Только администратор может импортировать группы/каналы")
        return
    
    await m.reply("📥 Выберите способ импорта списка групп:", reply_markup=get_import_export_keyboard())
    user_data[user_id] = {'state': 'waiting_import_type'}

@dp.message_handler(lambda m: m.text == "📤 Экспорт каналов")
async def export_channels(m: types.Message):
    user_id = m.from_user.id
    
    if not is_admin(user_id):
        await m.reply("❌ Только администратор может экспортировать список групп")
        return
    
    status_msg = await m.reply("🔄 Создаю Excel файл со списком групп...")
    
    try:
        filepath = export_channels_to_excel()
        
        if filepath and os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                await bot.send_document(
                    user_id,
                    f,
                    caption="📊 Список групп и каналов в формате Excel"
                )
            
            os.remove(filepath)
            await status_msg.delete()
        else:
            await status_msg.edit_text("❌ Ошибка при создании Excel файла")
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка: {str(e)}")

@dp.message_handler(lambda m: m.text == "📥 Импорт из Excel")
async def import_excel_prompt(m: types.Message):
    user_id = m.from_user.id
    await m.reply(
        "📥 Отправьте Excel файл (.xlsx) со списком групп\n\n"
        "Формат: первый столбец - название, второй - ссылка",
        reply_markup=get_import_export_keyboard()
    )
    user_data[user_id] = {'state': 'waiting_excel_file'}

@dp.message_handler(lambda m: m.text == "📥 Импорт из TXT")
async def import_txt_prompt(m: types.Message):
    user_id = m.from_user.id
    await m.reply(
        "📥 Отправьте текстовый файл (.txt) со списком групп\n\n"
        "Формат: Название, ссылка (каждая строка - одна группа)",
        reply_markup=get_import_export_keyboard()
    )
    user_data[user_id] = {'state': 'waiting_txt_file'}

@dp.message_handler(content_types=ContentType.DOCUMENT)
async def handle_document(message: types.Message):
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.reply("❌ Только администратор может загружать файлы")
        return
    
    if user_id not in user_data:
        await message.reply("Сначала выберите действие в меню", reply_markup=get_main_keyboard(user_id))
        return
    
    state = user_data[user_id].get('state')
    
    if state not in ['waiting_excel_file', 'waiting_txt_file']:
        return
    
    document = message.document
    file_name = document.file_name
    file_ext = os.path.splitext(file_name)[1].lower()
    
    # Проверяем расширение файла
    if state == 'waiting_excel_file' and file_ext not in ['.xlsx', '.xls']:
        await message.reply("❌ Пожалуйста, отправьте Excel файл (.xlsx или .xls)", reply_markup=get_main_keyboard(user_id))
        return
    
    if state == 'waiting_txt_file' and file_ext != '.txt':
        await message.reply("❌ Пожалуйста, отправьте текстовый файл (.txt)", reply_markup=get_main_keyboard(user_id))
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
        if os.path.exists(file_path):
            os.remove(file_path)
        
        # Формируем ответ
        if result['success']:
            response = f"✅ Импорт завершен!\n\n"
            response += f"📊 Добавлено новых групп/каналов: {result['added']}\n"
            response += f"📈 Всего групп/каналов в базе: {result['total']}\n"
            
            if result['duplicates']:
                response += f"\n⚠️ Найдены дубликаты: {len(result['duplicates'])}"
            
            if result['invalid']:
                response += f"\n❌ Некорректные ссылки: {len(result['invalid'])}"
        else:
            response = f"❌ Ошибка импорта: {result['error']}"
        
        await status_msg.delete()
        await message.reply(response, reply_markup=get_main_keyboard(user_id))
        
        # Очищаем состояние
        del user_data[user_id]
        
    except Exception as e:
        await status_msg.delete()
        await message.reply(f"❌ Ошибка при обработке файла: {str(e)}")
        if os.path.exists(file_path):
            os.remove(file_path)

@dp.message_handler(lambda m: m.text == "🔍 Поиск")
async def search_menu(m: types.Message):
    user_id = m.from_user.id

    global MASTER_SESSION 
    
    if not MASTER_SESSION:
        MASTER_SESSION = load_master_session()
    
    if not MASTER_SESSION and is_admin(user_id):
        await m.reply("🔄 Нужна авторизация", reply_markup=get_auth_keyboard())
        await start_auth(user_id, m)
        return
    
    # Проверяем есть ли каналы
    channels = load_channels()
    if not channels:
        await m.reply("❌ Нет доступных групп/каналов для поиска", reply_markup=get_main_keyboard(user_id))
        return
    
    await m.reply("🔍 Введи ключевые слова для поиска по группам:", reply_markup=get_main_keyboard(user_id))
    user_data[user_id] = {'state': 'waiting_keywords', 'channels': channels}

@dp.message_handler(lambda m: m.text == "🔄 Собрать всё")
async def collect_all(m: types.Message):
    user_id = m.from_user.id
    if not is_admin(user_id):
        await m.reply("❌ Только для админа")
        return
    
    channels = load_channels()
    if not channels:
        await m.reply("❌ Нет групп/каналов")
        return
    
    user_data[user_id] = {'state': 'waiting_period', 'keywords': 'все', 'channels': channels}
    await m.reply("⏱ За какой период?", reply_markup=get_period_keyboard())

@dp.message_handler(lambda m: m.text == "❓ Помощь")
async def help_cmd(m: types.Message):
    user_id = m.from_user.id
    
    if is_admin(user_id):
        text = (
            "❓ Помощь (Админ)\n\n"
            "📋 Список каналов - просмотр и управление списком групп\n"
            "➕ Добавить канал - добавить новую группу/канал\n"
            "📥 Импорт каналов - загрузить список групп из Excel/TXT\n"
            "📤 Экспорт каналов - сохранить список групп в Excel\n"
            "🔍 Поиск - начать поиск по ключевым словам в группах\n"
            "🔄 Собрать всё - все посты без фильтра\n"
            "⏹️ Стоп - остановить поиск\n"
            "🚪 Выйти - выйти из аккаунта\n\n"
            "🌍 Перевод: автоматический на русский\n"
            "⌨️ Свой период: 30 минут, 2 часа, 5 дней"
        )
    else:
        text = (
            "❓ Помощь\n\n"
            "📋 Список каналов - посмотреть доступные группы/каналы\n"
            "🔍 Поиск - начать поиск по ключевым словам в группах\n"
            "⏹️ Стоп - остановить поиск\n\n"
            "🌍 Перевод: автоматический на русский\n"
            "⌨️ Свой период: 30 минут, 2 часа, 5 дней"
        )
    
    await m.reply(text)

@dp.message_handler(lambda m: m.text == "⏹️ Стоп")
async def stop_cmd(m: types.Message):
    user_id = m.from_user.id
    if user_id in stop_flags:
        stop_flags[user_id] = True
        await m.reply("⏹️ Останавливаю...")

@dp.message_handler(lambda m: m.text == "🚪 Выйти")
async def logout(m: types.Message):
    user_id = m.from_user.id
    if is_admin(user_id):
        remove_master_session()
        if user_id in auth_data:
            del auth_data[user_id]
        await m.reply("🚪 Вышел", reply_markup=get_main_keyboard(user_id))

@dp.message_handler(lambda m: m.text == "❌ Отмена")
async def cancel(m: types.Message):
    user_id = m.from_user.id
    if user_id in auth_data:
        if 'client' in auth_data[user_id]:
            await auth_data[user_id]['client'].disconnect()
        del auth_data[user_id]
    await m.reply("❌ Отменено", reply_markup=get_main_keyboard(user_id))

@dp.message_handler(lambda m: m.text == "◀️ Назад")
async def back(m: types.Message):
    user_id = m.from_user.id
    
    if user_id in user_data:
        state = user_data[user_id].get('state')
        
        if state == 'waiting_search_type':
            del user_data[user_id]
            await m.reply("Главное меню", reply_markup=get_main_keyboard(user_id))
        elif state == 'waiting_import_type':
            del user_data[user_id]
            await m.reply("Главное меню", reply_markup=get_main_keyboard(user_id))
        elif state in ['waiting_keywords', 'waiting_period', 'waiting_excel_file', 'waiting_txt_file']:
            del user_data[user_id]
            await m.reply("Главное меню", reply_markup=get_main_keyboard(user_id))
        else:
            del user_data[user_id]
            await m.reply("Главное меню", reply_markup=get_main_keyboard(user_id))
    else:
        await m.reply("Главное меню", reply_markup=get_main_keyboard(user_id))

@dp.message_handler(lambda m: m.text == "◀️ Назад в меню")
async def back_to_menu(m: types.Message):
    user_id = m.from_user.id
    
    if user_id in user_data:
        del user_data[user_id]
    
    await m.reply("Главное меню", reply_markup=get_main_keyboard(user_id))

# ========== АВТОРИЗАЦИЯ ==========
async def start_auth(user_id, message):
    try:
        if user_id in auth_data and 'client' in auth_data[user_id]:
            await auth_data[user_id]['client'].disconnect()
        
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        
        auth_data[user_id] = {'client': client, 'state': 'waiting_phone'}
        await message.reply("📱 Введи номер телефона (например: +79123456789):", reply_markup=get_auth_keyboard())
    except Exception as e:
        await message.reply(f"❌ Ошибка: {e}")

@dp.message_handler(lambda m: m.from_user.id in auth_data and auth_data[m.from_user.id]['state'] == 'waiting_phone')
async def process_phone(m: types.Message):
    user_id = m.from_user.id
    phone = m.text.strip()
    
    if phone == "❌ Отмена":
        await cancel(m)
        return
    
    phone = re.sub(r'[^\d+]', '', phone)
    
    try:
        client = auth_data[user_id]['client']
        result = await client.send_code_request(phone)
        
        auth_data[user_id]['phone'] = phone
        auth_data[user_id]['hash'] = result.phone_code_hash
        auth_data[user_id]['state'] = 'waiting_code'
        
        await m.reply("🔐 Введи код из Telegram:", reply_markup=get_auth_keyboard())
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        await client.disconnect()
        del auth_data[user_id]

@dp.message_handler(lambda m: m.from_user.id in auth_data and auth_data[m.from_user.id]['state'] == 'waiting_code')
async def process_code(m: types.Message):
    user_id = m.from_user.id
    code = m.text.strip()
    
    if code == "❌ Отмена":
        await cancel(m)
        return
    
    code = re.sub(r'\D', '', code)
    
    try:
        client = auth_data[user_id]['client']
        await client.sign_in(auth_data[user_id]['phone'], code)
        
        session = client.session.save()
        save_session_string(session)
        
        me = await client.get_me()
        await m.reply(f"✅ Авторизация успешна!\nАккаунт: {me.first_name}", reply_markup=get_main_keyboard(user_id))
        
        del auth_data[user_id]
        
    except SessionPasswordNeededError:
        auth_data[user_id]['state'] = 'waiting_password'
        await m.reply("🔐 Требуется пароль двухфакторки. Введи пароль:")
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")

@dp.message_handler(lambda m: m.from_user.id in auth_data and auth_data[m.from_user.id]['state'] == 'waiting_password')
async def process_password(m: types.Message):
    user_id = m.from_user.id
    password = m.text.strip()
    
    try:
        client = auth_data[user_id]['client']
        await client.sign_in(password=password)
        
        session = client.session.save()
        save_session_string(session)
        
        me = await client.get_me()
        await m.reply(f"✅ Авторизация успешна!\nАккаунт: {me.first_name}", reply_markup=get_main_keyboard(user_id))
        
        del auth_data[user_id]
    except Exception as e:
        await m.reply(f"❌ Неверный пароль. Попробуй еще раз:")

# ========== ОБРАБОТКА ТЕКСТА ==========
@dp.message_handler(lambda m: m.from_user.id in user_data and user_data[m.from_user.id].get('state') == 'waiting_channel')
async def process_channel(m: types.Message):
    user_id = m.from_user.id
    link = m.text.strip()
    
    link = normalize_channel_url(link)
    name = extract_channel_name(link)
    
    channels = load_channels()
    
    for ch in channels:
        if ch['url'] == link:
            await m.reply("❌ Такая группа/канал уже есть в базе")
            del user_data[user_id]
            return
    
    channels.append({'name': name, 'url': link})
    save_channels(channels)
    
    del user_data[user_id]
    await m.reply(f"✅ Группа/канал добавлен!\n{name}\n{link}\nВсего в базе: {len(channels)}", reply_markup=get_main_keyboard(user_id))

@dp.message_handler(lambda m: m.from_user.id in user_data and user_data[m.from_user.id].get('state') == 'waiting_keywords')
async def process_keywords(m: types.Message):
    user_id = m.from_user.id
    keywords = m.text.strip()
    
    user_data[user_id]['keywords'] = keywords
    user_data[user_id]['state'] = 'waiting_period'
    
    await m.reply("⏱ За какой период ищем в группах?", reply_markup=get_period_keyboard())

@dp.message_handler(lambda m: m.from_user.id in user_data and user_data[m.from_user.id].get('state') == 'waiting_period')
async def process_period(m: types.Message):
    user_id = m.from_user.id
    text = m.text
    
    hours = 0
    
    if text == "🕐 1 час":
        hours = 1
    elif text == "🕒 3 часа":
        hours = 3
    elif text == "🕖 7 часов":
        hours = 7
    elif text == "📅 24 часа":
        hours = 24
    elif text == "📆 3 дня":
        hours = 72
    elif text == "📆 7 дней":
        hours = 168
    elif text == "⌨️ Свой":
        await m.reply("⌨️ Введи период (например: 30 минут, 2 часа, 5 дней):", reply_markup=get_custom_period_keyboard())
        user_data[user_id]['state'] = 'waiting_custom_period'
        return
    elif text == "◀️ Назад":
        await back(m)
        return
    else:
        await m.reply("❌ Выбери из кнопок")
        return
    
    user_data[user_id]['period_hours'] = hours
    user_data[user_id]['period_text'] = text
    
    # Переходим сразу к выбору формата
    await m.reply("🖼️ Выбери формат отчета:", reply_markup=get_image_keyboard())
    user_data[user_id]['state'] = 'waiting_image'

@dp.message_handler(lambda m: m.from_user.id in user_data and user_data[m.from_user.id].get('state') == 'waiting_custom_period')
async def process_custom_period(m: types.Message):
    user_id = m.from_user.id
    text = m.text
    
    if text == "◀️ Назад":
        await m.reply("⏱ За какой период ищем в группах?", reply_markup=get_period_keyboard())
        user_data[user_id]['state'] = 'waiting_period'
        return
    
    hours = parse_period(text)
    
    if not hours or hours <= 0:
        await m.reply("❌ Не понимаю. Попробуй еще раз (30 минут, 2 часа, 5 дней):")
        return
    
    if hours > MAX_PERIOD_HOURS:
        await m.reply(f"❌ Максимум {MAX_PERIOD_DAYS} дней. Введи меньше:")
        return
    
    user_data[user_id]['period_hours'] = hours
    user_data[user_id]['period_text'] = text
    
    # Переходим сразу к выбору формата
    await m.reply("🖼️ Выбери формат отчета:", reply_markup=get_image_keyboard())
    user_data[user_id]['state'] = 'waiting_image'

@dp.message_handler(lambda m: m.from_user.id in user_data and user_data[m.from_user.id].get('state') == 'waiting_image')
async def process_image(m: types.Message):
    user_id = m.from_user.id
    text = m.text
    
    if text == "◀️ Назад":
        await m.reply("⏱ За какой период ищем в группах?", reply_markup=get_period_keyboard())
        user_data[user_id]['state'] = 'waiting_period'
        return
    
    save_images = (text == "🖼️ С картинками")
    user_data[user_id]['save_images'] = save_images
    
    await m.reply(f"🔍 Начинаю поиск по группам...\nЭто может занять время", reply_markup=get_main_keyboard(user_id))
    
    await collect_from_channels(user_id)

# ========== СБОР ДАННЫХ ==========
async def collect_from_channels(user_id):
    client = None
    try:
        stop_flags[user_id] = False
        
        data = user_data[user_id]
        keywords = data['keywords']
        channels = data['channels']
        hours = data['period_hours']
        save_images = data['save_images']
        
        await bot.send_message(user_id, "🔄 Подключаюсь к Telegram...")
        
        if not MASTER_SESSION:
            await bot.send_message(user_id, "❌ Нет подключения к Telegram")
            return
        
        client = TelegramClient(StringSession(MASTER_SESSION), API_ID, API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            await bot.send_message(user_id, "❌ Сессия не активна")
            return
        
        doc = Document()
        doc.add_heading('Поиск по Telegram группам и каналам', 0).alignment = 1
        doc.add_paragraph(f"Дата: {datetime.now(UTC_PLUS_10).strftime('%d.%m.%Y %H:%M')} (UTC+10)")
        doc.add_paragraph(f"Ключевые слова: {keywords}")
        doc.add_paragraph(f"Период: {data['period_text']}")
        doc.add_paragraph("Перевод: на русский язык")
        doc.add_paragraph()
        
        total = 0
        processed = 0
        channels_with = 0
        
        now = datetime.now().astimezone()
        start = now - timedelta(hours=hours)
        
        for channel in channels:
            if stop_flags.get(user_id):
                break
            
            processed += 1
            await bot.send_message(user_id, f"📱 {processed}/{len(channels)} {channel['name']}")
            
            try:
                url = normalize_channel_url(channel['url'])
                entity = await client.get_entity(url)
                
                posts = []
                count = 0
                
                async for msg in client.iter_messages(entity, offset_date=now):
                    if count % 10 == 0 and stop_flags.get(user_id):
                        break
                    
                    if msg.date:
                        d = msg.date
                        if d.tzinfo is None:
                            d = d.replace(tzinfo=timezone.utc)
                        if d < start:
                            break
                    
                    if keywords.lower() == 'все' or (msg.text and keywords.lower() in msg.text.lower()):
                        posts.append(msg)
                        count += 1
                        total += 1
                
                if posts:
                    channels_with += 1
                    doc.add_heading(f"Группа/канал: {channel['name']}", level=1)
                    doc.add_paragraph(f"Ссылка: {url}")
                    doc.add_paragraph()
                    
                    for msg in posts:
                        p = doc.add_paragraph()
                        p.add_run(f"📅 {format_datetime_utc10(msg.date)}\n").bold = True
                        
                        if msg.text:
                            text = msg.text[:3000]
                            # Всегда переводим на русский
                            trans = translator.translate_to_russian(text)
                            if trans['was_translated']:
                                p.add_run(f"[Переведено с {trans['src_name']}]\n\n{trans['translated']}")
                            else:
                                p.add_run(text)
                        
                        if save_images and msg.media:
                            path = await download_media(msg, user_id, client)
                            if path:
                                doc.add_paragraph()
                                add_image_to_doc(doc, path)
                        
                        if msg.id:
                            username = url.split('/')[-1]
                            link = f"https://t.me/{username}/{msg.id}"
                            p = doc.add_paragraph()
                            add_hyperlink(p, "🔗 Ссылка на сообщение", link)
                        
                        doc.add_paragraph()
                    
                    doc.add_paragraph(f"✅ Найдено сообщений: {count}")
                    doc.add_page_break()
                
            except FloodWaitError as e:
                wait = e.seconds
                await bot.send_message(user_id, f"⚠️ Лимит запросов. Жду {wait} сек...")
                await asyncio.sleep(wait)
                continue
            except Exception as e:
                await bot.send_message(user_id, f"⚠️ Ошибка доступа: {str(e)[:100]}")
                doc.add_paragraph(f"❌ Ошибка доступа к группе/каналу: {channel['name']}")
                doc.add_page_break()
        
        doc.add_heading('Итоговая статистика', level=1)
        doc.add_paragraph(f"Обработано групп/каналов: {processed}/{len(channels)}")
        doc.add_paragraph(f"Групп/каналов с результатами: {channels_with}")
        doc.add_paragraph(f"Всего найдено сообщений: {total}")
        
        if total == 0:
            await bot.send_message(user_id, "📭 Ничего не найдено")
            return
        
        filename = f"search_report_{user_id}_{int(time.time())}.docx"
        doc.save(filename)
        
        with open(filename, 'rb') as f:
            await bot.send_document(user_id, f, caption=f"✅ Поиск завершен! Найдено сообщений: {total}")
        
        os.remove(filename)
        cleanup_temp_files(user_id)
        
    except Exception as e:
        await bot.send_message(user_id, f"❌ Ошибка: {e}")
    finally:
        if client:
            await client.disconnect()
        if user_id in user_data:
            del user_data[user_id]
        if user_id in stop_flags:
            del stop_flags[user_id]

# ========== КОЛБЭКИ ==========
@dp.callback_query_handler(lambda c: c.data.startswith('del_'))
async def delete_callback(call):
    await bot.answer_callback_query(call.id)
    
    user_id = call.from_user.id
    if not is_admin(user_id):
        await bot.send_message(user_id, "❌ Только для админа")
        return
    
    data = call.data
    channels = load_channels()
    
    if data == "del_all":
        save_channels([])
        await bot.send_message(user_id, "🗑 Все группы/каналы удалены", reply_markup=get_main_keyboard(user_id))
        return
    
    idx = int(data.split('_')[1])
    if idx < len(channels):
        deleted = channels.pop(idx)
        save_channels(channels)
        await bot.send_message(user_id, f"🗑 Удалена группа/канал: {deleted['name']}", reply_markup=get_main_keyboard(user_id))

@dp.callback_query_handler(lambda c: c.data == 'back_to_main')
async def back_callback(call):
    await bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    if user_id in user_data:
        del user_data[user_id]
    await bot.send_message(user_id, "Главное меню", reply_markup=get_main_keyboard(user_id))

# ========== НЕИЗВЕСТНЫЕ КОМАНДЫ ==========
@dp.message_handler()
async def unknown(m: types.Message):
    user_id = m.from_user.id
    
    if is_admin(user_id) and user_id in auth_data:
        state = auth_data[user_id].get('state')
        if state == 'waiting_phone':
            await process_phone(m)
        elif state == 'waiting_code':
            await process_code(m)
        elif state == 'waiting_password':
            await process_password(m)
        return
    
    if m.text and ('t.me/' in m.text or '@' in m.text):
        if is_admin(user_id):
            user_data[user_id] = {'state': 'waiting_channel'}
            await process_channel(m)
        else:
            await m.reply("❌ Только администратор может добавлять группы/каналы")
    else:
        await m.reply("Используй кнопки внизу экрана 👇", reply_markup=get_main_keyboard(user_id))

# ========== ЗАПУСК ==========
if __name__ == '__main__':
    MASTER_SESSION = load_master_session()
    channels = load_channels()
    
    print("=" * 50)
    print("🤖 Бот для поиска по группам запущен")
    print(f"👑 Админ ID: {ADMIN_ID}")
    print(f"📊 Групп/каналов в базе: {len(channels)}")
    print(f"🌍 Перевод: всегда на русский")
    print("=" * 50)
    
    executor.start_polling(dp, skip_updates=True)
