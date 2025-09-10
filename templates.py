import os
import logging
from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from database import Database

logger = logging.getLogger(__name__)

class TemplateStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_text = State()
    waiting_for_media = State()
    editing_template = State()

class TemplateManager:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.db = Database()
    
    def get_template_keyboard(self, templates: list) -> InlineKeyboardMarkup:
        """Створити клавіатуру з шаблонами"""
        keyboard = []
        
        for template in templates:
            # Визначаємо іконку за типом
            icon = self._get_template_icon(template['message_type'])
            button_text = f"{icon} {template['name']}"
            
            keyboard.append([
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"select_template_{template['id']}"
                )
            ])
        
        # Додаємо кнопки управління
        keyboard.append([
            InlineKeyboardButton(text="➕ Додати шаблон", callback_data="add_template"),
            InlineKeyboardButton(text="✏️ Редагувати", callback_data="edit_templates")
        ])
        keyboard.append([
            InlineKeyboardButton(text="❌ Закрити", callback_data="close_templates")
        ])
        
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    def _get_template_icon(self, message_type: str) -> str:
        """Отримати іконку для типу повідомлення"""
        icons = {
            'text': '📝',
            'photo': '🖼️',
            'video': '🎥',
            'audio': '🎵',
            'voice': '🎤',
            'document': '📄',
            'sticker': '😀',
            'animation': '🎬'
        }
        return icons.get(message_type, '📄')
    
    async def save_template_from_message(self, message: types.Message, name: str) -> bool:
        """Зберегти шаблон з повідомлення"""
        try:
            message_type = 'text'
            text = None
            file_id = None
            file_path = None
            file_name = None
            file_size = None
            
            # Визначаємо тип повідомлення та збираємо дані
            if message.photo:
                message_type = 'photo'
                file_id = message.photo[-1].file_id
                file_name = f"photo_{file_id}.jpg"
                file_size = message.photo[-1].file_size
                text = message.caption
                
                # Завантажуємо фото
                file_path = await self._download_media(file_id, file_name)
                
            elif message.video:
                message_type = 'video'
                file_id = message.video.file_id
                file_name = message.video.file_name or f"video_{file_id}.mp4"
                file_size = message.video.file_size
                text = message.caption
                
                # Завантажуємо відео
                file_path = await self._download_media(file_id, file_name)
                
            elif message.audio:
                message_type = 'audio'
                file_id = message.audio.file_id
                file_name = message.audio.file_name or f"audio_{file_id}.mp3"
                file_size = message.audio.file_size
                text = message.caption
                
                # Завантажуємо аудіо
                file_path = await self._download_media(file_id, file_name)
                
            elif message.voice:
                message_type = 'voice'
                file_id = message.voice.file_id
                file_name = f"voice_{file_id}.ogg"
                file_size = message.voice.file_size
                
                # Завантажуємо голосове повідомлення
                file_path = await self._download_media(file_id, file_name)
                
            elif message.document:
                message_type = 'document'
                file_id = message.document.file_id
                file_name = message.document.file_name or f"document_{file_id}"
                file_size = message.document.file_size
                text = message.caption
                
                # Завантажуємо документ
                file_path = await self._download_media(file_id, file_name)
                
            elif message.sticker:
                message_type = 'sticker'
                file_id = message.sticker.file_id
                file_name = f"sticker_{file_id}.webp"
                file_size = message.sticker.file_size
                
                # Завантажуємо стікер
                file_path = await self._download_media(file_id, file_name)
                
            elif message.animation:
                message_type = 'animation'
                file_id = message.animation.file_id
                file_name = message.animation.file_name or f"animation_{file_id}.gif"
                file_size = message.animation.file_size
                text = message.caption
                
                # Завантажуємо анімацію
                file_path = await self._download_media(file_id, file_name)
                
            else:
                # Текстове повідомлення
                message_type = 'text'
                text = message.text
            
            # Зберігаємо шаблон в базу даних
            print(f"DEBUG: Зберігаємо шаблон в БД:")
            print(f"DEBUG: name: {name}")
            print(f"DEBUG: message_type: {message_type}")
            print(f"DEBUG: text: {text}")
            print(f"DEBUG: file_id: {file_id}")
            print(f"DEBUG: file_path: {file_path}")
            print(f"DEBUG: file_name: {file_name}")
            print(f"DEBUG: file_size: {file_size}")
            
            template_id = self.db.add_template(
                name=name,
                message_type=message_type,
                text=text,
                file_id=file_id,
                file_path=file_path,
                file_name=file_name,
                file_size=file_size
            )
            
            if template_id:
                logger.info(f"✅ Шаблон '{name}' збережено з ID: {template_id}")
                return True
            else:
                logger.error(f"❌ Помилка при збереженні шаблону '{name}'")
                return False
                
        except Exception as e:
            logger.error(f"❌ Помилка при збереженні шаблону: {e}")
            return False
    
    async def _download_media(self, file_id: str, file_name: str) -> str:
        """Завантажити медіа файл"""
        try:
            # Створюємо папку media_files якщо її немає
            os.makedirs("media_files", exist_ok=True)
            
            # Шлях для збереження файлу
            file_path = os.path.join("media_files", file_name)
            
            # Завантажуємо файл
            file = await self.bot.get_file(file_id)
            await self.bot.download_file(file.file_path, file_path)
            
            logger.info(f"✅ Файл завантажено: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Помилка при завантаженні файлу {file_id}: {e}")
            return None
    
    def get_template_for_broadcast(self, template_id: int) -> dict:
        """Отримати шаблон для розсилки"""
        template = self.db.get_template(template_id)
        if not template:
            return None
        
        print(f"DEBUG: get_template_for_broadcast - template_id: {template_id}")
        print(f"DEBUG: template from DB: {template}")
        
        # Формуємо структуру для розсилки
        message_data = {
            'type': template['message_type'],
            'text': template['text']
        }
        
        # Додаємо інформацію про файл якщо є
        if template['file_path'] and os.path.exists(template['file_path']):
            message_data['file_path'] = template['file_path']
            message_data['file_id'] = template['file_id']
            print(f"DEBUG: File exists, added to message_data: {template['file_path']}")
        else:
            print(f"DEBUG: File path: {template['file_path']}, exists: {os.path.exists(template['file_path']) if template['file_path'] else False}")
        
        print(f"DEBUG: Final message_data: {message_data}")
        return message_data
    
    def get_edit_template_keyboard(self, template_id: int) -> InlineKeyboardMarkup:
        """Створити клавіатуру для редагування шаблону"""
        keyboard = [
            [
                InlineKeyboardButton(text="✏️ Редагувати назву", callback_data=f"edit_template_name_{template_id}"),
                InlineKeyboardButton(text="📝 Редагувати текст", callback_data=f"edit_template_text_{template_id}")
            ],
            [
                InlineKeyboardButton(text="🔄 Замінити медіа", callback_data=f"edit_template_media_{template_id}"),
                InlineKeyboardButton(text="🗑️ Видалити", callback_data=f"delete_template_{template_id}")
            ],
            [
                InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_templates")
            ]
        ]
        
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    def get_templates_list_keyboard(self) -> InlineKeyboardMarkup:
        """Створити клавіатуру зі списком шаблонів для редагування"""
        templates = self.db.get_templates()
        keyboard = []
        
        for template in templates:
            icon = self._get_template_icon(template['message_type'])
            button_text = f"{icon} {template['name']}"
            
            keyboard.append([
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=f"edit_template_{template['id']}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_templates")
        ])
        
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
