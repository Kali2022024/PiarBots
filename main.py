import asyncio
import logging
import os
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from config import TELEGRAM_BOT_TOKEN

# Імпортуємо наші модулі
from database import Database
from states import *
from utils import show_accounts_list
from registration import router as registration_router, init_registration_module
from groups import router as groups_router, init_groups_module
from broadcast import router as broadcast_router, init_broadcast_module
from mass_broadcast import router as mass_broadcast_router, init_mass_broadcast_module
from join_groups import router as join_groups_router, init_join_groups_module

# Завантажуємо змінні з .env файлу
load_dotenv()

# API дані з .env файлу
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ініціалізація бота та диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

# Ініціалізація бази даних
db = Database()

# Ініціалізація модулів
init_registration_module(API_ID, API_HASH, db)
init_groups_module(db)
init_broadcast_module(db)
init_mass_broadcast_module(db, bot)
init_join_groups_module(db)


# Імпортуємо RANDOM_STICKERS в database модуль
import database
RANDOM_STICKERS = database.RANDOM_STICKERS

@router.message(Command("start"))
async def cmd_start(message: Message):
    """Обробник команди /start"""
    welcome_text = f"""
🍽️ Ласкаво просимо до Piar Bot

🆔 Ваш ID: {message.from_user.id}

Я бот для реклами в telegram)

💡 Доступні команди:
/start - Почати роботу
/register_number - Привязать номер телефона
/accounts - Список аккаунтів
/delete_account - видалити аккаунт
/status_account - статус аккаунта
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Список аккаунтів", callback_data="accounts")],
        [InlineKeyboardButton(text="📱 Зареєструвати номер", callback_data="register_number")],
        [InlineKeyboardButton(text="🗑️ Видалити аккаунт", callback_data="delete_account")],
        [InlineKeyboardButton(text="📤 Запуск розсилання", callback_data="Message_in_all_chat_for_account")],
        [InlineKeyboardButton(text="👥 Додати список груп", callback_data="Groups_for_account")],
        [InlineKeyboardButton(text="➕ Додатись в групи", callback_data="join_groups")],
        [InlineKeyboardButton(text="🗑️ Видалити пакет груп", callback_data="delete_group_package")],
        [InlineKeyboardButton(text="📊 Статус розсилання", callback_data="broadcast_status")],
        [InlineKeyboardButton(text="📤 Масова розсилка", callback_data="Mass_broadcast")]
    ])
    
    await message.answer(welcome_text, reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "accounts")
async def accounts_callback(callback):
    """Обробка натискання кнопки списку аккаунтів"""
    await show_accounts_list(callback, db)
    await callback.answer()

@router.callback_query(lambda c: c.data == "register_number")
async def register_number_callback(callback, state):
    """Обробка натискання кнопки реєстрації номера"""
    from utils import start_registration_process
    await start_registration_process(callback, state)
    await callback.answer()

async def main():
    """Головна функція"""
    try:
        # Підключаємо роутери
        dp.include_router(router)
        dp.include_router(registration_router)
        dp.include_router(groups_router)
        dp.include_router(broadcast_router)
        dp.include_router(mass_broadcast_router)
        dp.include_router(join_groups_router)
        
        logger.info("🔄 Запуск бота...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Помилка при запуску бота: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(main())
