import asyncio
import logging
import sqlite3
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from datetime import timedelta
from config import TELEGRAM_BOT_TOKEN

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
/auth - Авторизоваться
/accounts - Список аккаунтів
/delete_account - видалити аккаунт
/status_account - статус аккаунта
    """