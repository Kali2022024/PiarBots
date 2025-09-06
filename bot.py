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
        """Ініціалізація бази даних та створення таблиць"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Таблиця аккаунтів
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
            
            # Таблиця пакетів груп
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS group_packages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    account_phone TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_phone) REFERENCES accounts (phone_number)
                )
            """)
            
            # Таблиця груп
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    group_username TEXT,
                    package_id INTEGER,
                    account_phone TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_phone) REFERENCES accounts (phone_number),
                    FOREIGN KEY (package_id) REFERENCES group_packages (id)
                )
            """)
            
            # Таблиця статусів розсилання
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS broadcast_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_phone TEXT NOT NULL,
                    message_text TEXT NOT NULL,
                    total_groups INTEGER DEFAULT 0,
                    sent_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP,
                    FOREIGN KEY (account_phone) REFERENCES accounts (phone_number)
                )
            """)
            
            # Міграція: додаємо колонку package_id до існуючої таблиці groups
            try:
                cursor.execute("ALTER TABLE groups ADD COLUMN package_id INTEGER")
                logger.info("✅ Додано колонку package_id до таблиці groups")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logger.info("ℹ️ Колонка package_id вже існує в таблиці groups")
                else:
                    logger.error(f"❌ Помилка при додаванні колонки package_id: {e}")
            
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
    
    # Методи для роботи з пакетами груп
    def create_group_package(self, name: str, account_phone: str) -> int:
        """Створити новий пакет груп"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO group_packages (name, account_phone)
                    VALUES (?, ?)
                """, (name, account_phone))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"❌ Помилка при створенні пакету груп: {e}")
            return 0
    
    def get_group_packages(self, account_phone: str) -> list:
        """Отримати всі пакети груп для аккаунта"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT gp.*, COUNT(g.id) as groups_count
                    FROM group_packages gp
                    LEFT JOIN groups g ON gp.id = g.package_id
                    WHERE gp.account_phone = ?
                    GROUP BY gp.id
                    ORDER BY gp.created_at DESC
                """, (account_phone,))
                packages = []
                for row in cursor.fetchall():
                    packages.append(dict(row))
                return packages
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні пакетів груп: {e}")
            return []
    
    def delete_group_package(self, package_id: int, account_phone: str) -> bool:
        """Видалити пакет груп та всі групи в ньому"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Спочатку видаляємо всі групи в пакеті
                cursor.execute("DELETE FROM groups WHERE package_id = ?", (package_id,))
                # Потім видаляємо сам пакет
                cursor.execute("DELETE FROM group_packages WHERE id = ? AND account_phone = ?", 
                             (package_id, account_phone))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Помилка при видаленні пакету груп: {e}")
            return False
    
    # Методи для роботи з групами
    def add_group(self, name: str, group_id: str, group_username: str, account_phone: str, package_id: int = None) -> bool:
        """Додати групу до бази даних"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO groups 
                    (name, group_id, group_username, account_phone, package_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (name, group_id, group_username, account_phone, package_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Помилка при додаванні групи: {e}")
            return False
    
    def get_groups_for_account(self, account_phone: str) -> list:
        """Отримати всі групи для конкретного аккаунта"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT g.*, gp.name as package_name
                    FROM groups g
                    LEFT JOIN group_packages gp ON g.package_id = gp.id
                    WHERE g.account_phone = ? 
                    ORDER BY gp.name ASC, g.name ASC
                """, (account_phone,))
                groups = []
                for row in cursor.fetchall():
                    groups.append(dict(row))
                return groups
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні груп: {e}")
            return []
    
    def get_groups_by_package(self, package_id: int) -> list:
        """Отримати всі групи в конкретному пакеті"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM groups WHERE package_id = ? 
                    ORDER BY name ASC
                """, (package_id,))
                groups = []
                for row in cursor.fetchall():
                    groups.append(dict(row))
                return groups
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні груп пакету: {e}")
            return []
    
    def delete_group(self, group_id: str, account_phone: str) -> bool:
        """Видалити групу з бази даних"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM groups WHERE group_id = ? AND account_phone = ?", 
                             (group_id, account_phone))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Помилка при видаленні групи: {e}")
            return False
    
    # Методи для роботи зі статусами розсилання
    def create_broadcast_status(self, account_phone: str, message_text: str, total_groups: int) -> int:
        """Створити новий статус розсилання"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO broadcast_status 
                    (account_phone, message_text, total_groups, status)
                    VALUES (?, ?, ?, 'running')
                """, (account_phone, message_text, total_groups))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"❌ Помилка при створенні статусу розсилання: {e}")
            return 0
    
    def update_broadcast_status(self, status_id: int, sent_count: int = None, 
                              failed_count: int = None, status: str = None) -> bool:
        """Оновити статус розсилання"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                updates = []
                params = []
                
                if sent_count is not None:
                    updates.append("sent_count = ?")
                    params.append(sent_count)
                
                if failed_count is not None:
                    updates.append("failed_count = ?")
                    params.append(failed_count)
                
                if status is not None:
                    updates.append("status = ?")
                    params.append(status)
                    if status in ['completed', 'failed']:
                        updates.append("finished_at = CURRENT_TIMESTAMP")
                
                if updates:
                    params.append(status_id)
                    query = f"UPDATE broadcast_status SET {', '.join(updates)} WHERE id = ?"
                    cursor.execute(query, params)
                    conn.commit()
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ Помилка при оновленні статусу розсилання: {e}")
            return False
    
    def get_broadcast_statuses(self) -> list:
        """Отримати всі статуси розсилання"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT bs.*, a.first_name, a.last_name, a.phone_number
                    FROM broadcast_status bs
                    JOIN accounts a ON bs.account_phone = a.phone_number
                    ORDER BY bs.started_at DESC
                """)
                statuses = []
                for row in cursor.fetchall():
                    statuses.append(dict(row))
                return statuses
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні статусів розсилання: {e}")
            return []


# Стани для FSM
class RegistrationStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_phone_to_delete = State()  # Додано стан для видалення аккаунта

class GroupStates(StatesGroup):
    waiting_for_group_name = State()
    waiting_for_group_id = State()
    waiting_for_group_list = State()
    waiting_for_package_name = State()
    waiting_for_single_group_id = State()
    waiting_for_account_selection = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_group_selection = State()
    waiting_for_account_selection = State()
    waiting_for_confirmation = State()

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

# ========== СПІЛЬНІ ФУНКЦІЇ ==========

async def show_accounts_list(message_or_callback):
    """Спільна функція для показу списку аккаунтів"""
    try:
        accounts = db.get_accounts()
        
        if not accounts:
            text = "📋 Список аккаунтів порожній.\n\nВикористайте команду /register_number для додавання нового аккаунта."
            if hasattr(message_or_callback, 'message'):
                await message_or_callback.message.answer(text)
                await message_or_callback.answer()
            else:
                await message_or_callback.answer(text)
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
                if hasattr(message_or_callback, 'message'):
                    await message_or_callback.message.answer(part, parse_mode='HTML')
                else:
                    await message_or_callback.answer(part, parse_mode='HTML')
        else:
            if hasattr(message_or_callback, 'message'):
                await message_or_callback.message.answer(accounts_text, parse_mode='HTML')
                await message_or_callback.answer()
            else:
                await message_or_callback.answer(accounts_text, parse_mode='HTML')
            
    except Exception as e:
        logger.error(f"❌ Помилка при отриманні списку аккаунтів: {e}")
        error_text = "❌ Помилка при отриманні списку аккаунтів. Спробуйте пізніше."
        if hasattr(message_or_callback, 'message'):
            await message_or_callback.message.answer(error_text)
            await message_or_callback.answer()
        else:
            await message_or_callback.answer(error_text)

async def start_registration_process(message_or_callback, state: FSMContext):
    """Спільна функція для початку процесу реєстрації"""
    text = "📱 Введіть номер телефону для реєстрації (формат +380XXXXXXXXX):"
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(text)
    else:
        await message_or_callback.answer(text)
    await state.set_state(RegistrationStates.waiting_for_phone)

async def send_broadcast_message(account_phone: str, message_text: str, groups: list, status_id: int):
    """Функція для розсилання повідомлень по групах"""
    try:
        # Отримуємо дані аккаунта
        accounts = db.get_accounts()
        account = None
        for acc in accounts:
            if acc['phone_number'] == account_phone:
                account = acc
                break
        
        if not account:
            logger.error(f"❌ Аккаунт {account_phone} не знайдено")
            db.update_broadcast_status(status_id, status='failed')
            return
        
        # Створюємо клієнт
        session_name = f"sessions/temp_{account_phone.replace('+', '').replace('-', '')}"
        client = TelegramClient(session_name, account['api_id'], account['api_hash'])
        
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.error(f"❌ Аккаунт {account_phone} не авторизований")
            db.update_broadcast_status(status_id, status='failed')
            await client.disconnect()
            return
        
        sent_count = 0
        failed_count = 0
        
        for group in groups:
            try:
                # Конвертуємо ID групи в int
                group_id = int(group['group_id'])
                
                # Відправляємо повідомлення
                await client.send_message(group_id, message_text)
                sent_count += 1
                logger.info(f"✅ Повідомлення відправлено в групу {group['name']} (ID: {group_id})")
                
                # Оновлюємо статус
                db.update_broadcast_status(status_id, sent_count=sent_count, failed_count=failed_count)
                
                # Затримка між повідомленнями
                await asyncio.sleep(2)
                
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Помилка при відправці в групу {group['name']} (ID: {group['group_id']}): {e}")
                db.update_broadcast_status(status_id, sent_count=sent_count, failed_count=failed_count)
        
        # Завершуємо розсилання
        db.update_broadcast_status(status_id, status='completed')
        logger.info(f"✅ Розсилання завершено. Відправлено: {sent_count}, Помилок: {failed_count}")
        
        await client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Помилка при розсиланні: {e}")
        db.update_broadcast_status(status_id, status='failed')

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
        [InlineKeyboardButton(text="👥 Додати список груп для аккаунта", callback_data="Groups_for_account")],
        [InlineKeyboardButton(text="📊 Статус розсилання", callback_data="broadcast_status")] 
    ])
    
    await message.answer(welcome_text, reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "accounts")
async def accounts_callback(callback: CallbackQuery):
    """Обробка натискання кнопки списку аккаунтів"""
    try:
        accounts = db.get_accounts()
        
        if not accounts:
            await callback.message.answer("📋 Список аккаунтів порожній.\n\nВикористайте команду /register_number для додавання нового аккаунта.")
            await callback.answer()
            return
        
        # Формуємо повідомлення зі списком аккаунтів
        accounts_text = "📋 <b>Список зареєстрованих аккаунтів:</b>\n\n"
        
        for i, account in enumerate(accounts, 1):
            status_emoji = "✅" if account['is_active'] else "❌"
            accounts_text += f"{i}. {status_emoji} <b>{account['phone_number']}</b>\n"
            accounts_text += f"   👤 {account['first_name'] or 'Не вказано'} {account['last_name'] or ''}\n"
            if account['username']:
                accounts_text += f"   🔗 @{account['username']}\n"
            accounts_text += f"   �� ID: {account['user_id']}\n"
            accounts_text += f"   �� Додано: {account['created_at']}\n\n"
        
        await callback.message.answer(accounts_text, parse_mode='HTML')
        await callback.answer()
        
    except Exception as e:
        logger.error(f"❌ Помилка при отриманні списку аккаунтів: {e}")
        await callback.message.answer("❌ Помилка при отриманні списку аккаунтів. Спробуйте пізніше.")
        await callback.answer()

@router.callback_query(lambda c: c.data == "register_number")
async def register_number_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки реєстрації"""
    await start_registration_process(callback, state)

@router.callback_query(lambda c: c.data == "Groups_for_account")
async def groups_for_account_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки додавання груп"""
    accounts = db.get_accounts()
    
    if not accounts:
        await callback.message.answer("❌ Немає зареєстрованих аккаунтів. Спочатку зареєструйте аккаунт.")
        await callback.answer()
        return
    
    # Створюємо клавіатуру з аккаунтами
    keyboard_buttons = []
    for account in accounts:
        button_text = f"📱 {account['phone_number']} ({account['first_name'] or 'Без імені'})"
        keyboard_buttons.append([InlineKeyboardButton(
            text=button_text, 
            callback_data=f"select_account_for_group_{account['phone_number']}"
        )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await callback.message.answer(
        "👥 Оберіть аккаунт для додавання груп:",
        reply_markup=keyboard
    )
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("select_account_for_group_"))
async def select_account_for_group_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору аккаунта для додавання груп"""
    account_phone = callback.data.replace("select_account_for_group_", "")
    
    # Зберігаємо вибраний аккаунт
    await state.update_data(selected_account=account_phone)
    
    # Показуємо поточні пакети груп для цього аккаунта
    packages = db.get_group_packages(account_phone)
    
    if packages:
        groups_text = f"📦 <b>Пакети груп для {account_phone}:</b>\n\n"
        for package in packages:
            groups_text += f"📦 <b>{package['name']}</b> ({package['groups_count']} груп)\n"
            groups_text += f"   📅 Створено: {package['created_at']}\n\n"
    else:
        groups_text = f"📦 <b>Пакети груп для {account_phone}:</b>\n\nСписок порожній."
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати групу", callback_data="add_new_group")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Groups_for_account")]
    ])
    
    await callback.message.answer(groups_text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()

@router.callback_query(lambda c: c.data == "add_new_group")
async def add_new_group_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки додавання нової групи"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Створити пакет груп", callback_data="create_group_package")],
        [InlineKeyboardButton(text="➕ Додати одну групу", callback_data="add_single_group")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Groups_for_account")]
    ])
    
    await callback.message.answer(
        "📝 Оберіть спосіб додавання груп:",
        reply_markup=keyboard
    )
    await callback.answer()

@router.callback_query(lambda c: c.data == "create_group_package")
async def create_group_package_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки створення пакету груп"""
    await callback.message.answer("📦 Введіть назву пакету груп:")
    await state.set_state(GroupStates.waiting_for_package_name)
    await callback.answer()

@router.message(GroupStates.waiting_for_package_name)
async def process_package_name(message: Message, state: FSMContext):
    """Обробка назви пакету груп"""
    package_name = message.text.strip()
    data = await state.get_data()
    account_phone = data['selected_account']
    
    # Створюємо пакет
    package_id = db.create_group_package(package_name, account_phone)
    
    if package_id:
        await state.update_data(package_id=package_id, package_name=package_name)
        await message.answer(
            f"✅ Пакет '{package_name}' створено!\n\n"
            f"📋 Тепер введіть список ID груп через кому (наприклад: 2105953426,2064362674,2133142559):"
        )
        await state.set_state(GroupStates.waiting_for_group_list)
    else:
        await message.answer("❌ Помилка при створенні пакету. Спробуйте ще раз.")
        await state.clear()

@router.callback_query(lambda c: c.data == "add_single_group")
async def add_single_group_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки додавання однієї групи"""
    await callback.message.answer("📝 Введіть назву групи:")
    await state.set_state(GroupStates.waiting_for_group_name)
    await callback.answer()

@router.callback_query(lambda c: c.data == "add_group_list")
async def add_group_list_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки додавання списку груп"""
    await callback.message.answer(
        "📋 Введіть список ID груп через кому (наприклад: 2105953426,2064362674,2133142559):"
    )
    await state.set_state(GroupStates.waiting_for_group_list)
    await callback.answer()

@router.message(GroupStates.waiting_for_group_name)
async def process_group_name(message: Message, state: FSMContext):
    """Обробка назви групи"""
    group_name = message.text.strip()
    await state.update_data(group_name=group_name)
    
    await message.answer("🆔 Введіть ID групи або username (наприклад: @groupname або -1001234567890):")
    await state.set_state(GroupStates.waiting_for_group_id)

@router.message(GroupStates.waiting_for_group_id)
async def process_group_id(message: Message, state: FSMContext):
    """Обробка ID групи"""
    group_id = message.text.strip()
    data = await state.get_data()
    group_name = data['group_name']
    account_phone = data['selected_account']
    
    # Визначаємо username та ID
    group_username = None
    if group_id.startswith('@'):
        group_username = group_id
        group_id = group_id[1:]  # Видаляємо @
    elif group_id.isdigit():
        # Якщо це число, додаємо префікс -100
        if not group_id.startswith('-100'):
            group_id = f"-100{group_id}"
    
    # Додаємо групу до бази даних
    success = db.add_group(group_name, group_id, group_username, account_phone)
    
    if success:
        await message.answer(f"✅ Група '{group_name}' успішно додана до аккаунта {account_phone}!")
    else:
        await message.answer("❌ Помилка при додаванні групи. Спробуйте ще раз.")
    
    await state.clear()

@router.message(GroupStates.waiting_for_group_list)
async def process_group_list(message: Message, state: FSMContext):
    """Обробка списку груп"""
    group_list_text = message.text.strip()
    data = await state.get_data()
    account_phone = data['selected_account']
    package_id = data.get('package_id')
    package_name = data.get('package_name', 'Без пакету')
    
    # Розділяємо список по комах
    group_ids = [gid.strip() for gid in group_list_text.split(',') if gid.strip()]
    
    if not group_ids:
        await message.answer("❌ Список груп порожній. Спробуйте ще раз:")
        return
    
    added_count = 0
    failed_count = 0
    
    for i, group_id in enumerate(group_ids, 1):
        try:
            # Перевіряємо чи це число
            if not group_id.isdigit():
                failed_count += 1
                continue
            
            # Додаємо префікс -100 для груп (якщо його немає)
            if not group_id.startswith('-100'):
                full_group_id = f"-100{group_id}"
            else:
                full_group_id = group_id
            
            # Створюємо назву групи
            group_name = f"Група {group_id}"
            
            # Додаємо групу до бази даних
            success = db.add_group(group_name, full_group_id, None, account_phone, package_id)
            
            if success:
                added_count += 1
            else:
                failed_count += 1
                
        except Exception as e:
            logger.error(f"❌ Помилка при додаванні групи {group_id}: {e}")
            failed_count += 1
    
    # Показуємо результат
    result_text = f"📊 <b>Результат додавання груп:</b>\n\n"
    result_text += f"📦 Пакет: {package_name}\n"
    result_text += f"✅ Успішно додано: {added_count}\n"
    result_text += f"❌ Помилок: {failed_count}\n"
    result_text += f"📱 Аккаунт: {account_phone}"
    
    await message.answer(result_text, parse_mode='HTML')
    await state.clear()

@router.callback_query(lambda c: c.data == "Message_in_all_chat_for_account")
async def message_in_all_chat_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки запуску розсилання"""
    accounts = db.get_accounts()
    
    if not accounts:
        await callback.message.answer("❌ Немає зареєстрованих аккаунтів. Спочатку зареєструйте аккаунт.")
        await callback.answer()
        return
    
    await callback.message.answer("📝 Введіть текст повідомлення для розсилання:")
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.answer()

@router.message(BroadcastStates.waiting_for_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    """Обробка тексту повідомлення для розсилання"""
    message_text = message.text.strip()
    
    if not message_text:
        await message.answer("❌ Текст повідомлення не може бути порожнім. Спробуйте ще раз:")
        return
    
    await state.update_data(message_text=message_text)
    
    # Показуємо список аккаунтів для вибору
    accounts = db.get_accounts()
    keyboard_buttons = []
    
    for account in accounts:
        packages = db.get_group_packages(account['phone_number'])
        total_groups = sum(p['groups_count'] for p in packages)
        button_text = f"📱 {account['phone_number']} ({len(packages)} пакетів, {total_groups} груп)"
        keyboard_buttons.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"select_account_for_broadcast_{account['phone_number']}"
        )])
    
    # Додаємо кнопку для відправки в одну групу
    keyboard_buttons.append([InlineKeyboardButton(
        text="🎯 Відправити в одну групу",
        callback_data="send_to_single_group"
    )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await message.answer(
        "👤 Оберіть аккаунт для розсилання або відправте в одну групу:",
        reply_markup=keyboard
    )

@router.callback_query(lambda c: c.data.startswith("select_account_for_broadcast_"))
async def select_account_for_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору аккаунта для розсилання"""
    account_phone = callback.data.replace("select_account_for_broadcast_", "")
    
    # Отримуємо пакети груп для цього аккаунта
    packages = db.get_group_packages(account_phone)
    
    if not packages:
        await callback.message.answer(f"❌ У аккаунта {account_phone} немає доданих пакетів груп. Спочатку додайте групи.")
        await callback.answer()
        return
    
    # Зберігаємо вибраний аккаунт
    await state.update_data(selected_account=account_phone)
    
    # Показуємо список пакетів для вибору
    keyboard_buttons = []
    for package in packages:
        button_text = f"📦 {package['name']} ({package['groups_count']} груп)"
        keyboard_buttons.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"select_package_{package['id']}"
        )])
    
    keyboard_buttons.append([InlineKeyboardButton(
        text="🔙 Назад",
        callback_data="Message_in_all_chat_for_account"
    )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    packages_text = f"📦 <b>Пакети груп для аккаунта {account_phone}:</b>\n\n"
    for package in packages:
        packages_text += f"📦 {package['name']} ({package['groups_count']} груп)\n"
    
    await callback.message.answer(
        packages_text + "\nОберіть пакет для розсилання:",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("select_package_"))
async def select_package_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору пакету для розсилання"""
    package_id = int(callback.data.replace("select_package_", ""))
    data = await state.get_data()
    account_phone = data['selected_account']
    message_text = data['message_text']
    
    # Отримуємо групи з пакету
    selected_groups = db.get_groups_by_package(package_id)
    
    if not selected_groups:
        await callback.message.answer("❌ Помилка при виборі пакету груп.")
        await callback.answer()
        return
    
    # Зберігаємо вибрані групи
    await state.update_data(selected_groups=selected_groups)
    
    # Показуємо підтвердження
    confirmation_text = f"📤 <b>Підтвердження розсилання:</b>\n\n"
    confirmation_text += f"📱 <b>Аккаунт:</b> {account_phone}\n"
    confirmation_text += f"📦 <b>Пакет:</b> {selected_groups[0].get('package_name', 'Невідомо')}\n"
    confirmation_text += f"👥 <b>Групи:</b> {len(selected_groups)}\n"
    confirmation_text += f"📝 <b>Повідомлення:</b>\n{message_text}\n\n"
    confirmation_text += "Підтвердити розсилання?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Підтвердити", callback_data="confirm_broadcast")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Message_in_all_chat_for_account")]
    ])
    
    await callback.message.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()

@router.callback_query(lambda c: c.data == "send_to_single_group")
async def send_to_single_group_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки відправки в одну групу"""
    await callback.message.answer("🎯 Введіть ID групи для відправки:")
    await state.set_state(GroupStates.waiting_for_single_group_id)
    await callback.answer()

@router.message(GroupStates.waiting_for_single_group_id)
async def process_single_group_id(message: Message, state: FSMContext):
    """Обробка ID однієї групи для відправки"""
    group_id = message.text.strip()
    data = await state.get_data()
    message_text = data['message_text']
    
    # Форматуємо ID групи
    if group_id.isdigit():
        if not group_id.startswith('-100'):
            full_group_id = f"-100{group_id}"
        else:
            full_group_id = group_id
    else:
        await message.answer("❌ ID групи повинен бути числом. Спробуйте ще раз:")
        return
    
    # Створюємо фейкову групу для розсилання
    fake_group = {
        'id': 0,
        'name': f'Група {group_id}',
        'group_id': full_group_id,
        'group_username': None,
        'package_name': 'Одна група'
    }
    
    # Зберігаємо вибрану групу
    await state.update_data(selected_groups=[fake_group])
    
    # Показуємо підтвердження
    confirmation_text = f"📤 <b>Підтвердження відправки в одну групу:</b>\n\n"
    confirmation_text += f"🎯 <b>Група:</b> {group_id}\n"
    confirmation_text += f"📝 <b>Повідомлення:</b>\n{message_text}\n\n"
    confirmation_text += "Підтвердити відправку?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Підтвердити", callback_data="confirm_broadcast")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Message_in_all_chat_for_account")]
    ])
    
    await message.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "confirm_broadcast")
async def confirm_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження розсилання"""
    data = await state.get_data()
    account_phone = data.get('selected_account')
    message_text = data['message_text']
    selected_groups = data['selected_groups']
    
    # Якщо це відправка в одну групу, використовуємо перший доступний аккаунт
    if not account_phone:
        accounts = db.get_accounts()
        if accounts:
            account_phone = accounts[0]['phone_number']
        else:
            await callback.message.answer("❌ Немає доступних аккаунтів для відправки.")
            await state.clear()
            await callback.answer()
            return
    
    # Створюємо статус розсилання
    status_id = db.create_broadcast_status(account_phone, message_text, len(selected_groups))
    
    if status_id:
        if len(selected_groups) == 1 and selected_groups[0].get('package_name') == 'Одна група':
            await callback.message.answer("🚀 Відправка в одну групу запущена! Статус можна переглянути в меню 'Статус розсилання'.")
        else:
            await callback.message.answer("🚀 Розсилання запущено! Статус можна переглянути в меню 'Статус розсилання'.")
        
        # Запускаємо розсилання в фоновому режимі
        asyncio.create_task(send_broadcast_message(account_phone, message_text, selected_groups, status_id))
    else:
        await callback.message.answer("❌ Помилка при запуску розсилання.")
    
    await state.clear()
    await callback.answer()

@router.callback_query(lambda c: c.data == "broadcast_status")
async def broadcast_status_callback(callback: CallbackQuery):
    """Обробка натискання кнопки статусу розсилання"""
    statuses = db.get_broadcast_statuses()
    
    if not statuses:
        await callback.message.answer("📊 Немає історії розсилання.")
        await callback.answer()
        return
    
    status_text = "📊 <b>Статус розсилання:</b>\n\n"
    
    for status in statuses[:10]:  # Показуємо останні 10
        status_emoji = {
            'pending': '⏳',
            'running': '🔄',
            'completed': '✅',
            'failed': '❌'
        }.get(status['status'], '❓')
        
        status_text += f"{status_emoji} <b>{status['phone_number']}</b>\n"
        status_text += f"📝 {status['message_text'][:50]}{'...' if len(status['message_text']) > 50 else ''}\n"
        status_text += f"📊 {status['sent_count']}/{status['total_groups']} відправлено\n"
        status_text += f"❌ Помилок: {status['failed_count']}\n"
        status_text += f"🕒 {status['started_at']}\n"
        if status['finished_at']:
            status_text += f"🏁 {status['finished_at']}\n"
        status_text += "\n"
    
    await callback.message.answer(status_text, parse_mode='HTML')
    await callback.answer()

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
    await start_registration_process(message, state)

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
    await show_accounts_list(message)

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