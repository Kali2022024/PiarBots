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
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
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
            
            # Таблиця налаштувань масової розсилки
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mass_broadcast_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    interval_seconds INTEGER DEFAULT 60,
                    use_random_interval BOOLEAN DEFAULT 0,
                    min_random_seconds INTEGER DEFAULT 30,
                    max_random_seconds INTEGER DEFAULT 120,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")  # Використовуємо WAL режим
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO group_packages (name, account_phone)
                        VALUES (?, ?)
                    """, (name, account_phone))
                    conn.commit()
                    return cursor.lastrowid
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠️ База даних заблокована, спроба {attempt + 1}/{max_retries}")
                    import time
                    time.sleep(0.5 * (attempt + 1))  # Збільшуємо затримку з кожною спробою
                    continue
                else:
                    logger.error(f"❌ Помилка при створенні пакету груп: {e}")
                    return 0
            except Exception as e:
                logger.error(f"❌ Помилка при створенні пакету груп: {e}")
                return 0
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
    
    def get_all_group_packages(self) -> list:
        """Отримати всі пакети груп"""
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM group_packages ORDER BY created_at DESC")
                packages = []
                for row in cursor.fetchall():
                    packages.append(dict(row))
                return packages
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні всіх пакетів груп: {e}")
            return []
    
    def get_group_package(self, package_id: int) -> dict:
        """Отримати конкретний пакет груп за ID"""
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM group_packages WHERE id = ?", (package_id,))
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні пакету груп: {e}")
            return None
    
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
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")  # Використовуємо WAL режим
                    cursor = conn.cursor()
                    
                    # Спочатку перевіряємо, чи група вже існує для цього аккаунта
                    cursor.execute("""
                        SELECT COUNT(*) FROM groups WHERE group_id = ? AND account_phone = ?
                    """, (group_id, account_phone))
                    count = cursor.fetchone()[0]
                    
                    if count > 0:
                        # Група вже існує для цього аккаунта
                        return False
                    
                    # Додаємо групу тільки якщо її немає
                    cursor.execute("""
                        INSERT INTO groups 
                        (name, group_id, group_username, account_phone, package_id)
                        VALUES (?, ?, ?, ?, ?)
                    """, (name, group_id, group_username, account_phone, package_id))
                    conn.commit()
                    return True
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠️ База даних заблокована при додаванні групи, спроба {attempt + 1}/{max_retries}")
                    import time
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    logger.error(f"❌ Помилка при додаванні групи: {e}")
                    return False
            except Exception as e:
                logger.error(f"❌ Помилка при додаванні групи: {e}")
                return False
        return False
    
    def group_exists_in_database(self, group_id: str) -> bool:
        """Перевірити, чи існує група в базі даних (для будь-якого аккаунта)"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT COUNT(*) FROM groups WHERE group_id = ?
                    """, (group_id,))
                    count = cursor.fetchone()[0]
                    return count > 0
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠️ База даних заблокована при перевірці групи, спроба {attempt + 1}/{max_retries}")
                    import time
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    logger.error(f"❌ Помилка при перевірці групи: {e}")
                    return False
            except Exception as e:
                logger.error(f"❌ Помилка при перевірці групи: {e}")
                return False
        return False
    
    def group_exists_for_account(self, group_id: str, account_phone: str) -> bool:
        """Перевірити, чи існує група для конкретного аккаунта"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT COUNT(*) FROM groups WHERE group_id = ? AND account_phone = ?
                    """, (group_id, account_phone))
                    count = cursor.fetchone()[0]
                    return count > 0
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠️ База даних заблокована при перевірці групи для аккаунта, спроба {attempt + 1}/{max_retries}")
                    import time
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    logger.error(f"❌ Помилка при перевірці групи для аккаунта: {e}")
                    return False
            except Exception as e:
                logger.error(f"❌ Помилка при перевірці групи для аккаунта: {e}")
                return False
        return False

    async def send_message_with_retry(self, client, group_id: str, group_name: str, message_text: str, max_retries: int = 3) -> bool:
        """Відправити повідомлення з retry логікою та перевіркою існування групи"""
        import time
        
        for attempt in range(max_retries):
            try:
                # Перевіряємо, чи існує група
                try:
                    entity = await client.get_entity(int(group_id))
                    logger.info(f"✅ Група {group_name} ({group_id}) знайдена, відправляємо повідомлення...")
                except Exception as e:
                    logger.warning(f"⚠️ Група {group_name} ({group_id}) не знайдена: {e}")
                    return False
                
                # Відправляємо повідомлення
                await client.send_message(entity, message_text)
                logger.info(f"✅ Повідомлення успішно відправлено в групу {group_name} ({group_id})")
                return True
                
            except Exception as e:
                error_msg = str(e)
                logger.warning(f"⚠️ Спроба {attempt + 1}/{max_retries} невдала для групи {group_name} ({group_id}): {error_msg}")
                
                # Перевіряємо тип помилки
                if "Could not find the input entity" in error_msg:
                    logger.error(f"❌ Група {group_name} ({group_id}) не існує або недоступна")
                    return False
                elif "Chat admin privileges are required" in error_msg:
                    logger.warning(f"⚠️ Недостатньо прав для відправки в групу {group_name} ({group_id})")
                    return False
                elif "database is locked" in error_msg:
                    logger.warning(f"⚠️ База даних заблокована, очікуємо...")
                    time.sleep(2)
                    continue
                elif attempt < max_retries - 1:
                    # Для інших помилок - повторюємо спробу
                    wait_time = 2 ** attempt  # Експоненційна затримка
                    logger.info(f"⏳ Очікуємо {wait_time} секунд перед наступною спробою...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ Всі спроби вичерпано для групи {group_name} ({group_id}): {error_msg}")
                    return False
        
        return False
    
    def get_groups_for_account(self, account_phone: str) -> list:
        """Отримати всі групи для конкретного аккаунта"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")  # Використовуємо WAL режим
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
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠️ База даних заблокована при отриманні груп, спроба {attempt + 1}/{max_retries}")
                    import time
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    logger.error(f"❌ Помилка при отриманні груп: {e}")
                    return []
            except Exception as e:
                logger.error(f"❌ Помилка при отриманні груп: {e}")
                return []
        return []
    
    def get_groups_by_package(self, package_id: int) -> list:
        """Отримати всі групи в конкретному пакеті"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
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
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠️ База даних заблокована при отриманні груп пакету, спроба {attempt + 1}/{max_retries}")
                    import time
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    logger.error(f"❌ Помилка при отриманні груп пакету: {e}")
                    return []
            except Exception as e:
                logger.error(f"❌ Помилка при отриманні груп пакету: {e}")
                return []
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
    
    # Методи для роботи з налаштуваннями масової розсилки
    def get_mass_broadcast_settings(self) -> dict:
        """Отримати налаштування масової розсилки"""
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM mass_broadcast_settings ORDER BY id DESC LIMIT 1")
                row = cursor.fetchone()
                if row:
                    return dict(row)
                else:
                    # Створюємо налаштування за замовчуванням
                    cursor.execute("""
                        INSERT INTO mass_broadcast_settings 
                        (interval_seconds, use_random_interval, min_random_seconds, max_random_seconds)
                        VALUES (60, 0, 30, 120)
                    """)
                    conn.commit()
                    return {
                        'id': cursor.lastrowid,
                        'interval_seconds': 60,
                        'use_random_interval': 0,
                        'min_random_seconds': 30,
                        'max_random_seconds': 120
                    }
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні налаштувань масової розсилки: {e}")
            return {
                'interval_seconds': 60,
                'use_random_interval': 0,
                'min_random_seconds': 30,
                'max_random_seconds': 120
            }
    
    def update_mass_broadcast_settings(self, interval_seconds: int, use_random_interval: bool = False, 
                                     min_random_seconds: int = 30, max_random_seconds: int = 120) -> bool:
        """Оновити налаштування масової розсилки"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR REPLACE INTO mass_broadcast_settings 
                        (id, interval_seconds, use_random_interval, min_random_seconds, max_random_seconds, updated_at)
                        VALUES (1, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (interval_seconds, use_random_interval, min_random_seconds, max_random_seconds))
                    conn.commit()
                    return True
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠️ База даних заблокована при оновленні налаштувань, спроба {attempt + 1}/{max_retries}")
                    import time
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    logger.error(f"❌ Помилка при оновленні налаштувань масової розсилки: {e}")
                    return False
            except Exception as e:
                logger.error(f"❌ Помилка при оновленні налаштувань масової розсилки: {e}")
                return False
        return False


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

class JoinGroupsStates(StatesGroup):
    waiting_for_group_ids = State()
    waiting_for_interval = State()
    waiting_for_account_selection = State()

class MassBroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_interval = State()
    waiting_for_random_settings = State()
    waiting_for_package_selection = State()

class DeletePackageStates(StatesGroup):
    waiting_for_package_name = State()

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
                
                # Відправляємо повідомлення з retry логікою
                success = await db.send_message_with_retry(
                    client, 
                    group_id, 
                    group['name'], 
                    message_text
                )
                
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
                
                # Оновлюємо статус
                db.update_broadcast_status(status_id, sent_count=sent_count, failed_count=failed_count)
                
                # Затримка між повідомленнями
                await asyncio.sleep(2)
                
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Критична помилка при відправці в групу {group['name']} (ID: {group['group_id']}): {e}")
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
        [InlineKeyboardButton(text="👥 Додати список груп", callback_data="Groups_for_account")],
        [InlineKeyboardButton(text="➕ Додатись в групи", callback_data="join_groups")],
        [InlineKeyboardButton(text="🗑️ Видалити пакет груп", callback_data="delete_group_package")],
        [InlineKeyboardButton(text="📊 Статус розсилання", callback_data="broadcast_status")],
        [InlineKeyboardButton(text="📤 Масова розсилка", callback_data="Mass_broadcast")]
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
    await callback.message.answer("📦 Введіть назву пакету груп:")
    await state.set_state(GroupStates.waiting_for_package_name)
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
    
    # Отримуємо всі аккаунти
    accounts = db.get_accounts()
    
    if not accounts:
        await message.answer("❌ Немає зареєстрованих аккаунтів.")
        await state.clear()
        return
    
    await message.answer(
        f"📦 <b>Створення пакету '{package_name}':</b>\n\n"
        f"👥 <b>Аккаунтів:</b> {len(accounts)}\n\n"
        f"📋 Введіть список ID груп через кому (наприклад: 2105953426,2064362674,2133142559):"
    )
    await state.update_data(package_name=package_name)
    await state.set_state(GroupStates.waiting_for_group_list)

@router.callback_query(lambda c: c.data == "add_single_group")
async def add_single_group_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки додавання однієї групи"""
    await callback.message.answer("📝 Введіть назву групи:")
    await state.set_state(GroupStates.waiting_for_group_name)
    await callback.answer()

@router.callback_query(lambda c: c.data == "add_group_list")
async def add_group_list_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки додавання списку груп для всіх аккаунтів"""
    # Отримуємо всі аккаунти
    accounts = db.get_accounts()
    
    if not accounts:
        await callback.message.answer("❌ Немає зареєстрованих аккаунтів. Спочатку зареєструйте аккаунт.")
        await callback.answer()
        return
    
    await callback.message.answer(
        f"📋 <b>Додавання списку груп для всіх аккаунтів:</b>\n\n"
        f"👥 <b>Аккаунтів:</b> {len(accounts)}\n\n"
        f"Введіть список ID груп через кому (наприклад: 2105953426,2064362674,2133142559):"
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
    """Обробка списку груп для всіх аккаунтів"""
    group_list_text = message.text.strip()
    data = await state.get_data()
    package_name = data.get('package_name', 'Без назви')
    
    # Розділяємо список по комах
    group_ids = [gid.strip() for gid in group_list_text.split(',') if gid.strip()]
    
    if not group_ids:
        await message.answer("❌ Список груп порожній. Спробуйте ще раз:")
        return
    
    # Отримуємо всі аккаунти
    accounts = db.get_accounts()
    
    if not accounts:
        await message.answer("❌ Немає зареєстрованих аккаунтів.")
        await state.clear()
        return
    
    # Створюємо один пакет для всіх аккаунтів
    # Використовуємо перший аккаунт як власника пакету
    first_account = accounts[0]['phone_number']
    package_id = db.create_group_package(package_name, first_account)
    
    if not package_id:
        await message.answer("❌ Помилка створення пакету.")
        await state.clear()
        return
    
    # Спочатку фільтруємо дублікати всередині списку та перевіряємо в базі даних
    unique_groups = []
    seen_groups = set()  # Для відстеження дублікатів всередині списку
    
    for group_id in group_ids:
        try:
            # Перевіряємо чи це число
            if not group_id.isdigit():
                continue
            
            # Додаємо префікс -100 для груп (якщо його немає)
            if not group_id.startswith('-100'):
                full_group_id = f"-100{group_id}"
            else:
                full_group_id = group_id
            
            # Перевіряємо дублікати всередині списку
            if full_group_id in seen_groups:
                continue
            seen_groups.add(full_group_id)
            
            # Перевіряємо, чи група вже існує в базі даних (для будь-якого аккаунта)
            if not db.group_exists_in_database(full_group_id):
                unique_groups.append(full_group_id)
        except Exception as e:
            logger.error(f"❌ Помилка при обробці групи {group_id}: {e}")
    
    if not unique_groups:
        await message.answer("ℹ️ Всі групи зі списку вже існують в базі даних.")
        await state.clear()
        return
    
    # Підраховуємо дублікати всередині списку
    duplicates_in_list = len(group_ids) - len(seen_groups)
    
    if duplicates_in_list > 0:
        await message.answer(f"🔄 Знайдено {duplicates_in_list} дублікатів всередині списку.")
    
    await message.answer(f"📋 Знайдено {len(unique_groups)} нових груп з {len(seen_groups)} унікальних в списку.")
    await message.answer(f"🚀 Додавання {len(unique_groups)} груп в пакет '{package_name}'...")
    
    total_added = 0
    total_failed = 0
    
    # Додаємо групи для всіх аккаунтів
    for full_group_id in unique_groups:
        try:
            # Створюємо назву групи
            group_name = f"Група {full_group_id.replace('-100', '')}"
            
            # Додаємо групу для кожного аккаунта
            for account in accounts:
                account_phone = account['phone_number']
                
                # Додаємо групу до бази даних (метод сам перевіряє дублікати)
                success = db.add_group(group_name, full_group_id, None, account_phone, package_id)
                
                if success:
                    total_added += 1
                else:
                    # Якщо success = False, це означає що група вже існує
                    pass
                    
        except Exception as e:
            total_failed += 1
            logger.error(f"❌ Помилка при додаванні групи {full_group_id}: {e}")
    
    await message.answer(f"✅ Додано {total_added} груп, помилок {total_failed}")
    
    # Показуємо загальний результат
    result_text = f"📊 <b>Загальний результат додавання груп:</b>\n\n"
    result_text += f"📦 <b>Пакет:</b> {package_name}\n"
    result_text += f"👥 <b>Аккаунтів:</b> {len(accounts)}\n"
    result_text += f"📋 <b>Груп в списку:</b> {len(group_ids)}\n"
    result_text += f"🔄 <b>Дублікатів в списку:</b> {duplicates_in_list}\n"
    result_text += f"🔍 <b>Унікальних в списку:</b> {len(seen_groups)}\n"
    result_text += f"🆕 <b>Нових груп:</b> {len(unique_groups)}\n"
    result_text += f"✅ <b>Всього додано:</b> {total_added}\n"
    result_text += f"❌ <b>Всього помилок:</b> {total_failed}\n\n"
    result_text += f"📦 <b>Створено пакетів:</b> 1\n"
    result_text += f"ℹ️ <b>Примітка:</b> Кожна група додається для всіх аккаунтів"
    
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
        callback_data="Mass_broadcast"
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
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")]
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
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")]
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

@router.callback_query(lambda c: c.data == "Mass_broadcast")
async def mass_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки масової розсилки"""
    accounts = db.get_accounts()
    
    if not accounts:
        await callback.message.answer("❌ Немає зареєстрованих аккаунтів. Спочатку зареєструйте аккаунт.")
        await callback.answer()
        return
    
    # Отримуємо поточні налаштування
    settings = db.get_mass_broadcast_settings()
    
    # Показуємо поточні налаштування
    settings_text = f"⚙️ <b>Поточні налаштування масової розсилки:</b>\n\n"
    settings_text += f"⏱️ <b>Інтервал:</b> {settings['interval_seconds']} секунд\n"
    if settings['use_random_interval']:
        settings_text += f"🎲 <b>Рандомний інтервал:</b> {settings['min_random_seconds']}-{settings['max_random_seconds']} секунд\n"
    else:
        settings_text += f"🎲 <b>Рандомний інтервал:</b> Вимкнено\n"
    settings_text += f"👥 <b>Аккаунтів:</b> {len(accounts)}\n\n"
    settings_text += "📝 Введіть текст повідомлення для масової розсилки:"
    
    await callback.message.answer(settings_text, parse_mode='HTML')
    await state.set_state(MassBroadcastStates.waiting_for_message)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_message)
async def process_mass_broadcast_message(message: Message, state: FSMContext):
    """Обробка тексту повідомлення для масової розсилки"""
    message_text = message.text.strip()
    
    if not message_text:
        await message.answer("❌ Текст повідомлення не може бути порожнім. Спробуйте ще раз:")
        return
    
    # Зберігаємо текст повідомлення
    await state.update_data(message_text=message_text)
    
    # Отримуємо налаштування
    settings = db.get_mass_broadcast_settings()
    
    # Показуємо налаштування інтервалу
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ 10 секунд", callback_data="mass_interval_10")],
        [InlineKeyboardButton(text="⏱️ 30 секунд", callback_data="mass_interval_30")],
        [InlineKeyboardButton(text="⏰ 1 хвилина", callback_data="mass_interval_60")],
        [InlineKeyboardButton(text="🕐 5 хвилин", callback_data="mass_interval_300")],
        [InlineKeyboardButton(text="🕑 15 хвилин", callback_data="mass_interval_900")],
        [InlineKeyboardButton(text="🕒 1 година", callback_data="mass_interval_3600")],
        [InlineKeyboardButton(text="🕓 6 годин", callback_data="mass_interval_21600")],
        [InlineKeyboardButton(text="🕔 24 години", callback_data="mass_interval_86400")],
        [InlineKeyboardButton(text="✏️ Ввести власний", callback_data="mass_interval_custom")],
        [InlineKeyboardButton(text="🎲 Рандомний інтервал", callback_data="mass_random_interval")]
    ])
    
    await message.answer(
        f"⏱️ <b>Налаштування інтервалу між аккаунтами:</b>\n\n"
        f"📝 <b>Повідомлення:</b> {message_text[:50]}{'...' if len(message_text) > 50 else ''}\n\n"
        f"Поточний інтервал: {settings['interval_seconds']} секунд\n\n"
        f"Оберіть інтервал між розсилками з різних аккаунтів:",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@router.callback_query(lambda c: c.data.startswith("mass_interval_"))
async def process_mass_interval_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору інтервалу для масової розсилки"""
    interval_data = callback.data.replace("mass_interval_", "")
    
    if interval_data == "custom":
        await callback.message.answer(
            "✏️ Введіть інтервал в секундах (від 10 до 86400):"
        )
        await state.set_state(MassBroadcastStates.waiting_for_interval)
    else:
        try:
            interval = int(interval_data)
            await state.update_data(interval=interval)
            await start_mass_broadcast_process(callback, state)
        except ValueError:
            await callback.message.answer("❌ Помилка при обробці інтервалу.")
    
    await callback.answer()

@router.callback_query(lambda c: c.data == "mass_random_interval")
async def process_mass_random_interval_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору рандомного інтервалу"""
    await callback.message.answer(
        "🎲 <b>Налаштування рандомного інтервалу:</b>\n\n"
        "Введіть мінімальний та максимальний інтервал через кому:\n"
        "Наприклад: 30,120 (від 30 до 120 секунд)",
        parse_mode='HTML'
    )
    await state.set_state(MassBroadcastStates.waiting_for_random_settings)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_interval)
async def process_custom_mass_interval(message: Message, state: FSMContext):
    """Обробка власного інтервалу для масової розсилки"""
    try:
        interval = int(message.text.strip())
        if 10 <= interval <= 86400:  # Від 10 секунд до 24 годин
            await state.update_data(interval=interval)
            await start_mass_broadcast_process(message, state)
        else:
            await message.answer("❌ Інтервал повинен бути від 10 до 86400 секунд. Спробуйте ще раз:")
    except ValueError:
        await message.answer("❌ Введіть число від 10 до 86400. Спробуйте ще раз:")

@router.message(MassBroadcastStates.waiting_for_random_settings)
async def process_random_settings(message: Message, state: FSMContext):
    """Обробка налаштувань рандомного інтервалу"""
    try:
        settings_text = message.text.strip()
        min_interval, max_interval = map(int, settings_text.split(','))
        
        if 10 <= min_interval <= max_interval <= 86400:
            await state.update_data(
                use_random_interval=True,
                min_random_seconds=min_interval,
                max_random_seconds=max_interval
            )
            await start_mass_broadcast_process(message, state)
        else:
            await message.answer("❌ Невірний діапазон. Мінімум: 10, максимум: 86400. Спробуйте ще раз:")
    except ValueError:
        await message.answer("❌ Введіть два числа через кому (наприклад: 30,120). Спробуйте ще раз:")

async def start_mass_broadcast_process(message_or_callback, state: FSMContext):
    """Показ вибору пакетів для масової розсилки"""
    data = await state.get_data()
    message_text = data['message_text']
    interval = data.get('interval', 60)
    use_random = data.get('use_random_interval', False)
    min_random = data.get('min_random_seconds', 30)
    max_random = data.get('max_random_seconds', 120)
    
    # Отримуємо всі аккаунти
    accounts = db.get_accounts()
    
    if not accounts:
        if hasattr(message_or_callback, 'message'):
            await message_or_callback.message.answer("❌ Немає зареєстрованих аккаунтів.")
        else:
            await message_or_callback.answer("❌ Немає зареєстрованих аккаунтів.")
        return
    
    # Отримуємо всі пакети груп
    packages = db.get_all_group_packages()
    
    if not packages:
        if hasattr(message_or_callback, 'message'):
            await message_or_callback.message.answer("❌ Немає створених пакетів груп. Спочатку створіть пакети груп.")
        else:
            await message_or_callback.answer("❌ Немає створених пакетів груп. Спочатку створіть пакети груп.")
        return
    
    # Показуємо вибір пакетів
    selection_text = f"📦 <b>Вибір пакетів груп для масової розсилки:</b>\n\n"
    selection_text += f"📝 <b>Повідомлення:</b> {message_text[:50]}{'...' if len(message_text) > 50 else ''}\n"
    
    if use_random:
        selection_text += f"🎲 <b>Рандомний інтервал:</b> {min_random}-{max_random} секунд\n"
    else:
        selection_text += f"⏱️ <b>Інтервал:</b> {interval} секунд\n"
    
    selection_text += f"👥 <b>Аккаунтів:</b> {len(accounts)}\n\n"
    selection_text += "Оберіть пакети груп для розсилки:"
    
    # Створюємо клавіатуру з пакетами
    keyboard_buttons = []
    for package in packages:
        # Отримуємо кількість груп в пакеті
        groups_count = len(db.get_groups_by_package(package['id']))
        button_text = f"📦 {package['name']} ({groups_count} груп)"
        keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"mass_select_package_{package['id']}")])
    
    keyboard_buttons.append([InlineKeyboardButton(text="✅ Всі пакети", callback_data="mass_select_all_packages")])
    keyboard_buttons.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(selection_text, parse_mode='HTML', reply_markup=keyboard)
    else:
        await message_or_callback.answer(selection_text, parse_mode='HTML', reply_markup=keyboard)

@router.callback_query(lambda c: c.data.startswith("mass_select_package_"))
async def mass_select_package_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору пакету для масової розсилки"""
    package_id = int(callback.data.replace("mass_select_package_", ""))
    
    # Зберігаємо вибраний пакет
    await state.update_data(selected_package_id=package_id)
    
    # Показуємо підтвердження
    await show_mass_broadcast_confirmation(callback, state)

@router.callback_query(lambda c: c.data == "mass_select_all_packages")
async def mass_select_all_packages_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору всіх пакетів для масової розсилки"""
    # Зберігаємо вибір всіх пакетів
    await state.update_data(selected_package_id=None)  # None означає всі пакети
    
    # Показуємо підтвердження
    await show_mass_broadcast_confirmation(callback, state)

async def show_mass_broadcast_confirmation(callback: CallbackQuery, state: FSMContext):
    """Показ підтвердження масової розсилки"""
    data = await state.get_data()
    message_text = data['message_text']
    interval = data.get('interval', 60)
    use_random = data.get('use_random_interval', False)
    min_random = data.get('min_random_seconds', 30)
    max_random = data.get('max_random_seconds', 120)
    selected_package_id = data.get('selected_package_id')
    
    # Отримуємо всі аккаунти
    accounts = db.get_accounts()
    
    # Показуємо підтвердження
    confirmation_text = f"📤 <b>Підтвердження масової розсилки:</b>\n\n"
    confirmation_text += f"👥 <b>Аккаунтів:</b> {len(accounts)}\n"
    confirmation_text += f"📝 <b>Повідомлення:</b> {message_text[:100]}{'...' if len(message_text) > 100 else ''}\n"
    
    if selected_package_id:
        package = db.get_group_package(selected_package_id)
        if package:
            groups_count = len(db.get_groups_by_package(selected_package_id))
            confirmation_text += f"📦 <b>Пакет:</b> {package['name']} ({groups_count} груп)\n"
    else:
        confirmation_text += f"📦 <b>Пакети:</b> Всі пакети\n"
    
    if use_random:
        confirmation_text += f"🎲 <b>Рандомний інтервал:</b> {min_random}-{max_random} секунд\n"
    else:
        confirmation_text += f"⏱️ <b>Інтервал:</b> {interval} секунд\n"
    
    confirmation_text += "\nПідтвердити масову розсилку?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Підтвердити", callback_data="confirm_mass_broadcast")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")]
    ])
    
    await callback.message.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()

@router.callback_query(lambda c: c.data == "confirm_mass_broadcast")
async def confirm_mass_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження масової розсилки"""
    data = await state.get_data()
    message_text = data['message_text']
    interval = data.get('interval', 60)
    use_random = data.get('use_random_interval', False)
    min_random = data.get('min_random_seconds', 30)
    max_random = data.get('max_random_seconds', 120)
    selected_package_id = data.get('selected_package_id')
    
    # Зберігаємо налаштування
    db.update_mass_broadcast_settings(interval, use_random, min_random, max_random)
    
    await callback.message.answer("🚀 Запуск масової розсилки...")
    
    # Запускаємо масову розсилку в фоновому режимі
    asyncio.create_task(mass_broadcast_process(message_text, interval, use_random, min_random, max_random, selected_package_id, callback.message))
    
    await state.clear()
    await callback.answer()

async def mass_broadcast_process(message_text: str, interval: int, use_random: bool, 
                               min_random: int, max_random: int, selected_package_id: int, message_obj):
    """Функція для масової розсилки через всі аккаунти"""
    try:
        accounts = db.get_accounts()
        
        if not accounts:
            await message_obj.answer("❌ Немає зареєстрованих аккаунтів.")
            return
        
        total_accounts = len(accounts)
        successful_accounts = 0
        failed_accounts = 0
        
        await message_obj.answer(f"🚀 Початок масової розсилки з {total_accounts} аккаунтів...")
        
        for i, account in enumerate(accounts, 1):
            try:
                await message_obj.answer(f"📤 Розсилка з аккаунта {i}/{total_accounts}: {account['phone_number']}")
                
                # Отримуємо групи для цього аккаунта з retry логікою
                groups = []
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        if selected_package_id:
                            # Отримуємо всі групи з конкретного пакету (незалежно від аккаунта)
                            groups = db.get_groups_by_package(selected_package_id)
                        else:
                            # Отримуємо всі групи для цього аккаунта
                            groups = db.get_groups_for_account(account['phone_number'])
                        break
                    except Exception as e:
                        if "database is locked" in str(e) and attempt < max_retries - 1:
                            logger.warning(f"⚠️ База даних заблокована при отриманні груп, спроба {attempt + 1}/{max_retries}")
                            import time
                            time.sleep(0.5 * (attempt + 1))
                            continue
                        else:
                            logger.error(f"❌ Помилка при отриманні груп: {e}")
                            break
                
                if not groups:
                    if selected_package_id:
                        await message_obj.answer(f"⚠️ У пакету немає груп для відправки")
                    else:
                        await message_obj.answer(f"⚠️ У аккаунта {account['phone_number']} немає груп")
                    failed_accounts += 1
                    continue
                
                # Відправляємо повідомлення
                sent_count = 0
                failed_count = 0
                
                # Створюємо клієнт один раз для аккаунта
                session_name = f"sessions/temp_{account['phone_number'].replace('+', '').replace('-', '')}"
                client = TelegramClient(session_name, account['api_id'], account['api_hash'])
                
                try:
                    await client.connect()
                    
                    if await client.is_user_authorized():
                        for group in groups:
                            try:
                                # Відправляємо повідомлення з retry логікою
                                group_id = int(group['group_id'])
                                success = await db.send_message_with_retry(
                                    client, 
                                    group_id, 
                                    group['name'], 
                                    message_text
                                )
                                
                                if success:
                                    sent_count += 1
                                else:
                                    failed_count += 1
                                
                                # Затримка між повідомленнями
                                await asyncio.sleep(2)
                                
                            except Exception as e:
                                failed_count += 1
                                logger.error(f"❌ Критична помилка при відправці в групу {group['name']}: {e}")
                    else:
                        await message_obj.answer(f"❌ Аккаунт {account['phone_number']} не авторизований")
                        failed_accounts += 1
                        continue
                        
                except Exception as e:
                    failed_count += len(groups)
                    logger.error(f"❌ Помилка підключення аккаунта {account['phone_number']}: {e}")
                finally:
                    # Завжди закриваємо клієнт
                    try:
                        await client.disconnect()
                    except:
                        pass
                
                await message_obj.answer(
                    f"✅ Аккаунт {account['phone_number']}: відправлено {sent_count}, помилок {failed_count}"
                )
                successful_accounts += 1
                
                # Затримка між аккаунтами
                if i < total_accounts:
                    if use_random:
                        import random
                        wait_time = random.randint(min_random, max_random)
                    else:
                        wait_time = interval
                    
                    await message_obj.answer(f"⏱️ Очікування {wait_time} секунд до наступного аккаунта...")
                    await asyncio.sleep(wait_time)
                
            except Exception as e:
                failed_accounts += 1
                await message_obj.answer(f"❌ Помилка з аккаунтом {account['phone_number']}: {str(e)[:100]}")
                logger.error(f"❌ Помилка з аккаунтом {account['phone_number']}: {e}")
        
        # Показуємо підсумок
        summary_text = f"📊 <b>Підсумок масової розсилки:</b>\n\n"
        summary_text += f"👥 <b>Всього аккаунтів:</b> {total_accounts}\n"
        summary_text += f"✅ <b>Успішних:</b> {successful_accounts}\n"
        summary_text += f"❌ <b>Помилок:</b> {failed_accounts}\n"
        summary_text += f"📝 <b>Повідомлення:</b> {message_text[:50]}{'...' if len(message_text) > 50 else ''}"
        
        await message_obj.answer(summary_text, parse_mode='HTML')
        
    except Exception as e:
        await message_obj.answer(f"❌ Критична помилка масової розсилки: {str(e)[:200]}")
        logger.error(f"❌ Критична помилка масової розсилки: {e}")

@router.message(BroadcastStates.waiting_for_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    """Обробка тексту повідомлення для розсилання"""
    message_text = message.text.strip()
    
    if not message_text:
        await message.answer("❌ Текст повідомлення не може бути порожнім. Спробуйте ще раз:")
        return
    
    await state.update_data(message_text=message_text)
    
    # Отримуємо всі аккаунти
    accounts = db.get_accounts()
    
    if not accounts:
        await message.answer("❌ Немає зареєстрованих аккаунтів. Спочатку зареєструйте аккаунт.")
        return
    
    # Використовуємо ВСІ аккаунти
    await state.update_data(selected_accounts=accounts)
    
    # Показуємо пакети для першого аккаунта
    first_account = accounts[0]
    packages = db.get_group_packages(first_account['phone_number'])
    
    if not packages:
        await message.answer(f"❌ У першого аккаунта {first_account['phone_number']} немає пакетів груп. Спочатку додайте групи.")
        return
    
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
        callback_data="Mass_broadcast"
    )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    # Формуємо список всіх аккаунтів
    accounts_list = ", ".join([acc['phone_number'] for acc in accounts])
    
    packages_text = f"📦 <b>Пакети груп для масової розсилки:</b>\n\n"
    packages_text += f"📱 <b>Всі аккаунти ({len(accounts)}):</b> {accounts_list}\n\n"
    packages_text += f"📦 <b>Пакети для {first_account['phone_number']}:</b>\n"
    for package in packages:
        packages_text += f"📦 {package['name']} ({package['groups_count']} груп)\n"
    
    await message.answer(
        packages_text + "\nОберіть пакет для розсилання:",
        parse_mode='HTML',
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
        callback_data="Mass_broadcast"
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
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")]
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
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")]
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

@router.callback_query(lambda c: c.data == "delete_group_package")
async def delete_group_package_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки видалення пакету груп"""
    # Отримуємо всі пакети
    packages = db.get_all_group_packages()
    
    if not packages:
        await callback.message.answer("❌ Немає створених пакетів груп.")
        await callback.answer()
        return
    
    # Показуємо список пакетів
    packages_text = "📦 <b>Наявні пакети груп:</b>\n\n"
    for i, package in enumerate(packages, 1):
        # Отримуємо кількість груп в пакеті
        groups_count = len(db.get_groups_by_package(package['id']))
        packages_text += f"{i}. <b>{package['name']}</b>\n"
        packages_text += f"   👤 Аккаунт: {package['account_phone']}\n"
        packages_text += f"   📊 Груп: {groups_count}\n"
        packages_text += f"   📅 Створено: {package['created_at']}\n\n"
    
    packages_text += "Введіть назву пакету для видалення:"
    
    await callback.message.answer(packages_text, parse_mode='HTML')
    await state.set_state(DeletePackageStates.waiting_for_package_name)
    await callback.answer()

@router.message(DeletePackageStates.waiting_for_package_name)
async def process_delete_package_name(message: Message, state: FSMContext):
    """Обробка назви пакету для видалення"""
    package_name = message.text.strip()
    
    # Знаходимо пакет за назвою
    packages = db.get_all_group_packages()
    target_package = None
    
    for package in packages:
        if package['name'].lower() == package_name.lower():
            target_package = package
            break
    
    if not target_package:
        await message.answer("❌ Пакет з такою назвою не знайдено. Спробуйте ще раз:")
        return
    
    # Отримуємо групи в пакеті
    groups = db.get_groups_by_package(target_package['id'])
    groups_count = len(groups)
    
    # Показуємо підтвердження
    confirmation_text = f"⚠️ <b>Підтвердження видалення пакету:</b>\n\n"
    confirmation_text += f"📦 <b>Назва:</b> {target_package['name']}\n"
    confirmation_text += f"👤 <b>Аккаунт:</b> {target_package['account_phone']}\n"
    confirmation_text += f"📊 <b>Груп в пакеті:</b> {groups_count}\n"
    confirmation_text += f"📅 <b>Створено:</b> {target_package['created_at']}\n\n"
    confirmation_text += f"<b>Це дія видалить пакет та всі {groups_count} груп в ньому!</b>\n\n"
    confirmation_text += "Підтвердити видалення?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Підтвердити видалення", callback_data=f"confirm_delete_package_{target_package['id']}")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="delete_group_package")]
    ])
    
    await message.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)
    await state.clear()

@router.callback_query(lambda c: c.data.startswith("confirm_delete_package_"))
async def confirm_delete_package_callback(callback: CallbackQuery):
    """Підтвердження видалення пакету"""
    package_id = int(callback.data.replace("confirm_delete_package_", ""))
    
    # Отримуємо інформацію про пакет
    package = db.get_group_package(package_id)
    if not package:
        await callback.message.answer("❌ Пакет не знайдено.")
        await callback.answer()
        return
    
    # Видаляємо пакет
    success = db.delete_group_package(package_id, package['account_phone'])
    
    if success:
        await callback.message.answer(
            f"✅ Пакет '{package['name']}' та всі його групи успішно видалено!"
        )
    else:
        await callback.message.answer("❌ Помилка при видаленні пакету.")
    
    await callback.answer()

@router.callback_query(lambda c: c.data == "join_groups")
async def join_groups_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки додавання до груп"""
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
            callback_data=f"select_account_for_join_{account['phone_number']}"
        )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await callback.message.answer(
        "👤 Оберіть аккаунт для приєднання до груп:",
        reply_markup=keyboard
    )
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("select_account_for_join_"))
async def select_account_for_join_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору аккаунта для приєднання до груп"""
    account_phone = callback.data.replace("select_account_for_join_", "")
    
    # Зберігаємо вибраний аккаунт
    await state.update_data(selected_account=account_phone)
    
    await callback.message.answer(
        "📋 Введіть ID груп або посилання через кому, до яких потрібно приєднатись:\n\n"
        "📝 <b>Підтримувані формати:</b>\n"
        "• ID груп: 2105953426,2064362674,2133142559 (автоматично додасться -100)\n"
        "• Username: @groupname, @channelname\n"
        "• Посилання: https://t.me/groupname\n"
        "• Invite посилання: https://t.me/joinchat/AAAAAEkk2WdoDrB4-Q8-gg\n\n"
        "💡 <b>Примітка:</b> Можна змішувати різні формати в одному списку",
        parse_mode='HTML'
    )
    await state.set_state(JoinGroupsStates.waiting_for_group_ids)
    await callback.answer()

@router.message(JoinGroupsStates.waiting_for_group_ids)
async def process_group_ids_for_join(message: Message, state: FSMContext):
    """Обробка списку ID груп для приєднання"""
    group_ids_text = message.text.strip()
    data = await state.get_data()
    account_phone = data['selected_account']
    
    # Розділяємо список по комах
    group_ids = [gid.strip() for gid in group_ids_text.split(',') if gid.strip()]
    
    if not group_ids:
        await message.answer("❌ Список груп порожній. Спробуйте ще раз:")
        return
    
    # Валідуємо та обробляємо різні формати груп
    valid_group_ids = []
    for group_input in group_ids:
        group_input = group_input.strip()
        
        # Перевіряємо різні формати
        if group_input.isdigit():
            # Звичайний ID групи
            valid_group_ids.append(group_input)
        elif group_input.startswith('@'):
            # Username
            valid_group_ids.append(group_input)
        elif group_input.startswith('https://t.me/'):
            # Посилання
            valid_group_ids.append(group_input)
        elif group_input.startswith('t.me/'):
            # Посилання без https
            valid_group_ids.append(f"https://{group_input}")
        else:
            await message.answer(f"❌ Невірний формат '{group_input}'. Використовуйте ID, @username або посилання. Спробуйте ще раз:")
            return
    
    # Зберігаємо список груп
    await state.update_data(group_ids=valid_group_ids)
    
    # Показуємо налаштування інтервалу
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ 5 секунд", callback_data="interval_5")],
        [InlineKeyboardButton(text="⏱️ 10 секунд", callback_data="interval_10")],
        [InlineKeyboardButton(text="⏰ 15 секунд", callback_data="interval_15")],
        [InlineKeyboardButton(text="🕐 30 секунд", callback_data="interval_30")],
        [InlineKeyboardButton(text="🕑 60 секунд", callback_data="interval_60")],
        [InlineKeyboardButton(text="✏️ Ввести власний", callback_data="interval_custom")]
    ])
    
    await message.answer(
        f"⏱️ <b>Налаштування інтервалу:</b>\n\n"
        f"📱 <b>Аккаунт:</b> {account_phone}\n"
        f"👥 <b>Груп для приєднання:</b> {len(valid_group_ids)}\n\n"
        f"Оберіть інтервал між приєднаннями до груп:",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@router.callback_query(lambda c: c.data.startswith("interval_"))
async def process_interval_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору інтервалу"""
    interval_data = callback.data.replace("interval_", "")
    
    if interval_data == "custom":
        await callback.message.answer(
            "✏️ Введіть інтервал в секундах (від 5 до 60):"
        )
        await state.set_state(JoinGroupsStates.waiting_for_interval)
    else:
        try:
            interval = int(interval_data)
            await state.update_data(interval=interval)
            await start_join_groups_process(callback, state)
        except ValueError:
            await callback.message.answer("❌ Помилка при обробці інтервалу.")
    
    await callback.answer()

@router.message(JoinGroupsStates.waiting_for_interval)
async def process_custom_interval(message: Message, state: FSMContext):
    """Обробка власного інтервалу"""
    try:
        interval = int(message.text.strip())
        if 5 <= interval <= 60:
            await state.update_data(interval=interval)
            await start_join_groups_process(message, state)
        else:
            await message.answer("❌ Інтервал повинен бути від 5 до 60 секунд. Спробуйте ще раз:")
    except ValueError:
        await message.answer("❌ Введіть число від 5 до 60. Спробуйте ще раз:")

async def start_join_groups_process(message_or_callback, state: FSMContext):
    """Запуск процесу приєднання до груп"""
    data = await state.get_data()
    account_phone = data['selected_account']
    group_ids = data['group_ids']
    interval = data['interval']
    
    # Показуємо підтвердження
    confirmation_text = f"📤 <b>Підтвердження приєднання до груп:</b>\n\n"
    confirmation_text += f"📱 <b>Аккаунт:</b> {account_phone}\n"
    confirmation_text += f"👥 <b>Груп для приєднання:</b> {len(group_ids)}\n"
    confirmation_text += f"⏱️ <b>Інтервал:</b> {interval} секунд\n\n"
    confirmation_text += f"📋 <b>Список груп:</b>\n"
    for i, group_id in enumerate(group_ids[:10], 1):  # Показуємо перші 10
        confirmation_text += f"{i}. {group_id}\n"
    if len(group_ids) > 10:
        confirmation_text += f"... та ще {len(group_ids) - 10} груп\n"
    confirmation_text += "\nПідтвердити приєднання?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Підтвердити", callback_data="confirm_join_groups")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="join_groups")]
    ])
    
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)
    else:
        await message_or_callback.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "confirm_join_groups")
async def confirm_join_groups_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження приєднання до груп"""
    data = await state.get_data()
    account_phone = data['selected_account']
    group_ids = data['group_ids']
    interval = data['interval']
    
    await callback.message.answer("🚀 Запуск приєднання до груп...")
    
    # Запускаємо процес приєднання в фоновому режимі
    asyncio.create_task(join_groups_process(account_phone, group_ids, interval, callback.message))
    
    await state.clear()
    await callback.answer()

async def join_groups_process(account_phone: str, group_ids: list, interval: int, message_obj):
    """Функція для приєднання до груп через Telethon"""
    try:
        # Отримуємо дані аккаунта
        accounts = db.get_accounts()
        account = None
        for acc in accounts:
            if acc['phone_number'] == account_phone:
                account = acc
                break
        
        if not account:
            await message_obj.answer(f"❌ Аккаунт {account_phone} не знайдено")
            return
        
        # Створюємо клієнт
        session_name = f"sessions/temp_{account_phone.replace('+', '').replace('-', '')}"
        client = TelegramClient(session_name, account['api_id'], account['api_hash'])
        
        await client.connect()
        
        if not await client.is_user_authorized():
            await message_obj.answer(f"❌ Аккаунт {account_phone} не авторизований")
            await client.disconnect()
            return
        
        # Створюємо пакет груп з назвою = номер телефону
        package_name = account_phone
        package_id = db.create_group_package(package_name, account_phone)
        
        if not package_id:
            await message_obj.answer("❌ Помилка при створенні пакету груп")
            await client.disconnect()
            return
        
        joined_count = 0
        failed_count = 0
        already_joined = 0
        
        for i, group_input in enumerate(group_ids, 1):
            try:
                # Обробляємо різні формати груп
                group_entity = None
                group_name = f"Група {group_input}"
                group_username = None
                full_group_id = None
                
                try:
                    if group_input.isdigit():
                        # Звичайний ID групи
                        if not group_input.startswith('-100'):
                            full_group_id = f"-100{group_input}"
                        else:
                            full_group_id = group_input
                        
                        # Перевіряємо чи група вже існує в базі
                        existing_groups = db.get_groups_for_account(account_phone)
                        group_exists = any(g['group_id'] == full_group_id for g in existing_groups)
                        
                        if group_exists:
                            already_joined += 1
                            await message_obj.answer(f"ℹ️ Група {group_input} вже додана до аккаунта")
                            continue
                        
                        # Отримуємо entity групи
                        group_entity = await client.get_entity(int(full_group_id))
                        
                    elif group_input.startswith('@'):
                        # Username
                        username = group_input[1:]  # Видаляємо @
                        group_entity = await client.get_entity(username)
                        group_username = group_input
                        
                        # Перевіряємо чи група вже існує в базі
                        existing_groups = db.get_groups_for_account(account_phone)
                        group_exists = any(g['group_username'] == group_username for g in existing_groups)
                        
                        if group_exists:
                            already_joined += 1
                            await message_obj.answer(f"ℹ️ Група {group_input} вже додана до аккаунта")
                            continue
                        
                    elif group_input.startswith('https://t.me/'):
                        # Посилання
                        if '/joinchat/' in group_input:
                            # Invite посилання
                            invite_hash = group_input.split('/joinchat/')[-1]
                            try:
                                # Використовуємо ImportChatInviteRequest для invite посилань
                                updates = await client(ImportChatInviteRequest(invite_hash))
                                # Отримуємо інформацію з updates
                                if hasattr(updates, 'chats') and updates.chats:
                                    group_entity = updates.chats[0]
                                else:
                                    raise Exception("Не вдалося отримати інформацію про групу з invite посилання")
                            except Exception as invite_error:
                                failed_count += 1
                                await message_obj.answer(f"❌ Помилка приєднання через invite посилання {group_input}: {str(invite_error)[:100]}")
                                continue
                        else:
                            # Звичайне посилання
                            username = group_input.replace('https://t.me/', '')
                            if username.startswith('@'):
                                username = username[1:]
                            group_entity = await client.get_entity(username)
                            group_username = f"@{username}"
                            
                            # Перевіряємо чи група вже існує в базі
                            existing_groups = db.get_groups_for_account(account_phone)
                            group_exists = any(g['group_username'] == group_username for g in existing_groups)
                            
                            if group_exists:
                                already_joined += 1
                                await message_obj.answer(f"ℹ️ Група {group_input} вже додана до аккаунта")
                                continue
                    
                    if group_entity:
                        # Намагаємося приєднатися до групи
                        try:
                            if hasattr(group_entity, 'id'):
                                # Для звичайних груп/каналів
                                await client(JoinChannelRequest(group_entity))
                                full_group_id = str(group_entity.id)
                            else:
                                # Для invite посилань - вже приєдналися вище
                                full_group_id = str(group_entity.id)
                            
                            # Отримуємо інформацію про групу
                            group_name = getattr(group_entity, 'title', f'Група {group_input}')
                            if not group_username:
                                group_username = getattr(group_entity, 'username', None)
                                if group_username and not group_username.startswith('@'):
                                    group_username = f"@{group_username}"
                            
                            # Додаємо групу до бази даних
                            db.add_group(group_name, full_group_id, group_username, account_phone, package_id)
                            
                            joined_count += 1
                            await message_obj.answer(f"✅ Приєднано до групи {group_name} ({group_input})")
                            
                        except Exception as join_error:
                            failed_count += 1
                            await message_obj.answer(f"❌ Помилка приєднання до групи {group_input}: {str(join_error)[:100]}")
                    else:
                        failed_count += 1
                        await message_obj.answer(f"❌ Не вдалося знайти групу {group_input}")
                
                except Exception as entity_error:
                    failed_count += 1
                    await message_obj.answer(f"❌ Помилка обробки групи {group_input}: {str(entity_error)[:100]}")
                
                # Затримка між приєднаннями
                if i < len(group_ids):  # Не чекаємо після останньої групи
                    await asyncio.sleep(interval)
                    
            except Exception as e:
                failed_count += 1
                await message_obj.answer(f"❌ Критична помилка обробки групи {group_input}: {str(e)[:100]}")
        
        # Показуємо підсумок
        summary_text = f"📊 <b>Підсумок приєднання до груп:</b>\n\n"
        summary_text += f"📱 <b>Аккаунт:</b> {account_phone}\n"
        summary_text += f"✅ <b>Успішно приєднано:</b> {joined_count}\n"
        summary_text += f"❌ <b>Помилок:</b> {failed_count}\n"
        summary_text += f"ℹ️ <b>Вже були додані:</b> {already_joined}\n"
        summary_text += f"📦 <b>Пакет створено:</b> {package_name}"
        
        await message_obj.answer(summary_text, parse_mode='HTML')
        
        await client.disconnect()
        
    except Exception as e:
        await message_obj.answer(f"❌ Критична помилка при приєднанні до груп: {str(e)[:200]}")
        logger.error(f"❌ Помилка при приєднанні до груп: {e}")

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