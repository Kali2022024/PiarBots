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
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberInvalidError, PhoneCodeExpiredError, FloodWaitError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from dotenv import load_dotenv
from config import TELEGRAM_BOT_TOKEN
import random
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
            
            # Додаємо міграцію для нових колонок інтервалів між повідомленнями
            try:
                cursor.execute("ALTER TABLE mass_broadcast_settings ADD COLUMN message_interval_seconds INTEGER DEFAULT 10")
                logger.info("✅ Додано колонку message_interval_seconds до таблиці mass_broadcast_settings")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logger.info("ℹ️ Колонка message_interval_seconds вже існує в таблиці mass_broadcast_settings")
                else:
                    logger.error(f"❌ Помилка при додаванні колонки message_interval_seconds: {e}")
            
            try:
                cursor.execute("ALTER TABLE mass_broadcast_settings ADD COLUMN use_random_message_interval BOOLEAN DEFAULT 0")
                logger.info("✅ Додано колонку use_random_message_interval до таблиці mass_broadcast_settings")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logger.info("ℹ️ Колонка use_random_message_interval вже існує в таблиці mass_broadcast_settings")
                else:
                    logger.error(f"❌ Помилка при додаванні колонки use_random_message_interval: {e}")
            
            try:
                cursor.execute("ALTER TABLE mass_broadcast_settings ADD COLUMN min_message_interval_seconds INTEGER DEFAULT 5")
                logger.info("✅ Додано колонку min_message_interval_seconds до таблиці mass_broadcast_settings")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logger.info("ℹ️ Колонка min_message_interval_seconds вже існує в таблиці mass_broadcast_settings")
                else:
                    logger.error(f"❌ Помилка при додаванні колонки min_message_interval_seconds: {e}")
            
            try:
                cursor.execute("ALTER TABLE mass_broadcast_settings ADD COLUMN max_message_interval_seconds INTEGER DEFAULT 30")
                logger.info("✅ Додано колонку max_message_interval_seconds до таблиці mass_broadcast_settings")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logger.info("ℹ️ Колонка max_message_interval_seconds вже існує в таблиці mass_broadcast_settings")
                else:
                    logger.error(f"❌ Помилка при додаванні колонки max_message_interval_seconds: {e}")
            
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

    async def send_message_with_retry(self, client, group_id: str, group_name: str, message_data, message_obj=None, max_retries: int = 3) -> bool:
        """Відправити повідомлення з retry логікою та перевіркою існування групи"""
        import time
        import random
        
        for attempt in range(max_retries):
            try:
                # Перевіряємо, чи існує група
                try:
                    entity = await client.get_entity(int(group_id))
                    logger.info(f"✅ Група {group_name} ({group_id}) знайдена, відправляємо повідомлення...")
                except Exception as e:
                    logger.warning(f"⚠️ Група {group_name} ({group_id}) не знайдена: {e}")
                    return False
                
                # Випадкова пауза перед відправкою
                await add_random_pause()
                
                # Імітуємо друк
                await simulate_typing(client, entity)
                
                # Обробляємо різні типи повідомлень
                if isinstance(message_data, str):
                    # Старий формат - просто текст
                    enhanced_message = add_random_emoji_to_text(message_data)
                    
                    # Визначаємо чи відправляти стикер або текст
                    if should_send_sticker() and RANDOM_STICKERS:
                        try:
                            # Відправляємо випадковий стикер
                            sticker_id = random.choice(RANDOM_STICKERS)
                            await client.send_file(entity, sticker_id)
                            logger.info(f"✅ Стикер успішно відправлено в групу {group_name} ({group_id})")
                        except Exception as sticker_error:
                            logger.warning(f"⚠️ Не вдалося відправити стикер, відправляємо текст: {sticker_error}")
                            # Якщо стикер не вдався, відправляємо текст
                            await client.send_message(entity, enhanced_message)
                            logger.info(f"✅ Повідомлення успішно відправлено в групу {group_name} ({group_id})")
                    else:
                        # Відправляємо текст з емоціями
                        await client.send_message(entity, enhanced_message)
                        logger.info(f"✅ Повідомлення успішно відправлено в групу {group_name} ({group_id})")
                
                elif isinstance(message_data, dict):
                    # Новий формат - структуроване повідомлення
                    message_type = message_data.get('type', 'text')
                    text = message_data.get('text')
                    file_path = message_data.get('file_path')
                    
                    if message_type == 'text':
                        # Текстове повідомлення
                        enhanced_message = add_random_emoji_to_text(text) if text else ""
                        
                        if should_send_sticker() and RANDOM_STICKERS:
                            try:
                                sticker_id = random.choice(RANDOM_STICKERS)
                                await client.send_file(entity, sticker_id)
                                logger.info(f"✅ Стикер успішно відправлено в групу {group_name} ({group_id})")
                            except Exception as sticker_error:
                                logger.warning(f"⚠️ Не вдалося відправити стикер, відправляємо текст: {sticker_error}")
                                await client.send_message(entity, enhanced_message)
                                logger.info(f"✅ Повідомлення успішно відправлено в групу {group_name} ({group_id})")
                        else:
                            await client.send_message(entity, enhanced_message)
                            logger.info(f"✅ Повідомлення успішно відправлено в групу {group_name} ({group_id})")
                    
                    elif message_type in ['photo', 'video', 'audio', 'document'] and file_path:
                        # Медіа-повідомлення
                        import os
                        if os.path.exists(file_path):
                            # Підготовка підпису
                            caption = text
                            if caption and should_add_emoji_to_caption():
                                caption = add_random_emoji_to_text(caption)
                            
                            # Відправка медіа
                            if message_type == 'photo':
                                await client.send_file(entity, file_path, caption=caption)
                            elif message_type == 'video':
                                await client.send_file(entity, file_path, caption=caption, video_note=False)
                            elif message_type == 'audio':
                                await client.send_file(entity, file_path, caption=caption, voice_note=False)
                            else:  # document
                                await client.send_file(entity, file_path, caption=caption)
                            
                            logger.info(f"✅ {message_type.capitalize()} успішно відправлено в групу {group_name} ({group_id})")
                            
                            # Додатково відправляємо стикер якщо потрібно
                            if should_send_sticker_with_media() and RANDOM_STICKERS:
                                try:
                                    await asyncio.sleep(random.uniform(1.0, 3.0))  # Пауза перед стикером
                                    sticker_id = random.choice(RANDOM_STICKERS)
                                    await client.send_file(entity, sticker_id)
                                    logger.info(f"✅ Додатковий стикер відправлено в групу {group_name} ({group_id})")
                                except Exception as sticker_error:
                                    logger.warning(f"⚠️ Не вдалося відправити додатковий стикер: {sticker_error}")
                        else:
                            logger.error(f"❌ Файл {file_path} не існує")
                            return False
                    else:
                        logger.error(f"❌ Неправильний формат повідомлення: {message_data}")
                        return False
                
                return True
                
            except FloodWaitError as flood_error:
                # Обробка FloodWaitError
                random_time = random.randint(10, 50)
                wait_time = flood_error.seconds
                total_wait = wait_time + random_time
                logger.warning(f"⏳ FloodWait: чекаємо {total_wait} секунд, Flood wait: {wait_time}, Random time: {random_time}")
                
                # Відправляємо повідомлення в чат про FloodWait
                try:
                    await message_obj.answer(f"⏳ <b>FloodWait Error!</b>\n\n"
                                           f"🕐 <b>Чекаємо:</b> {total_wait} секунд\n"
                                           f"📊 <b>Flood wait:</b> {wait_time} сек\n"
                                           f"🎲 <b>Random time:</b> {random_time} сек\n"
                                           f"📝 <b>Група:</b> {group_name}\n\n"
                                           f"⏰ <b>Продовжимо через:</b> {total_wait} сек",
                                           parse_mode='HTML')
                except:
                    pass  # Якщо не вдалося відправити повідомлення в чат
                
                await asyncio.sleep(total_wait)
                continue
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
                elif "CHAT_SEND_PHOTOS_FORBIDDEN" in error_msg:
                    logger.warning(f"⚠️ Відправка фото заборонена в групі {group_name} ({group_id})")
                    return False
                elif "CHAT_SEND_MEDIA_FORBIDDEN" in error_msg:
                    logger.warning(f"⚠️ Відправка медіа заборонена в групі {group_name} ({group_id})")
                    return False
                elif "CHAT_SEND_VIDEOS_FORBIDDEN" in error_msg:
                    logger.warning(f"⚠️ Відправка відео заборонена в групі {group_name} ({group_id})")
                    return False
                elif "CHAT_SEND_AUDIOS_FORBIDDEN" in error_msg:
                    logger.warning(f"⚠️ Відправка аудіо заборонена в групі {group_name} ({group_id})")
                    return False
                elif any(media_error in error_msg for media_error in ["CHAT_SEND_PHOTOS_FORBIDDEN", "CHAT_SEND_MEDIA_FORBIDDEN", "CHAT_SEND_VIDEOS_FORBIDDEN", "CHAT_SEND_AUDIOS_FORBIDDEN"]):
                    # Якщо медіа заборонено, спробуємо відправити текст як fallback
                    if isinstance(message_data, dict) and message_data.get('text'):
                        logger.warning(f"⚠️ Медіа заборонено в групі {group_name} ({group_id}), відправляємо текст як fallback")
                        try:
                            enhanced_text = add_random_emoji_to_text(message_data['text'])
                            await client.send_message(entity, enhanced_text)
                            logger.info(f"✅ Текст успішно відправлено як fallback в групу {group_name} ({group_id})")
                            return True
                        except Exception as fallback_error:
                            logger.error(f"❌ Fallback також не вдався для групи {group_name} ({group_id}): {fallback_error}")
                            return False
                    else:
                        logger.warning(f"⚠️ Медіа заборонено в групі {group_name} ({group_id}) і немає тексту для fallback")
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
                        (interval_seconds, use_random_interval, min_random_seconds, max_random_seconds,
                         message_interval_seconds, use_random_message_interval, min_message_interval_seconds, max_message_interval_seconds)
                        VALUES (60, 0, 30, 120, 10, 0, 5, 30)
                    """)
                    conn.commit()
                    return {
                        'id': cursor.lastrowid,
                        'interval_seconds': 60,
                        'use_random_interval': 0,
                        'min_random_seconds': 30,
                        'max_random_seconds': 120,
                        'message_interval_seconds': 10,
                        'use_random_message_interval': 0,
                        'min_message_interval_seconds': 5,
                        'max_message_interval_seconds': 30
                    }
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні налаштувань масової розсилки: {e}")
            return {
                'interval_seconds': 60,
                'use_random_interval': 0,
                'min_random_seconds': 30,
                'max_random_seconds': 120,
                'message_interval_seconds': 10,
                'use_random_message_interval': 0,
                'min_message_interval_seconds': 5,
                'max_message_interval_seconds': 30
            }
    
    def update_mass_broadcast_settings(self, interval_seconds: int, use_random_interval: bool = False, 
                                     min_random_seconds: int = 30, max_random_seconds: int = 120,
                                     message_interval_seconds: int = 10, use_random_message_interval: bool = False,
                                     min_message_interval_seconds: int = 5, max_message_interval_seconds: int = 30) -> bool:
        """Оновити налаштування масової розсилки"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                    conn.execute("PRAGMA journal_mode=WAL")
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR REPLACE INTO mass_broadcast_settings 
                        (id, interval_seconds, use_random_interval, min_random_seconds, max_random_seconds,
                         message_interval_seconds, use_random_message_interval, min_message_interval_seconds, max_message_interval_seconds, updated_at)
                        VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (interval_seconds, use_random_interval, min_random_seconds, max_random_seconds,
                          message_interval_seconds, use_random_message_interval, min_message_interval_seconds, max_message_interval_seconds))
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
    waiting_for_random_interval_config = State()

class MassBroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_interval = State()
    waiting_for_random_settings = State()
    waiting_for_package_selection = State()
    waiting_for_message_interval_config = State()
    waiting_for_different_messages = State()
    waiting_for_account_message = State()
    waiting_for_broadcast_mode = State()
    waiting_for_message_type = State()
    waiting_for_media_file = State()
    waiting_for_media_caption = State()

class DeletePackageStates(StatesGroup):
    waiting_for_package_name = State()

# Ініціалізація бота та диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

# Ініціалізація бази даних
db = Database()

# Глобальні змінні для відстеження активних розсилок
active_broadcasts = {}  # {user_id: {'task': task, 'stop_event': event}}

# Списки емоцій та стикерів для автоматичного додавання
RANDOM_EMOJIS = [
    "😊", "😄", "😃", "😁", "😆", "😅", "😂", "🤣", "😊", "😇", "🙂", "🙃", "😉", "😌", "😍", "🥰", "😘", "😗", "😙", "😚",
    "😋", "😛", "😝", "😜", "🤪", "🤨", "🧐", "🤓", "😎", "🤩", "🥳", "😏", "😒", "😞", "😔", "😟", "😕", "🙁", "☹️", "😣",
    "😖", "😫", "😩", "🥺", "😢", "😭", "😤", "😠", "😡", "🤬", "🤯", "😳", "🥵", "🥶", "😱", "😨", "😰", "😥", "😓", "🤗",
    "🤔", "🤭", "🤫", "🤥", "😶", "😐", "😑", "😬", "🙄", "😯", "😦", "😧", "😮", "😲", "🥱", "😴", "🤤", "😪", "😵", "🤐",
    "🥴", "🤢", "🤮", "🤧", "😷", "🤒", "🤕", "🤑", "🤠", "😈", "👿", "👹", "👺", "🤡", "💩", "👻", "💀", "☠️", "👽", "👾",
    "🤖", "🎃", "😺", "😸", "😹", "😻", "😼", "😽", "🙀", "😿", "😾", "👶", "🧒", "👦", "👧", "🧑", "👨", "👩", "🧓", "👴",
    "👵", "👱", "👨‍🦰", "👩‍🦰", "👨‍🦱", "👩‍🦱", "👨‍🦳", "👩‍🦳", "👨‍🦲", "👩‍🦲", "🧔", "👨‍💼", "👩‍💼", "👨‍🔬", "👩‍🔬", "👨‍💻", "👩‍💻", "👨‍🎤", "👩‍🎤", "👨‍🎨", "👩‍🎨",
    "👨‍✈️", "👩‍✈️", "👨‍🚀", "👩‍🚀", "👨‍🚒", "👩‍🚒", "👮", "👮‍♂️", "👮‍♀️", "🕵️", "🕵️‍♂️", "🕵️‍♀️", "💂", "💂‍♂️", "💂‍♀️", "🥷", "👷", "👷‍♂️", "👷‍♀️", "🤴",
    "👸", "👳", "👳‍♂️", "👳‍♀️", "👲", "🧕", "🤵", "🤵‍♂️", "🤵‍♀️", "👰", "👰‍♂️", "👰‍♀️", "🤰", "🤱", "👼", "🎅", "🤶", "🦸", "🦸‍♂️", "🦸‍♀️",
    "🦹", "🦹‍♂️", "🦹‍♀️", "🧙", "🧙‍♂️", "🧙‍♀️", "🧚", "🧚‍♂️", "🧚‍♀️", "🧛", "🧛‍♂️", "🧛‍♀️", "🧜", "🧜‍♂️", "🧜‍♀️", "🧝", "🧝‍♂️", "🧝‍♀️", "🧞", "🧞‍♂️", "🧞‍♀️",
    "🧟", "🧟‍♂️", "🧟‍♀️", "💆", "💆‍♂️", "💆‍♀️", "💇", "💇‍♂️", "💇‍♀️", "🚶", "🚶‍♂️", "🚶‍♀️", "🧍", "🧍‍♂️", "🧍‍♀️", "🧎", "🧎‍♂️", "🧎‍♀️", "🏃", "🏃‍♂️", "🏃‍♀️",
    "💃", "🕺", "🕴️", "👯", "👯‍♂️", "👯‍♀️", "🧖", "🧖‍♂️", "🧖‍♀️", "🧗", "🧗‍♂️", "🧗‍♀️", "🤺", "🏇", "⛷️", "🏂", "🏌️", "🏌️‍♂️", "🏌️‍♀️", "🏄",
    "🏄‍♂️", "🏄‍♀️", "🚣", "🚣‍♂️", "🚣‍♀️", "🏊", "🏊‍♂️", "🏊‍♀️", "⛹️", "⛹️‍♂️", "⛹️‍♀️", "🏋️", "🏋️‍♂️", "🏋️‍♀️", "🚴", "🚴‍♂️", "🚴‍♀️", "🚵", "🚵‍♂️", "🚵‍♀️", "🤸",
    "🤸‍♂️", "🤸‍♀️", "🤼", "🤼‍♂️", "🤼‍♀️", "🤽", "🤽‍♂️", "🤽‍♀️", "🤾", "🤾‍♂️", "🤾‍♀️", "🤹", "🤹‍♂️", "🤹‍♀️", "🧘", "🧘‍♂️", "🧘‍♀️", "🛀", "🛌", "👭", "👫", "👬",
    "💏", "💑", "👪", "🗣️", "👤", "👥", "🫂", "👋", "🤚", "🖐️", "✋", "🖖", "👌", "🤏", "✌️", "🤞", "🫰", "🤟", "🤘", "🤙",
    "👈", "👉", "👆", "🖕", "👇", "☝️", "🫵", "👍", "👎", "✊", "👊", "🤛", "🤜", "👏", "🙌", "👐", "🤲", "🤝", "🙏", "✍️",
    "💅", "🤳", "💪", "🦾", "🦿", "🦵", "🦶", "👂", "🦻", "👃", "🧠", "🦷", "🦴", "👀", "👁️", "👅", "👄", "💋", "🩸", "💀", "🦴"
]

# Стикери (ID стикерів Telegram)
RANDOM_STICKERS = [
    "CAACAgIAAxkBAAIBY2Y8X8K8X8K8X8K8X8K8X8K8X8K8",  # Приклад ID стикера
    "CAACAgIAAxkBAAIBZGY8X8K8X8K8X8K8X8K8X8K8X8K8",  # Потрібно замінити на реальні ID
    "CAACAgIAAxkBAAIBZWY8X8K8X8K8X8K8X8K8X8K8X8K8",  # стикерів з вашого бота
]

# Перевірка конфігурації
if not API_ID or not API_HASH:
    logger.error("❌ Помилка: API_ID або API_HASH не встановлено!")
    logger.error("Створіть .env файл з правильними даними")
    exit(1)

# ========== ФУНКЦІЇ ДЛЯ ЕМОЦІЙ ТА ІМІТАЦІЇ ==========

def add_random_emoji_to_text(text: str) -> str:
    """Додає випадкові емоції до тексту"""
    import random
    
    # Випадково вибираємо кількість емоцій (1-3)
    num_emojis = random.randint(1, 3)
    
    # Випадково вибираємо позиції для емоцій
    positions = random.sample(range(len(text) + 1), min(num_emojis, len(text) + 1))
    positions.sort()
    
    # Додаємо емоції
    result = text
    for i, pos in enumerate(positions):
        emoji = random.choice(RANDOM_EMOJIS)
        result = result[:pos + i] + emoji + result[pos + i:]
    
    return result

async def simulate_typing(client, entity, duration: int = None):
    """Імітує статус 'печатает...'"""
    import random
    
    if duration is None:
        duration = random.randint(2, 5)  # 2-5 секунд
    
    try:
        await client.send_read_acknowledge(entity)
        # Відправляємо статус "печатает"
        action = client.action(entity, 'typing')
        await action
        await asyncio.sleep(duration)
    except Exception as e:
        logger.warning(f"⚠️ Не вдалося імітувати друк: {e}")

async def add_random_pause():
    """Додає випадкову паузу для імітації реального користувача"""
    import random
    
    # Випадкова пауза від 1 до 3 секунд
    pause_time = random.uniform(1.0, 3.0)
    await asyncio.sleep(pause_time)

def should_send_sticker() -> bool:
    """Визначає чи потрібно відправити стикер замість тексту"""
    import random
    
    # 10% шанс відправити стикер
    return random.random() < 0.1

def get_media_type_from_file(file_path: str) -> str:
    """Визначає тип медіа-файлу за розширенням"""
    import os
    
    extension = os.path.splitext(file_path.lower())[1]
    
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
    video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv']
    audio_extensions = ['.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac']
    
    if extension in image_extensions:
        return 'photo'
    elif extension in video_extensions:
        return 'video'
    elif extension in audio_extensions:
        return 'audio'
    else:
        return 'document'

async def download_media_file(bot, file_id: str, file_path: str) -> bool:
    """Завантажує медіа-файл з Telegram"""
    try:
        file = await bot.get_file(file_id)
        await bot.download_file(file.file_path, file_path)
        return True
    except Exception as e:
        logger.error(f"❌ Помилка завантаження файлу {file_id}: {e}")
        return False

def should_add_emoji_to_caption() -> bool:
    """Визначає чи додавати емоції до підпису медіа"""
    import random
    
    # 70% шанс додати емоції до підпису
    return random.random() < 0.7

def should_send_sticker_with_media() -> bool:
    """Визначає чи відправляти стикер разом з медіа"""
    import random
    
    # 15% шанс відправити стикер разом з медіа
    return random.random() < 0.15

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

async def send_broadcast_message(account_phone: str, message_text: str, groups: list, status_id: int, message_obj=None):
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
                    message_text,
                    message_obj
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
        f"📋 Введіть список ID груп через кому (наприклад: 2105953426,2064362674,2133142559):",
        parse_mode='HTML'
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
    
    # Додаємо басейни для всіх аккаунтів
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
    
    # Отримуємо басейни з пакету
    selected_groups = db.get_groups_by_package(package_id)
    
    if not selected_groups:
        await callback.message.answer("❌ Помилка при виборі пакету груп.")
        await callback.answer()
        return
    
    # Зберігаємо вибрані басейни
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
        asyncio.create_task(send_broadcast_message(account_phone, message_text, selected_groups, status_id, callback.message))
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
    settings_text += "📝 Введіть текст повідомлення або надішліть медіа-файл для масової розсилки:\n"
    settings_text += "Для того щоб обрати різні повідомлення для аккаунтів, натисніть на кнопку 📝 Різні повідомлення для аккаунтів"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Різні повідомлення для аккаунтів", callback_data="mass_different_messages")]
    ])


    await callback.message.answer(settings_text, parse_mode='HTML',reply_markup=keyboard)
    await state.set_state(MassBroadcastStates.waiting_for_message)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_message)
async def process_mass_broadcast_message(message: Message, state: FSMContext):
    """Обробка повідомлення для масової розсилки (текст або медіа)"""
    
    # Перевіряємо чи це медіа-файл
    if message.photo or message.video or message.audio or message.document:
        # Це медіа-файл
        await process_mass_media_file(message, state)
    else:
        # Це текст
        message_text = message.text.strip()
        
        if not message_text:
            await message.answer("❌ Текст повідомлення не може бути порожнім. Спробуйте ще раз:")
            return
        
        # Зберігаємо текст повідомлення
        await state.update_data(message_text=message_text)
        
        # Показуємо кнопки для вибору типу розсилки
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Різні повідомлення для аккаунтів", callback_data="mass_different_messages")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
        ])
        
        await message.answer(
            f"📝 <b>Повідомлення:</b> {message_text[:100]}{'...' if len(message_text) > 100 else ''}\n\n"
            f"Оберіть тип розсилки:",
            parse_mode='HTML',
            reply_markup=keyboard
        )

async def process_mass_media_file(message: Message, state: FSMContext):
    """Обробка медіа-файлу для масової розсилки"""
    import os
    
    # Створюємо папку для медіа-файлів
    media_dir = "media_files"
    if not os.path.exists(media_dir):
        os.makedirs(media_dir)
    
    file_id = None
    file_path = None
    message_type = None
    
    # Визначаємо тип медіа та отримуємо file_id
    if message.photo:
        file_id = message.photo[-1].file_id
        message_type = "photo"
        file_path = f"{media_dir}/mass_photo_{file_id}.jpg"
    elif message.video:
        file_id = message.video.file_id
        message_type = "video"
        file_path = f"{media_dir}/mass_video_{file_id}.mp4"
    elif message.audio:
        file_id = message.audio.file_id
        message_type = "audio"
        file_path = f"{media_dir}/mass_audio_{file_id}.mp3"
    elif message.document:
        file_id = message.document.file_id
        message_type = "document"
        file_path = f"{media_dir}/mass_document_{file_id}"
    
    if not file_id:
        await message.answer("❌ Неправильний тип файлу. Спробуйте ще раз.")
        return
    
    # Завантажуємо файл
    success = await download_media_file(bot, file_id, file_path)
    if not success:
        await message.answer("❌ Помилка завантаження файлу. Спробуйте ще раз.")
        return
    
    # Зберігаємо інформацію про медіа
    await state.update_data(
        message_type=message_type,
        media_file_path=file_path,
        media_file_id=file_id
    )
    
    # Показуємо опції для медіа
    media_type_names = {
        "photo": "🖼️ фото",
        "audio": "🎵 аудіо",
        "video": "🎬 відео",
        "document": "📄 документ"
    }
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📎 Без підпису", callback_data="mass_media_no_caption")],
        [InlineKeyboardButton(text="📝 З підписом", callback_data="mass_media_with_caption")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
    ])
    
    await message.answer(
        f"📎 <b>Завантажено {media_type_names[message_type]} для масової розсилки!</b>\n\n"
        f"📁 <b>Файл:</b> {os.path.basename(file_path)}\n\n"
        f"Оберіть опцію:",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@router.callback_query(lambda c: c.data in ["mass_media_no_caption", "mass_media_with_caption"])
async def process_mass_media_caption_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору підпису для медіа в загальній розсилці"""
    has_caption = callback.data == "mass_media_with_caption"
    data = await state.get_data()
    message_type = data.get('message_type')
    
    # Зберігаємо інформацію про підпис
    await state.update_data(has_caption=has_caption)
    
    media_type_names = {
        "photo": "🖼️ фото",
        "audio": "🎵 аудіо", 
        "video": "🎬 відео",
        "document": "📄 документ"
    }
    
    if has_caption:
        await callback.message.answer(
            f"📝 <b>Введіть підпис для {media_type_names[message_type]}:</b>\n\n"
            f"📎 Цей підпис буде додано до всіх медіа-файлів у розсилці",
            parse_mode='HTML'
        )
        await state.set_state(MassBroadcastStates.waiting_for_media_caption)
    else:
        # Без підпису - переходимо до налаштування інтервалів
        await show_interval_settings(callback, state)
    
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_media_caption)
async def process_mass_media_caption(message: Message, state: FSMContext):
    """Обробка введення підпису для медіа в загальній розсилці"""
    caption = message.text.strip()
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    file_path = data.get('media_file_path')
    
    if not caption:
        await message.answer("❌ Підпис не може бути порожнім. Спробуйте ще раз:")
        return
    
    # Зберігаємо повідомлення з підписом для конкретного акаунта
    await save_account_message(state, phone, message_type, file_path, caption)
    await show_remaining_accounts(message, state)

@router.callback_query(lambda c: c.data == "mass_same_message")
async def process_mass_same_message_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору однакового повідомлення для всіх"""
    await show_interval_settings(callback, state)

@router.callback_query(lambda c: c.data == "mass_different_messages")
async def process_mass_different_messages_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору різних повідомлень для аккаунтів"""
    # Отримуємо всі аккаунти
    accounts = db.get_accounts()
    
    if not accounts:
        await callback.message.answer("❌ Немає зареєстрованих аккаунтів.")
        await callback.answer()
        return
    
    # Створюємо клавіатуру з номерами телефонів
    keyboard_buttons = []
    for account in accounts:
        phone = account['phone_number']
        button_text = f"📱 {phone}"
        keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"mass_account_message_{phone}")])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await callback.message.answer(
        "📝 <b>Налаштування різних повідомлень для аккаунтів:</b>\n\n"
        "Оберіть аккаунт для налаштування повідомлення:",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("mass_account_message_"))
async def process_mass_account_message_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору аккаунта для налаштування повідомлення"""
    phone = callback.data.replace("mass_account_message_", "")
    
    # Зберігаємо вибраний аккаунт
    await state.update_data(selected_account_for_message=phone)
    
    # Показуємо вибір типу повідомлення
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Текстове повідомлення", callback_data="message_type_text")],
        [InlineKeyboardButton(text="🖼️ Фото", callback_data="message_type_photo")],
        [InlineKeyboardButton(text="🎵 Аудіо", callback_data="message_type_audio")],
        [InlineKeyboardButton(text="🎬 Відео", callback_data="message_type_video")],
        [InlineKeyboardButton(text="📄 Документ", callback_data="message_type_document")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="mass_different_messages")]
    ])
    
    await callback.message.answer(
        f"📝 <b>Налаштування повідомлення для аккаунта {phone}:</b>\n\n"
        f"Оберіть тип повідомлення:",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await state.set_state(MassBroadcastStates.waiting_for_message_type)
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("message_type_"))
async def process_message_type_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору типу повідомлення"""
    message_type = callback.data.replace("message_type_", "")
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    
    # Зберігаємо тип повідомлення
    await state.update_data(message_type=message_type)
    
    if message_type == "text":
        # Для текстового повідомлення
        await callback.message.answer(
            f"📝 <b>Введіть текст повідомлення для аккаунта {phone}:</b>",
            parse_mode='HTML'
        )
        await state.set_state(MassBroadcastStates.waiting_for_account_message)
    else:
        # Для медіа-повідомлень
        media_type_names = {
            "photo": "🖼️ фото",
            "audio": "🎵 аудіо",
            "video": "🎬 відео",
            "document": "📄 документ"
        }
        
        # Показуємо опції для медіа
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📎 Без підпису", callback_data="media_no_caption")],
            [InlineKeyboardButton(text="📝 З підписом", callback_data="media_with_caption")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"mass_account_message_{phone}")]
        ])
        
        await callback.message.answer(
            f"📎 <b>Завантажте {media_type_names[message_type]} для аккаунта {phone}:</b>\n\n"
            f"Оберіть опцію:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        await state.set_state(MassBroadcastStates.waiting_for_media_file)
    
    await callback.answer()

@router.callback_query(lambda c: c.data in ["media_no_caption", "media_with_caption"])
async def process_media_caption_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору підпису для медіа"""
    has_caption = callback.data == "media_with_caption"
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    
    # Зберігаємо інформацію про підпис
    await state.update_data(has_caption=has_caption)
    
    media_type_names = {
        "photo": "🖼️ фото",
        "audio": "🎵 аудіо", 
        "video": "🎬 відео",
        "document": "📄 документ"
    }
    
    if has_caption:
        await callback.message.answer(
            f"📎 <b>Завантажте {media_type_names[message_type]} для аккаунта {phone}:</b>\n\n"
            f"📝 Після завантаження файлу введіть підпис:",
            parse_mode='HTML'
        )
    else:
        await callback.message.answer(
            f"📎 <b>Завантажте {media_type_names[message_type]} для аккаунта {phone}:</b>\n\n"
            f"📎 Файл буде відправлено без підпису:",
            parse_mode='HTML'
        )
    
    await state.set_state(MassBroadcastStates.waiting_for_media_file)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_media_file)
async def process_media_file(message: Message, state: FSMContext):
    """Обробка завантаження медіа-файлу"""
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    has_caption = data.get('has_caption', False)
    
    # Створюємо папку для медіа-файлів
    import os
    media_dir = "media_files"
    if not os.path.exists(media_dir):
        os.makedirs(media_dir)
    
    file_id = None
    file_path = None
    
    # Отримуємо file_id залежно від типу медіа
    if message_type == "photo" and message.photo:
        file_id = message.photo[-1].file_id
        file_path = f"{media_dir}/{phone}_photo_{file_id}.jpg"
    elif message_type == "audio" and message.audio:
        file_id = message.audio.file_id
        file_path = f"{media_dir}/{phone}_audio_{file_id}.mp3"
    elif message_type == "video" and message.video:
        file_id = message.video.file_id
        file_path = f"{media_dir}/{phone}_video_{file_id}.mp4"
    elif message_type == "document" and message.document:
        file_id = message.document.file_id
        file_path = f"{media_dir}/{phone}_document_{file_id}"
    
    if not file_id:
        await message.answer("❌ Неправильний тип файлу. Спробуйте ще раз.")
        return
    
    # Завантажуємо файл
    success = await download_media_file(bot, file_id, file_path)
    if not success:
        await message.answer("❌ Помилка завантаження файлу. Спробуйте ще раз.")
        return
    
    # Зберігаємо інформацію про файл
    await state.update_data(
        media_file_path=file_path,
        media_file_id=file_id
    )
    
    if has_caption:
        # Запитуємо підпис
        await message.answer(
            f"📝 <b>Введіть підпис для медіа-файлу:</b>\n\n"
            f"📱 Аккаунт: {phone}\n"
            f"📎 Файл: {os.path.basename(file_path)}",
            parse_mode='HTML'
        )
        await state.set_state(MassBroadcastStates.waiting_for_media_caption)
    else:
        # Зберігаємо повідомлення без підпису
        await save_account_message(state, phone, message_type, file_path, None)
        await show_remaining_accounts(message, state)

@router.message(MassBroadcastStates.waiting_for_media_caption)
async def process_media_caption(message: Message, state: FSMContext):
    """Обробка введення підпису для медіа"""
    caption = message.text.strip()
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    file_path = data.get('media_file_path')
    
    # Зберігаємо повідомлення з підписом
    await save_account_message(state, phone, message_type, file_path, caption)
    await show_remaining_accounts(message, state)

async def save_account_message(state: FSMContext, phone: str, message_type: str, file_path: str = None, text: str = None):
    """Зберігає повідомлення для аккаунта"""
    data = await state.get_data()
    account_messages = data.get('account_messages', {})
    
    # Створюємо структуру повідомлення
    message_data = {
        'type': message_type,
        'text': text,
        'file_path': file_path
    }
    
    account_messages[phone] = message_data
    await state.update_data(account_messages=account_messages)

@router.callback_query(lambda c: c.data.startswith("mass_account_message_"))
async def process_mass_account_message_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору аккаунта для налаштування повідомлення"""
    phone = callback.data.replace("mass_account_message_", "")
    
    # Зберігаємо вибраний аккаунт
    await state.update_data(selected_account_for_message=phone)
    
    # Показуємо вибір типу повідомлення
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Текстове повідомлення", callback_data="message_type_text")],
        [InlineKeyboardButton(text="🖼️ Фото", callback_data="message_type_photo")],
        [InlineKeyboardButton(text="🎵 Аудіо", callback_data="message_type_audio")],
        [InlineKeyboardButton(text="🎬 Відео", callback_data="message_type_video")],
        [InlineKeyboardButton(text="📄 Документ", callback_data="message_type_document")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="mass_different_messages")]
    ])
    
    await callback.message.answer(
        f"📝 <b>Налаштування повідомлення для аккаунта {phone}:</b>\n\n"
        f"Оберіть тип повідомлення:",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await state.set_state(MassBroadcastStates.waiting_for_message_type)
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("message_type_"))
async def process_message_type_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору типу повідомлення"""
    message_type = callback.data.replace("message_type_", "")
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    
    # Зберігаємо тип повідомлення
    await state.update_data(message_type=message_type)
    
    if message_type == "text":
        # Для текстового повідомлення
        await callback.message.answer(
            f"📝 <b>Введіть текст повідомлення для аккаунта {phone}:</b>",
            parse_mode='HTML'
        )
        await state.set_state(MassBroadcastStates.waiting_for_account_message)
    else:
        # Для медіа-повідомлень
        media_type_names = {
            "photo": "🖼️ фото",
            "audio": "🎵 аудіо",
            "video": "🎬 відео",
            "document": "📄 документ"
        }
        
        # Показуємо опції для медіа
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📎 Без підпису", callback_data="media_no_caption")],
            [InlineKeyboardButton(text="📝 З підписом", callback_data="media_with_caption")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"mass_account_message_{phone}")]
        ])
        
        await callback.message.answer(
            f"📎 <b>Завантажте {media_type_names[message_type]} для аккаунта {phone}:</b>\n\n"
            f"Оберіть опцію:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        await state.set_state(MassBroadcastStates.waiting_for_media_file)
    
    await callback.answer()

@router.callback_query(lambda c: c.data in ["media_no_caption", "media_with_caption"])
async def process_media_caption_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору підпису для медіа"""
    has_caption = callback.data == "media_with_caption"
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    
    # Зберігаємо інформацію про підпис
    await state.update_data(has_caption=has_caption)
    
    media_type_names = {
        "photo": "🖼️ фото",
        "audio": "🎵 аудіо", 
        "video": "🎬 відео",
        "document": "📄 документ"
    }
    
    if has_caption:
        await callback.message.answer(
            f"📎 <b>Завантажте {media_type_names[message_type]} для аккаунта {phone}:</b>\n\n"
            f"📝 Після завантаження файлу введіть підпис:",
            parse_mode='HTML'
        )
    else:
        await callback.message.answer(
            f"📎 <b>Завантажте {media_type_names[message_type]} для аккаунта {phone}:</b>\n\n"
            f"📎 Файл буде відправлено без підпису:",
            parse_mode='HTML'
        )
    
    await state.set_state(MassBroadcastStates.waiting_for_media_file)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_media_file)
async def process_media_file(message: Message, state: FSMContext):
    """Обробка завантаження медіа-файлу"""
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    has_caption = data.get('has_caption', False)
    
    # Створюємо папку для медіа-файлів
    import os
    media_dir = "media_files"
    if not os.path.exists(media_dir):
        os.makedirs(media_dir)
    
    file_id = None
    file_path = None
    
    # Отримуємо file_id залежно від типу медіа
    if message_type == "photo" and message.photo:
        file_id = message.photo[-1].file_id
        file_path = f"{media_dir}/{phone}_photo_{file_id}.jpg"
    elif message_type == "audio" and message.audio:
        file_id = message.audio.file_id
        file_path = f"{media_dir}/{phone}_audio_{file_id}.mp3"
    elif message_type == "video" and message.video:
        file_id = message.video.file_id
        file_path = f"{media_dir}/{phone}_video_{file_id}.mp4"
    elif message_type == "document" and message.document:
        file_id = message.document.file_id
        file_path = f"{media_dir}/{phone}_document_{file_id}"
    
    if not file_id:
        await message.answer("❌ Неправильний тип файлу. Спробуйте ще раз.")
        return
    
    # Завантажуємо файл
    success = await download_media_file(bot, file_id, file_path)
    if not success:
        await message.answer("❌ Помилка завантаження файлу. Спробуйте ще раз.")
        return
    
    # Зберігаємо інформацію про файл
    await state.update_data(
        media_file_path=file_path,
        media_file_id=file_id
    )
    
    if has_caption:
        # Запитуємо підпис
        await message.answer(
            f"📝 <b>Введіть підпис для медіа-файлу:</b>\n\n"
            f"📱 Аккаунт: {phone}\n"
            f"📎 Файл: {os.path.basename(file_path)}",
            parse_mode='HTML'
        )
        await state.set_state(MassBroadcastStates.waiting_for_media_caption)
    else:
        # Зберігаємо повідомлення без підпису
        await save_account_message(state, phone, message_type, file_path, None)
        await show_remaining_accounts(message, state)

@router.message(MassBroadcastStates.waiting_for_media_caption)
async def process_media_caption(message: Message, state: FSMContext):
    """Обробка введення підпису для медіа"""
    caption = message.text.strip()
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    file_path = data.get('media_file_path')
    
    # Зберігаємо повідомлення з підписом
    await save_account_message(state, phone, message_type, file_path, caption)
    await show_remaining_accounts(message, state)

async def save_account_message(state: FSMContext, phone: str, message_type: str, file_path: str = None, text: str = None):
    """Зберігає повідомлення для аккаунта"""
    data = await state.get_data()
    account_messages = data.get('account_messages', {})
    
    # Створюємо структуру повідомлення
    message_data = {
        'type': message_type,
        'text': text,
        'file_path': file_path
    }
    
    account_messages[phone] = message_data
    await state.update_data(account_messages=account_messages)

async def show_remaining_accounts(message: Message, state: FSMContext):
    """Показує список аккаунтів, які ще потрібно налаштувати"""
    data = await state.get_data()
    account_messages = data.get('account_messages', {})
    
    # Отримуємо всі аккаунти
    accounts = db.get_accounts()
    all_phones = [acc['phone_number'] for acc in accounts]
    
    
    # Знаходимо аккаунти без налаштованих повідомлень
    remaining_phones = [phone for phone in all_phones if phone not in account_messages]
    
    if remaining_phones:
        # Показуємо кнопки для решти аккаунтів
        keyboard_buttons = []
        for phone in remaining_phones[:5]:  # Показуємо максимум 5 кнопок
            button_text = f"📱 {phone}"
            keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"mass_account_message_{phone}")])
        
        # Додаємо кнопку запуску розсилки якщо є налаштовані повідомлення
        if len(account_messages) > 0:
            keyboard_buttons.append([InlineKeyboardButton(text="🚀 Запустити розсилку", callback_data="start_different_messages_broadcast")])
        
        keyboard_buttons.append([InlineKeyboardButton(text="✅ Завершити налаштування", callback_data="mass_finish_messages")])
        keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        
        await message.answer(
            f"✅ <b>Повідомлення налаштовано для {len(account_messages)} аккаунтів</b>\n\n"
            f"📱 Залишилося налаштувати: {len(remaining_phones)} аккаунтів\n\n"
            f"Оберіть наступний аккаунт або запустіть розсилку:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    else:
        # Всі аккаунти налаштовані - переходимо до налаштування інтервалів
        await message.answer(
            f"✅ <b>Всі повідомлення налаштовано!</b>\n\n"
            f"📱 Налаштовано для {len(account_messages)} аккаунтів\n\n"
            f"🔄 Переходимо до налаштування інтервалів...",
            parse_mode='HTML'
        )
        await show_interval_settings(message, state)

        
@router.callback_query(lambda c: c.data == "start_different_messages_broadcast")
async def start_different_messages_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Запуск розсилки різних повідомлень"""
    data = await state.get_data()
    account_messages = data.get('account_messages', {})
    
    if not account_messages:
        await callback.message.answer("❌ Немає налаштованих повідомлень для розсилки.")
        await callback.answer()
        return
    
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
        [InlineKeyboardButton(text="🎲 Рандомний інтервал", callback_data="mass_random_interval")],
        [InlineKeyboardButton(text="📨 Налаштування інтервалів між повідомленнями", callback_data="mass_message_intervals")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="mass_different_messages")]
    ])
    
    # Показуємо підсумок налаштованих повідомлень
    summary_text = f"📝 <b>Налаштовано повідомлень для {len(account_messages)} аккаунтів:</b>\n\n"
    for phone, msg_data in account_messages.items():
        if isinstance(msg_data, dict):
            msg_type = msg_data.get('type', 'text')
            text = msg_data.get('text', '')
            if msg_type == 'text':
                summary_text += f"📱 {phone}: {text[:30]}{'...' if len(text) > 30 else ''}\n"
            else:
                summary_text += f"📱 {phone}: {msg_type} {f'({text[:20]}...)' if text else ''}\n"
        else:
            summary_text += f"📱 {phone}: {str(msg_data)[:30]}{'...' if len(str(msg_data)) > 30 else ''}\n"
    
    summary_text += f"\n⏱️ <b>Поточний інтервал:</b> {settings['interval_seconds']} секунд\n\n"
    summary_text += f"Оберіть інтервал між розсилками з різних аккаунтів:"
    
    await callback.message.answer(summary_text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_account_message)
async def process_account_message(message: Message, state: FSMContext):
    """Обробка введення повідомлення для конкретного аккаунта"""
    message_text = message.text.strip()
    
    if not message_text:
        await message.answer("❌ Текст повідомлення не може бути порожнім. Спробуйте ще раз:")
        return
    
    # Отримуємо дані зі стану
    data = await state.get_data()
    selected_account = data['selected_account_for_message']
    
    # Зберігаємо текстове повідомлення
    await save_account_message(state, selected_account, "text", None, message_text)
    
    # Показуємо решту аккаунтів
    await show_remaining_accounts(message, state)
@router.callback_query(lambda c: c.data == "mass_finish_messages")
async def process_mass_finish_messages_callback(callback: CallbackQuery, state: FSMContext):
    """Завершення налаштування повідомлень"""
    await show_interval_settings(callback, state)

async def show_interval_settings(message_or_callback, state: FSMContext):
    """Показ налаштувань інтервалів"""
    # Отримуємо дані з FSM
    data = await state.get_data()
    message_text = data.get('message_text', '')
    account_messages = data.get('account_messages', {})
    
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
        [InlineKeyboardButton(text="🎲 Рандомний інтервал", callback_data="mass_random_interval")],
        [InlineKeyboardButton(text="📨 Налаштування інтервалів між повідомленнями", callback_data="mass_message_intervals")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="mass_different_messages" if account_messages else "Mass_broadcast")]
    ])
    
    if account_messages:
        # Різні повідомлення
        message_info = f"📝 <b>Різні повідомлення для {len(account_messages)} аккаунтів</b>"
    else:
        # Однакове повідомлення
        message_info = f"📝 <b>Повідомлення:</b> {message_text[:50]}{'...' if len(message_text) > 50 else ''}"
    
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(
            f"⏱️ <b>Налаштування інтервалу між аккаунтами:</b>\n\n"
            f"{message_info}\n\n"
            f"Поточний інтервал: {settings['interval_seconds']} секунд\n\n"
            f"Оберіть інтервал між розсилками з різних аккаунтів:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    else:
        await message_or_callback.answer(
            f"⏱️ <b>Налаштування інтервалу між аккаунтами:</b>\n\n"
            f"{message_info}\n\n"
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
            
            # Перевіряємо чи це різні повідомлення
            data = await state.get_data()
            account_messages = data.get('account_messages', {})
            
            if account_messages:
                # Це різні повідомлення - переходимо до налаштувань інтервалів між повідомленнями
                await start_mass_broadcast_process(callback, state)
            else:
                # Це звичайна розсилка
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

@router.callback_query(lambda c: c.data == "mass_message_intervals")
async def process_mass_message_intervals_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка налаштувань інтервалів між повідомленнями"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Швидкий (5-15 сек)", callback_data="mass_msg_interval_5_15")],
        [InlineKeyboardButton(text="🕐 Середній (10-30 сек)", callback_data="mass_msg_interval_10_30")],
        [InlineKeyboardButton(text="🕑 Повільний (20-60 сек)", callback_data="mass_msg_interval_20_60")],
        [InlineKeyboardButton(text="✏️ Власний діапазон", callback_data="mass_msg_interval_custom")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
    ])
    
    await callback.message.answer(
        "📨 <b>Налаштування інтервалів між повідомленнями:</b>\n\n"
        "⚡ <b>Швидкий:</b> 5-15 секунд\n"
        "🕐 <b>Середній:</b> 10-30 секунд\n"
        "🕑 <b>Повільний:</b> 20-60 секунд\n"
        "✏️ <b>Власний:</b> введіть min-max через кому\n\n"
        "💡 <b>Примітка:</b> Це інтервал між відправкою повідомлень в різні групи одним аккаунтом",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("mass_msg_interval_"))
async def process_mass_message_interval_range_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору діапазону інтервалів між повідомленнями"""
    data = callback.data
    
    if data == "mass_msg_interval_custom":
        # Запитуємо користувача ввести власний діапазон
        await callback.message.answer(
            "✏️ <b>Введіть власний діапазон інтервалів між повідомленнями:</b>\n\n"
            "📝 <b>Формат:</b> min,max (наприклад: 8,25)\n"
            "⏱️ <b>Діапазон:</b> від 1 до 300 секунд\n\n"
            "💡 <b>Приклади:</b>\n"
            "• 5,15 (від 5 до 15 секунд)\n"
            "• 10,30 (від 10 до 30 секунд)\n"
            "• 20,60 (від 20 до 60 секунд)",
            parse_mode='HTML'
        )
        await state.set_state(MassBroadcastStates.waiting_for_message_interval_config)
        await callback.answer()
        return
    
    # Обробляємо попередньо визначені діапазони
    if data == "mass_msg_interval_5_15":
        min_interval, max_interval = 5, 15
    elif data == "mass_msg_interval_10_30":
        min_interval, max_interval = 10, 30
    elif data == "mass_msg_interval_20_60":
        min_interval, max_interval = 20, 60
    else:
        await callback.answer("❌ Невідомий діапазон інтервалу")
        return
    
    # Зберігаємо налаштування інтервалів між повідомленнями
    await state.update_data(
        message_interval=10,  # Фіксований інтервал (не використовується при рандомному)
        use_random_message_interval=True,
        min_message_interval=min_interval,
        max_message_interval=max_interval
    )
    
    await callback.answer(f"📨 Вибрано рандомний інтервал між повідомленнями ({min_interval}-{max_interval} сек)")
    await show_package_selection(callback, state)

@router.message(MassBroadcastStates.waiting_for_message_interval_config)
async def process_custom_mass_message_interval(message: Message, state: FSMContext):
    """Обробка введення власного діапазону інтервалів між повідомленнями"""
    try:
        # Парсимо введений діапазон
        parts = message.text.strip().split(',')
        if len(parts) != 2:
            await message.answer("❌ Неправильний формат. Введіть min,max (наприклад: 8,25)")
            return
        
        min_interval = int(parts[0].strip())
        max_interval = int(parts[1].strip())
        
        # Перевіряємо валідність діапазону
        if min_interval < 1 or max_interval > 300 or min_interval >= max_interval:
            await message.answer("❌ Неправильний діапазон. Мінімум: 1 сек, максимум: 300 сек, min < max")
            return
        
        # Зберігаємо налаштування
        await state.update_data(
            message_interval=10,  # Фіксований інтервал (не використовується при рандомному)
            use_random_message_interval=True,
            min_message_interval=min_interval,
            max_message_interval=max_interval
        )
        
        await message.answer(f"✅ Встановлено рандомний інтервал між повідомленнями: {min_interval}-{max_interval} секунд")
        await show_package_selection(message, state)
        
    except ValueError:
        await message.answer("❌ Введіть числа через кому (наприклад: 8,25)")
    except Exception as e:
        await message.answer(f"❌ Помилка при обробці діапазону: {str(e)}")

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
    """Показ налаштувань інтервалів між повідомленнями"""
    data = await state.get_data()
    message_text = data.get('message_text', '')
    account_messages = data.get('account_messages', {})
    interval = data.get('interval', 60)
    use_random = data.get('use_random_interval', False)
    min_random = data.get('min_random_seconds', 30)
    max_random = data.get('max_random_seconds', 120)
    
    # Показуємо налаштування інтервалів між повідомленнями
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Швидкий (5-15 сек)", callback_data="mass_msg_interval_5_15")],
        [InlineKeyboardButton(text="🕐 Середній (10-30 сек)", callback_data="mass_msg_interval_10_30")],
        [InlineKeyboardButton(text="🕑 Повільний (20-60 сек)", callback_data="mass_msg_interval_20_60")],
        [InlineKeyboardButton(text="✏️ Власний діапазон", callback_data="mass_msg_interval_custom")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
    ])
    
    interval_display = f"{min_random}-{max_random} сек (рандом)" if use_random else f"{interval} сек"
    
    # Формуємо інформацію про повідомлення
    if account_messages:
        message_info = f"📝 <b>Різні повідомлення для {len(account_messages)} аккаунтів</b>"
    else:
        message_info = f"📝 <b>Повідомлення:</b> {message_text[:50]}{'...' if len(message_text) > 50 else ''}"
    
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(
            f"📨 <b>Налаштування інтервалів між повідомленнями:</b>\n\n"
            f"{message_info}\n"
            f"⏱️ <b>Інтервал між аккаунтами:</b> {interval_display}\n\n"
            f"⚡ <b>Швидкий:</b> 5-15 секунд\n"
            f"🕐 <b>Середній:</b> 10-30 секунд\n"
            f"🕑 <b>Повільний:</b> 20-60 секунд\n"
            f"✏️ <b>Власний:</b> введіть min-max через кому\n\n"
            f"💡 <b>Примітка:</b> Це інтервал між відправкою повідомлень в різні групи одним аккаунтом",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    else:
        await message_or_callback.answer(
            f"📨 <b>Налаштування інтервалів між повідомленнями:</b>\n\n"
            f"{message_info}\n"
            f"⏱️ <b>Інтервал між аккаунтами:</b> {interval_display}\n\n"
            f"⚡ <b>Швидкий:</b> 5-15 секунд\n"
            f"🕐 <b>Середній:</b> 10-30 секунд\n"
            f"🕑 <b>Повільний:</b> 20-60 секунд\n"
            f"✏️ <b>Власний:</b> введіть min-max через кому\n\n"
            f"💡 <b>Примітка:</b> Це інтервал між відправкою повідомлень в різні групи одним аккаунтом",
            parse_mode='HTML',
            reply_markup=keyboard
        )

async def show_package_selection(message_or_callback, state: FSMContext):
    """Показ вибору пакетів для масової розсилки"""
    data = await state.get_data()
    message_text = data.get('message_text', '')
    account_messages = data.get('account_messages', {})
    interval = data.get('interval', 60)
    use_random = data.get('use_random_interval', False)
    min_random = data.get('min_random_seconds', 30)
    max_random = data.get('max_random_seconds', 120)
    message_interval = data.get('message_interval', 10)
    use_random_message_interval = data.get('use_random_message_interval', False)
    min_message_interval = data.get('min_message_interval', 5)
    max_message_interval = data.get('max_message_interval', 30)
    
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
    
    # Формуємо інформацію про повідомлення
    if account_messages:
        selection_text += f"📝 <b>Повідомлення:</b> Різні для {len(account_messages)} аккаунтів\n"
    else:
        selection_text += f"📝 <b>Повідомлення:</b> {message_text[:50]}{'...' if len(message_text) > 50 else ''}\n"
    
    if use_random:
        selection_text += f"🎲 <b>Рандомний інтервал між аккаунтами:</b> {min_random}-{max_random} секунд\n"
    else:
        selection_text += f"⏱️ <b>Інтервал між аккаунтами:</b> {interval} секунд\n"
    
    if use_random_message_interval:
        selection_text += f"🎲 <b>Рандомний інтервал між повідомленнями:</b> {min_message_interval}-{max_message_interval} секунд\n"
    else:
        selection_text += f"⏱️ <b>Інтервал між повідомленнями:</b> {message_interval} секунд\n"
    
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
    keyboard_buttons.append([InlineKeyboardButton(text="🌐 Всі чати на аккаунті", callback_data="mass_select_all_chats")])
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

@router.callback_query(lambda c: c.data == "mass_select_all_chats")
async def mass_select_all_chats_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору всіх чатів на аккаунті для масової розсилки"""
    # Зберігаємо вибір всіх чатів
    await state.update_data(selected_package_id="all_chats")  # "all_chats" означає всі чати на аккаунті
    
    # Показуємо підтвердження
    await show_mass_broadcast_confirmation(callback, state)

async def show_mass_broadcast_confirmation(callback: CallbackQuery, state: FSMContext):
    """Показ підтвердження масової розсилки"""
    data = await state.get_data()
    message_text = data.get('message_text', '')
    account_messages = data.get('account_messages', {})
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
    
    if account_messages:
        # Різні повідомлення
        confirmation_text += f"📝 <b>Повідомлення:</b> Різні для {len(account_messages)} аккаунтів\n"
        for phone, msg_data in account_messages.items():
            # Отримуємо текст повідомлення
            msg_text = msg_data.get('text', '')
            if msg_text:
                confirmation_text += f"   📱 {phone}: {msg_text[:30]}{'...' if len(msg_text) > 30 else ''}\n"
            else:
                confirmation_text += f"   📱 {phone}: {msg_data.get('type', 'медіа')}\n"
    else:
        # Однакове повідомлення
        confirmation_text += f"📝 <b>Повідомлення:</b> {message_text[:100]}{'...' if len(message_text) > 100 else ''}\n"
    
    if selected_package_id == "all_chats":
        confirmation_text += f"🌐 <b>Ціль:</b> Всі чати на аккаунті\n"
    elif selected_package_id:
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
    
    confirmation_text += "\nОберіть режим розсилки:"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Зациклена розсилка", callback_data="confirm_loop_broadcast")],
        [InlineKeyboardButton(text="✅ Одноразова розсилка", callback_data="confirm_mass_broadcast")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")]
    ])
    
    await callback.message.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)
    await callback.answer()

@router.callback_query(lambda c: c.data == "confirm_loop_broadcast")
async def confirm_loop_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження зацикленої розсилки"""
    data = await state.get_data()
    message_text = data.get('message_text', '')
    account_messages = data.get('account_messages', {})
    interval = data.get('interval', 60)
    use_random = data.get('use_random_interval', False)
    min_random = data.get('min_random_seconds', 30)
    max_random = data.get('max_random_seconds', 120)
    selected_package_id = data.get('selected_package_id')
    
    # Отримуємо налаштування інтервалів між повідомленнями
    message_interval = data.get('message_interval', 10)
    use_random_message_interval = data.get('use_random_message_interval', False)
    min_message_interval = data.get('min_message_interval', 5)
    max_message_interval = data.get('max_message_interval', 30)
    
    # Зберігаємо налаштування
    db.update_mass_broadcast_settings(interval, use_random, min_random, max_random,
                                     message_interval, use_random_message_interval, 
                                     min_message_interval, max_message_interval)
    
    await callback.message.answer("🔄 Запуск зацикленої розсилки...")
    
    # Створюємо event для зупинки
    stop_event = asyncio.Event()
    
    # Отримуємо дані про медіа
    message_type = data.get('message_type')
    media_file_path = data.get('media_file_path')
    
    # Запускаємо зациклену розсилку в фоновому режимі
    task = asyncio.create_task(loop_broadcast_process(
        message_text, 
        interval, 
        use_random, 
        min_random, 
        max_random, 
        selected_package_id, 
        callback.message,
        message_interval,
        use_random_message_interval,
        min_message_interval,
        max_message_interval,
        account_messages,
        stop_event,
        message_type,
        media_file_path
    ))
    
    # Зберігаємо активну розсилку
    active_broadcasts[callback.from_user.id] = {
        'task': task,
        'stop_event': stop_event
    }
    
    # Показуємо кнопку зупинки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹️ Зупинити розсилку", callback_data="stop_broadcast")]
    ])
    
    await callback.message.answer(
        "🔄 <b>Зациклена розсилка запущена!</b>\n\n"
        "⏹️ Натисніть кнопку нижче для зупинки",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    
    await state.clear()
    await callback.answer()

@router.callback_query(lambda c: c.data == "confirm_mass_broadcast")
async def confirm_mass_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження масової розсилки"""
    data = await state.get_data()
    message_text = data.get('message_text', '')
    account_messages = data.get('account_messages', {})
    interval = data.get('interval', 60)
    use_random = data.get('use_random_interval', False)
    min_random = data.get('min_random_seconds', 30)
    max_random = data.get('max_random_seconds', 120)
    selected_package_id = data.get('selected_package_id')
    
    # Отримуємо налаштування інтервалів між повідомленнями
    message_interval = data.get('message_interval', 10)
    use_random_message_interval = data.get('use_random_message_interval', False)
    min_message_interval = data.get('min_message_interval', 5)
    max_message_interval = data.get('max_message_interval', 30)
    
    # Зберігаємо налаштування
    db.update_mass_broadcast_settings(interval, use_random, min_random, max_random,
                                     message_interval, use_random_message_interval, 
                                     min_message_interval, max_message_interval)
    
    await callback.message.answer("🚀 Запуск масової розсилки...")
    
    # Створюємо event для зупинки
    stop_event = asyncio.Event()
    
    # Отримуємо дані про медіа
    message_type = data.get('message_type')
    media_file_path = data.get('media_file_path')
    
    # Запускаємо масову розсилку в фоновому режимі
    task = asyncio.create_task(mass_broadcast_process(
        message_text, 
        interval, 
        use_random, 
        min_random, 
        max_random, 
        selected_package_id, 
        callback.message,
        message_interval,
        use_random_message_interval,
        min_message_interval,
        max_message_interval,
        account_messages,
        stop_event,
        message_type,
        media_file_path
    ))
    
    # Зберігаємо активну розсилку
    active_broadcasts[callback.from_user.id] = {
        'task': task,
        'stop_event': stop_event
    }
    
    # Показуємо кнопку зупинки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹️ Зупинити розсилку", callback_data="stop_broadcast")]
    ])
    
    await callback.message.answer(
        "🚀 <b>Розсилка запущена!</b>\n\n"
        "⏹️ Натисніть кнопку нижче для зупинки",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    
    await state.clear()
    await callback.answer()

@router.callback_query(lambda c: c.data == "stop_broadcast")
async def stop_broadcast_callback(callback: CallbackQuery):
    """Зупинка розсилки"""
    user_id = callback.from_user.id
    
    if user_id in active_broadcasts:
        # Зупиняємо розсилку
        active_broadcasts[user_id]['stop_event'].set()
        
        # Чекаємо завершення задачі
        try:
            await active_broadcasts[user_id]['task']
        except asyncio.CancelledError:
            pass
        
        # Видаляємо з активних розсилок
        del active_broadcasts[user_id]
        
        await callback.message.answer("⏹️ <b>Розсилка зупинена!</b>", parse_mode='HTML')
    else:
        await callback.message.answer("❌ Немає активних розсилок для зупинки.")
    
    await callback.answer()

async def get_all_chats_for_account(account_phone: str):
    """Отримати всі чати на аккаунті"""
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
            return []
        
        # Створюємо клієнт
        session_name = f"sessions/temp_{account['phone_number'].replace('+', '').replace('-', '')}"
        client = TelegramClient(session_name, account['api_id'], account['api_hash'])
        
        try:
            await client.connect()
            
            if not await client.is_user_authorized():
                logger.error(f"❌ Аккаунт {account_phone} не авторизований")
                return []
            
            # Отримуємо всі діалоги
            dialogs = await client.get_dialogs()
            groups = []
            
            for dialog in dialogs:
                if dialog.is_group or dialog.is_channel:
                    # Додаємо групу/канал
                    group_info = {
                        'group_id': str(dialog.id),
                        'name': dialog.name or f"Група {dialog.id}",
                        'type': 'group' if dialog.is_group else 'channel'
                    }
                    groups.append(group_info)
            
            logger.info(f"✅ Знайдено {len(groups)} чатів на аккаунті {account_phone}")
            return groups
            
        finally:
            try:
                await client.disconnect()
            except:
                pass
                
    except Exception as e:
        logger.error(f"❌ Помилка при отриманні чатів для аккаунта {account_phone}: {e}")
        return []

async def mass_broadcast_process(message_text: str, interval: int, use_random: bool, 
                               min_random: int, max_random: int, selected_package_id: int, message_obj,
                               message_interval: int = 10, use_random_message_interval: bool = False,
                               min_message_interval: int = 5, max_message_interval: int = 30,
                               account_messages: dict = None, stop_event: asyncio.Event = None,
                               message_type: str = None, media_file_path: str = None):
    """Функція для масової розсилки через всі аккаунти"""
    try:
        accounts = db.get_accounts()
        
        if not accounts:
            await message_obj.answer("❌ Немає зареєстрованих аккаунтів.")
            return
        
        # Відправляємо інформаційне повідомлення про налаштування
        interval_info = f"🎲 Рандомний: {min_random}-{max_random} сек" if use_random else f"⏱️ Фіксований: {interval} сек"
        message_interval_info = f"🎲 Рандомний: {min_message_interval}-{max_message_interval} сек" if use_random_message_interval else f"⏱️ Фіксований: {message_interval} сек"
        
        # Формуємо інформацію про повідомлення
        if account_messages:
            message_info = f"📝 <b>Повідомлення:</b> Різні для {len(account_messages)} аккаунтів"
        else:
            message_info = f"📝 <b>Повідомлення:</b> {message_text[:50]}{'...' if len(message_text) > 50 else ''}"
        
        await message_obj.answer(f"🚀 <b>Запуск масової розсилки!</b>\n\n"
                               f"{message_info}\n"
                               f"👥 <b>Аккаунтів:</b> {len(accounts)}\n"
                               f"⏱️ <b>Інтервал між аккаунтами:</b> {interval_info}\n"
                               f"📨 <b>Інтервал між повідомленнями:</b> {message_interval_info}\n\n"
                               f"🔄 <b>Починаємо розсилку...</b>",
                               parse_mode='HTML')
        
        total_accounts = len(accounts)
        successful_accounts = 0
        failed_accounts = 0
        
        await message_obj.answer(f"🚀 Початок масової розсилки з {total_accounts} аккаунтів...")
        
        for i, account in enumerate(accounts, 1):
            try:
                await message_obj.answer(f"📤 Розсилка з аккаунта {i}/{total_accounts}: {account['phone_number']}")
                
                # Отримуємо басейни для цього аккаунта з retry логікою
                groups = []
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        if selected_package_id == "all_chats":
                            # Отримуємо всі чати на аккаунті
                            groups = await get_all_chats_for_account(account['phone_number'])
                        elif selected_package_id:
                            # Отримуємо всі басейни з конкретного пакету (незалежно від аккаунта)
                            groups = db.get_groups_by_package(selected_package_id)
                        else:
                            # Отримуємо басейни для цього аккаунта
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
                
                # Перевіряємо зупинку перед початком відправки
                if stop_event and stop_event.is_set():
                    await message_obj.answer("⏹️ Розсилка зупинена користувачем")
                    return
                
                # Відправляємо повідомлення
                sent_count = 0
                failed_count = 0
                
                # Створюємо клієнт один раз для аккаунта
                session_name = f"sessions/temp_{account['phone_number'].replace('+', '').replace('-', '')}"
                client = TelegramClient(session_name, account['api_id'], account['api_hash'])
                
                try:
                    await client.connect()
                    
                    if await client.is_user_authorized():
                        for j, group in enumerate(groups, 1):
                            # Перевіряємо зупинку перед кожною відправкою
                            if stop_event and stop_event.is_set():
                                await message_obj.answer("⏹️ Розсилка зупинена користувачем")
                                return
                            
                            try:
                                # Визначаємо повідомлення для цього аккаунта
                                if account_messages and account['phone_number'] in account_messages:
                                    current_message = account_messages[account['phone_number']]
                                else:
                                    # Для загальної розсилки - перевіряємо чи це медіа
                                    if message_type and media_file_path:
                                        # Це медіа-повідомлення
                                        current_message = {
                                            'type': message_type,
                                            'text': message_text,
                                            'file_path': media_file_path
                                        }
                                    else:
                                        # Це текстове повідомлення
                                        current_message = message_text
                                
                                # Відправляємо повідомлення з retry логікою
                                group_id = int(group['group_id'])
                                success = await db.send_message_with_retry(
                                    client, 
                                    group_id, 
                                    group['name'], 
                                    current_message,
                                    message_obj
                                )
                                
                                if success:
                                    sent_count += 1
                                else:
                                    failed_count += 1
                                
                                # Затримка між повідомленнями
                                if j < len(groups):  # Не чекаємо після останнього повідомлення
                                    if use_random_message_interval:
                                        # Рандомний інтервал між повідомленнями
                                        current_interval = random.randint(min_message_interval, max_message_interval)
                                        await message_obj.answer(f"⏳ <b>Затримка між повідомленнями:</b>\n\n"
                                                               f"🕐 <b>Чекаємо:</b> {current_interval} секунд\n"
                                                               f"📊 <b>Діапазон:</b> {min_message_interval}-{max_message_interval} сек\n"
                                                               f"📝 <b>Група:</b> {group['name']}\n"
                                                               f"📈 <b>Прогрес:</b> {j}/{len(groups)}",
                                                               parse_mode='HTML')
                                        await asyncio.sleep(current_interval)
                                    else:
                                        # Фіксований інтервал між повідомленнями
                                        await message_obj.answer(f"⏳ <b>Затримка між повідомленнями:</b>\n\n"
                                                               f"🕐 <b>Чекаємо:</b> {message_interval} секунд\n"
                                                               f"📝 <b>Група:</b> {group['name']}\n"
                                                               f"📈 <b>Прогрес:</b> {j}/{len(groups)}",
                                                               parse_mode='HTML')
                                        await asyncio.sleep(message_interval)
                                
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
                        wait_time = random.randint(min_random, max_random)
                        await message_obj.answer(f"⏱️ <b>Затримка між аккаунтами:</b>\n\n"
                                               f"🕐 <b>Чекаємо:</b> {wait_time} секунд\n"
                                               f"📊 <b>Діапазон:</b> {min_random}-{max_random} сек\n"
                                               f"📱 <b>Аккаунт:</b> {account['phone_number']}\n"
                                               f"📈 <b>Прогрес:</b> {i}/{total_accounts}",
                                               parse_mode='HTML')
                    else:
                        wait_time = interval
                        await message_obj.answer(f"⏱️ <b>Затримка між аккаунтами:</b>\n\n"
                                               f"🕐 <b>Чекаємо:</b> {wait_time} секунд\n"
                                               f"📱 <b>Аккаунт:</b> {account['phone_number']}\n"
                                               f"📈 <b>Прогрес:</b> {i}/{total_accounts}",
                                               parse_mode='HTML')
                    
                    # Чекаємо з можливістю зупинки
                    for _ in range(wait_time):
                        if stop_event and stop_event.is_set():
                            await message_obj.answer("⏹️ Розсилка зупинена користувачем")
                            return
                        await asyncio.sleep(1)
                
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

async def loop_broadcast_process(message_text: str, interval: int, use_random: bool, 
                               min_random: int, max_random: int, selected_package_id: int, message_obj,
                               message_interval: int = 10, use_random_message_interval: bool = False,
                               min_message_interval: int = 5, max_message_interval: int = 30,
                               account_messages: dict = None, stop_event: asyncio.Event = None,
                               message_type: str = None, media_file_path: str = None):
    """Функція для зацикленої розсилки через всі аккаунти"""
    cycle_count = 0
    
    try:
        while not (stop_event and stop_event.is_set()):
            cycle_count += 1
            await message_obj.answer(f"🔄 <b>Цикл {cycle_count} розсилки</b>",
            parse_mode='HTML'
            )
            
            # Викликаємо звичайну розсилку
            await mass_broadcast_process(
                message_text, 
                interval, 
                use_random, 
                min_random, 
                max_random, 
                selected_package_id, 
                message_obj,
                message_interval,
                use_random_message_interval,
                min_message_interval,
                max_message_interval,
                account_messages,
                stop_event,
                message_type,
                media_file_path
            )
            
            # Перевіряємо зупинку після завершення циклу
            if stop_event and stop_event.is_set():
                await message_obj.answer("⏹️ Зациклена розсилка зупинена користувачем")
                break
            
            # Коротка пауза між циклами
            await message_obj.answer("⏳ <b>Пауза між циклами...</b>")
            for _ in range(5):  # 5 секунд пауза
                if stop_event and stop_event.is_set():
                    break
                await asyncio.sleep(1)
        
    except Exception as e:
        logger.error(f"❌ Критична помилка в зацикленій розсилці: {e}")
        await message_obj.answer(f"❌ Критична помилка в зацикленій розсилці: {str(e)[:100]}")

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
        await message.answer(f"❌ У першого аккаунта {first_account['phone_number']} немає пакетів груп. Спочатку додайте басейни.")
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
        await callback.message.answer(f"❌ У аккаунта {account_phone} немає доданих пакетів груп. Спочатку додайте басейни.")
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
    
    # Отримуємо басейни з пакету
    selected_groups = db.get_groups_by_package(package_id)
    
    if not selected_groups:
        await callback.message.answer("❌ Помилка при виборі пакету груп.")
        await callback.answer()
        return
    
    # Зберігаємо вибрані басейни
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
        await message.answer("❌ ID басейни повинен бути числом. Спробуйте ще раз:")
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
        asyncio.create_task(send_broadcast_message(account_phone, message_text, selected_groups, status_id, callback.message))
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
    
    # Отримуємо басейни в пакеті
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
            f"✅ Пакет '{package['name']}' та всі його басейни успішно видалено!"
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
        "💡 <b>Примітка:</b> Можна змішувати різні формати в одному списку\n"
        "🎲 <b>Примітка:</b> Можна використовувати рандомний інтервал від 60 до 360 секунд",
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
        [InlineKeyboardButton(text="✏️ Ввести власний", callback_data="interval_custom")],
        [InlineKeyboardButton(text="🎲 Рандомний інтервал", callback_data="_random_interval")]
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

@router.callback_query(lambda c: c.data == "_random_interval")
async def process_random_interval_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору рандомного інтервалу"""
    # Показуємо опції конфігурації рандомного інтервалу
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Швидкий (10-40 сек)", callback_data="random_interval_10_40")],
        [InlineKeyboardButton(text="🕐 Середній (30-120 сек)", callback_data="random_interval_30_120")],
        [InlineKeyboardButton(text="🕑 Повільний (60-360 сек)", callback_data="random_interval_60_360")],
        [InlineKeyboardButton(text="✏️ Власний діапазон", callback_data="random_interval_custom")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="join_groups")]
    ])
    
    await callback.message.answer(
        "🎲 <b>Виберіть діапазон рандомного інтервалу:</b>\n\n"
        "⚡ <b>Швидкий:</b> 10-40 секунд\n"
        "🕐 <b>Середній:</b> 30-120 секунд\n"
        "🕑 <b>Повільний:</b> 60-360 секунд\n"
        "✏️ <b>Власний:</b> введіть min-max через кому",
        parse_mode='HTML',
        reply_markup=keyboard
    )
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("random_interval_"))
async def process_random_interval_range_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору діапазону рандомного інтервалу"""
    data = callback.data
    
    if data == "random_interval_custom":
        # Запитуємо користувача ввести власний діапазон
        await callback.message.answer(
            "✏️ <b>Введіть власний діапазон рандомного інтервалу:</b>\n\n"
            "📝 <b>Формат:</b> min,max (наприклад: 15,90)\n"
            "⏱️ <b>Діапазон:</b> від 5 до 3600 секунд\n\n"
            "💡 <b>Приклади:</b>\n"
            "• 10,40 (від 10 до 40 секунд)\n"
            "• 30,120 (від 30 до 120 секунд)\n"
            "• 60,300 (від 60 до 300 секунд)",
            parse_mode='HTML'
        )
        await state.set_state(JoinGroupsStates.waiting_for_random_interval_config)
        await callback.answer()
        return
    
    # Обробляємо попередньо визначені діапазони
    if data == "random_interval_10_40":
        min_interval, max_interval = 10, 40
    elif data == "random_interval_30_120":
        min_interval, max_interval = 30, 120
    elif data == "random_interval_60_360":
        min_interval, max_interval = 60, 360
    else:
        await callback.answer("❌ Невідомий діапазон інтервалу")
        return
    
    # Зберігаємо налаштування рандомного інтервалу
    await state.update_data(
        interval="_random_interval",
        min_random_interval=min_interval,
        max_random_interval=max_interval
    )
    
    await callback.answer(f"🎲 Вибрано рандомний інтервал ({min_interval}-{max_interval} сек)")
    await start_join_groups_process(callback, state)

@router.message(JoinGroupsStates.waiting_for_random_interval_config)
async def process_custom_random_interval(message: Message, state: FSMContext):
    """Обробка введення власного діапазону рандомного інтервалу"""
    try:
        # Парсимо введений діапазон
        parts = message.text.strip().split(',')
        if len(parts) != 2:
            await message.answer("❌ Неправильний формат. Введіть min,max (наприклад: 15,90)")
            return
        
        min_interval = int(parts[0].strip())
        max_interval = int(parts[1].strip())
        
        # Перевіряємо валідність діапазону
        if min_interval < 5 or max_interval > 3600 or min_interval >= max_interval:
            await message.answer("❌ Неправильний діапазон. Мінімум: 5 сек, максимум: 3600 сек, min < max")
            return
        
        # Зберігаємо налаштування
        await state.update_data(
            interval="_random_interval",
            min_random_interval=min_interval,
            max_random_interval=max_interval
        )
        
        await message.answer(f"✅ Встановлено рандомний інтервал: {min_interval}-{max_interval} секунд")
        await start_join_groups_process(message, state)
        
    except ValueError:
        await message.answer("❌ Введіть числа через кому (наприклад: 15,90)")
    except Exception as e:
        await message.answer(f"❌ Помилка при обробці діапазону: {str(e)}")

async def start_join_groups_process(message_or_callback, state: FSMContext):
    """Запуск процесу приєднання до груп"""
    data = await state.get_data()
    account_phone = data['selected_account']
    group_ids = data['group_ids']
    interval_type = data['interval']

    if interval_type == "_random_interval":
        min_interval = data.get('min_random_interval', 60)
        max_interval = data.get('max_random_interval', 360)
        interval_display = f"Рандомний інтервал від {min_interval} до {max_interval} секунд"
    else:   
        interval_display = f"{interval_type} секунд"
    
    # Показуємо підтвердження
    confirmation_text = f"📤 <b>Підтвердження приєднання до груп:</b>\n\n"
    confirmation_text += f"📱 <b>Аккаунт:</b> {account_phone}\n"
    confirmation_text += f"👥 <b>Груп для приєднання:</b> {len(group_ids)}\n"
    confirmation_text += f"⏱️ <b>Інтервал:</b> {interval_display} секунд\n\n"
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
    interval_type = data['interval']
    
    await callback.message.answer("🚀 Запуск приєднання до груп...")
    
    # Запускаємо процес приєднання в фоновому режимі
    try:
        asyncio.create_task(join_groups_process(account_phone, group_ids, interval_type, callback.message, data))
        await callback.answer("✅ Процес приєднання до груп запущено!")
    except Exception as e:
        await state.clear()
        await callback.message.answer(f"❌ Помилка при запуску процесу приєднання до груп: {e}")
        await callback.answer()
    
async def join_groups_process(account_phone: str, group_ids: list, interval_type, message_obj, interval_data=None):
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
        
        # Функція для затримки між групами
        async def wait_between_groups(current_index, total_groups):
            if current_index < total_groups:
                if interval_type == "_random_interval":
                    # Використовуємо налаштовані межі рандомного інтервалу
                    min_interval = interval_data.get('min_random_interval', 60) if interval_data else 60
                    max_interval = interval_data.get('max_random_interval', 360) if interval_data else 360
                    current_interval = random.randint(min_interval, max_interval)
                    await message_obj.answer(f"⏳ Чекаємо {current_interval} секунд перед наступною групою...")
                    await asyncio.sleep(current_interval)
                else:
                    await asyncio.sleep(int(interval_type))
        
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
                            await wait_between_groups(i, len(group_ids))
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
                            await wait_between_groups(i, len(group_ids))
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
                                await wait_between_groups(i, len(group_ids))
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
                            
                        except FloodWaitError as flood_error:
                            # Обробка FloodWaitError
                            random_time = random.randint(10,50)
                            wait_time = flood_error.seconds
                            await message_obj.answer(f"⏳ FloodWait: чекаємо {wait_time + random_time} секунд, Flooad wait: {wait_time}, Random time: {random_time} перед приєднанням до {group_input}")
                            await asyncio.sleep(wait_time + random_time)
                            # Повторюємо спробу приєднання
                            try:
                                await client(JoinChannelRequest(group_entity))
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
                                await message_obj.answer(f"✅ Приєднано до групи {group_name} ({group_input}) після очікування")
                            except Exception as retry_error:
                                failed_count += 1
                                await message_obj.answer(f"❌ Помилка приєднання до групи {group_input} після очікування: {str(retry_error)[:100]}")
                        except Exception as join_error:
                            failed_count += 1
                            await message_obj.answer(f"❌ Помилка приєднання до групи {group_input}: {str(join_error)[:100]}")
                    else:
                        failed_count += 1
                        await message_obj.answer(f"❌ Не вдалося знайти групу {group_input}")
                
                except Exception as entity_error:
                    failed_count += 1
                    await message_obj.answer(f"❌ Помилка обробки групи {group_input}: {str(entity_error)[:100]}")
                
                # Затримка між приєднаннями (тільки якщо група успішно оброблена)
                await wait_between_groups(i, len(group_ids))
                    
            except Exception as e:
                failed_count += 1
                await message_obj.answer(f"❌ Критична помилка обробки групи {group_input}: {str(e)[:100]}")
                await wait_between_groups(i, len(group_ids))
        
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