import sqlite3
import os
import logging
import asyncio
import random
from telethon.errors import FloodWaitError
from aiogram import Bot

# Налаштування логування
logger = logging.getLogger(__name__)

# Глобальні змінні для стикерів (будуть імпортовані з основного файлу)
RANDOM_STICKERS = []

class Database:
    def __init__(self, db_path: str = "accounts.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_bot(self):
        """Отримує екземпляр бота з config"""
        from config import bot
        return bot
    
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
            
            # Таблиця шаблонів повідомлень
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    message_type TEXT NOT NULL DEFAULT 'text',
                    text TEXT,
                    file_id TEXT,
                    file_path TEXT,
                    file_name TEXT,
                    file_size INTEGER,
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
        from utils import add_random_pause, simulate_typing, add_random_emoji_to_text, should_send_sticker, should_add_emoji_to_caption, should_send_sticker_with_media
        
        for attempt in range(max_retries):
            try:
                # Перевіряємо, чи існує група
                entity = None
                try:
                    logger.info(f"🔍 Шукаємо групу з ID: {group_id} (тип: {type(group_id)})")
                    
                    # Спробуємо різні формати ID
                    id_variants = [
                        int(group_id),  # Оригінальний ID
                        int(group_id.replace('-100', '')),  # Без префіксу -100
                        int(group_id.replace('-100', '100')),  # Заміна -100 на 100
                    ]
                    
                    # Додаємо спробу з username, якщо він є
                    if hasattr(self, 'get_group_username') and group_name:
                        try:
                            username = self.get_group_username(group_id)
                            if username:
                                id_variants.append(username)
                        except:
                            pass
                    
                    for variant_id in id_variants:
                        try:
                            logger.info(f"🔍 Спробуємо ID: {variant_id}")
                            entity = await client.get_entity(variant_id)
                            logger.info(f"✅ Група {group_name} знайдена з ID: {variant_id}")
                            break
                        except Exception as variant_error:
                            logger.info(f"❌ ID {variant_id} не працює: {variant_error}")
                            continue
                    
                    if entity is None:
                        # Спробуємо знайти групу через діалоги
                        try:
                            logger.info(f"🔍 Шукаємо групу {group_name} через діалоги...")
                            dialogs = await client.get_dialogs()
                            for dialog in dialogs:
                                if dialog.is_group or dialog.is_channel:
                                    if str(dialog.id) == str(group_id) or str(dialog.id) == str(group_id).replace('-100', ''):
                                        entity = dialog.entity
                                        logger.info(f"✅ Група {group_name} знайдена через діалоги з ID: {dialog.id}")
                                        break
                        except Exception as dialog_error:
                            logger.info(f"❌ Помилка при пошуку через діалоги: {dialog_error}")
                        
                        if entity is None:
                            logger.warning(f"⚠️ Група {group_name} ({group_id}) не знайдена з жодним варіантом ID")
                            return False
                        
                except Exception as e:
                    logger.warning(f"⚠️ Група {group_name} ({group_id}) не знайдена: {e}")
                    return False
                
                # Випадкова пауза перед відправкою
                await add_random_pause()
                
                # Імітуємо друк
                await simulate_typing(client, entity)
                
                # ШТУЧНА ПОМИЛКА ДЛЯ ТЕСТУВАННЯ - FloodWait(400)
                #if attempt == 0:  # Тільки на першій спробі
                #    logger.info("🧪 ТЕСТ: Викликаємо штучну FloodWait(400) помилку")
                #   from telethon.errors import FloodWaitError
                #    test_error = FloodWaitError(420, 1)  # 420 - код помилки, 400 - секунди
                #    logger.info(f"🧪 ТЕСТ: Створено FloodWaitError з seconds={test_error.seconds}")
                #    raise test_error
                
                # Обробляємо різні типи повідомлень
                if isinstance(message_data, str):
                    # Старий формат - просто текст
                    enhanced_message = add_random_emoji_to_text(message_data)
                    
                    # Визначаємо чи відправляти стикер або текст
                    if should_send_sticker() and len(RANDOM_STICKERS) > 0:
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
                            logger.info(f"✅ Відправлено стикер")
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
                        
                        if should_send_sticker() and len(RANDOM_STICKERS) > 0:
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
                    
                    elif message_type in ['photo', 'video', 'audio', 'document','sticker','voice','animation']:
                        # Медіа-повідомлення
                        import os
                        file_id = message_data.get('file_id')
                        
                        # Для стікерів відправляємо як файл
                        if message_type == 'sticker':
                            success = await self.send_sticker_as_file(client, entity, file_id, file_path)
                            if success:
                                logger.info(f"✅ Стікер відправлено в групу {group_name} ({group_id})")
                            else:
                                logger.warning(f"⚠️ Не вдалося відправити стікер")
                                return False
                        elif file_path and os.path.exists(file_path):
                            # Підготовка підпису
                            caption = text
                            if caption and should_add_emoji_to_caption():
                                caption = add_random_emoji_to_text(caption)
                            
                            # Використовуємо тільки file_path для медіа-файлів
                            media_source = file_path
                            
                            # Відправка медіа
                            if message_type == 'photo':
                                await client.send_file(entity, media_source, caption=caption)
                            elif message_type == 'video':
                                await client.send_file(entity, media_source, caption=caption, video_note=True)
                            elif message_type == 'audio':
                                await client.send_file(entity, media_source, caption=caption, voice_note=True)
                            elif message_type == 'animation':
                                await client.send_file(entity, media_source, caption=caption)
                            elif message_type == 'voice':
                                await client.send_file(entity, media_source)
                            else:  # document
                                await client.send_file(entity, media_source, caption=caption)
                            
                            logger.info(f"✅ {message_type.capitalize()} успішно відправлено в групу {group_name} ({group_id})")
                            
                            # Додатково відправляємо стикер якщо потрібно
                            if should_send_sticker_with_media() and len(RANDOM_STICKERS) > 0:
                                try:
                                    await asyncio.sleep(random.uniform(1.0, 3.0))  # Пауза перед стикером
                                    sticker_id = random.choice(RANDOM_STICKERS)
                                    await client.send_file(entity, sticker_id)
                                    logger.info(f"✅ Додатковий стикер відправлено в групу {group_name} ({group_id})")
                                except Exception as sticker_error:
                                    logger.warning(f"⚠️ Не вдалося відправити додатковий стикер: {sticker_error}")
                        else:
                            if message_type == 'sticker':
                                logger.error(f"❌ Sticker file_id не знайдено")
                            else:
                                logger.error(f"❌ Файл {file_path} не існує")
                            return False
                    else:
                        logger.error(f"❌ Неправильний формат повідомлення: {message_data}")
                        return False
                
                return True
                
            except FloodWaitError as flood_error:
                # Обробка FloodWaitError
                random_time = random.randint(300, 600)
                wait_time = flood_error.seconds
                total_wait = wait_time + random_time
                logger.warning(f"⏳ FloodWait: чекаємо {total_wait} секунд, Flood wait: {wait_time}, Random time: {random_time}")
                logger.info(f"🧪 flood_error.seconds = {flood_error.seconds}, type = {type(flood_error.seconds)}")
                
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
    
    def set_broadcast_status(self, account_phone: str, message_text: str, total_groups: int, 
                           sent_count: int = 0, failed_count: int = 0, status: str = 'running') -> int:
        """Встановити статус розсилання (створює новий або оновлює існуючий)"""
        try:
            print(f"DEBUG: set_broadcast_status - account_phone: {account_phone}, message_text: {message_text}, total_groups: {total_groups}, status: {status}")
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Спочатку очищаємо старі статуси для цього аккаунта
                cursor.execute("""
                    UPDATE broadcast_status 
                    SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                    WHERE account_phone = ? AND status IN ('pending', 'running')
                """, (account_phone,))
                
                # Створюємо новий статус
                cursor.execute("""
                    INSERT INTO broadcast_status 
                    (account_phone, message_text, total_groups, sent_count, failed_count, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (account_phone, message_text, total_groups, sent_count, failed_count, status))
                conn.commit()
                
                status_id = cursor.lastrowid
                print(f"DEBUG: set_broadcast_status - створено статус з ID: {status_id}")
                return status_id
        except Exception as e:
            logger.error(f"❌ Помилка при встановленні статусу розсилання: {e}")
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
    
    def update_broadcast_status_by_phone(self, account_phone: str, sent_count: int = None, 
                                       failed_count: int = None, status: str = None) -> bool:
        """Оновити статус розсилання за номером телефону"""
        try:
            print(f"DEBUG: update_broadcast_status_by_phone - account_phone: {account_phone}, sent_count: {sent_count}, failed_count: {failed_count}, status: {status}")
            
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
                    params.append(account_phone)
                    query = f"UPDATE broadcast_status SET {', '.join(updates)} WHERE account_phone = ? AND status IN ('pending', 'running')"
                    cursor.execute(query, params)
                    updated_count = cursor.rowcount
                    conn.commit()
                    print(f"DEBUG: update_broadcast_status_by_phone - оновлено {updated_count} записів")
                    return updated_count > 0
                return False
        except Exception as e:
            logger.error(f"❌ Помилка при оновленні статусу розсилання за телефоном: {e}")
            return False
    
    def is_account_broadcasting(self, account_phone: str) -> bool:
        """Перевірити чи аккаунт зараз надсилає повідомлення"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Перевіряємо тільки записи не старші за 2 години
                cursor.execute("""
                    SELECT COUNT(*) FROM broadcast_status 
                    WHERE account_phone = ? AND status IN ('pending', 'running')
                    AND started_at > datetime('now', '-2 hours')
                """, (account_phone,))
                count = cursor.fetchone()[0]
                
                print(f"DEBUG: is_account_broadcasting - аккаунт {account_phone}: count = {count}")
                
                # Відладочна інформація
                if count > 0:
                    cursor.execute("""
                        SELECT id, status, started_at, message_text FROM broadcast_status 
                        WHERE account_phone = ? AND status IN ('pending', 'running')
                        AND started_at > datetime('now', '-2 hours')
                        ORDER BY started_at DESC
                    """, (account_phone,))
                    records = cursor.fetchall()
                    print(f"DEBUG: Аккаунт {account_phone} має {count} активних розсилок (останні 2 години):")
                    for record in records:
                        print(f"  - ID: {record[0]}, Status: {record[1]}, Started: {record[2]}, Message: {record[3][:50]}...")
                else:
                    # Показуємо всі записи для цього аккаунта (включаючи старі)
                    cursor.execute("""
                        SELECT id, status, started_at, message_text FROM broadcast_status 
                        WHERE account_phone = ? 
                        ORDER BY started_at DESC
                        LIMIT 5
                    """, (account_phone,))
                    all_records = cursor.fetchall()
                    print(f"DEBUG: Аккаунт {account_phone} - останні 5 записів:")
                    for record in all_records:
                        print(f"  - ID: {record[0]}, Status: {record[1]}, Started: {record[2]}, Message: {record[3][:50]}...")
                
                return count > 0
        except Exception as e:
            logger.error(f"❌ Помилка при перевірці статусу аккаунта {account_phone}: {e}")
            return False
    
    def cleanup_old_broadcast_statuses(self, hours_old: int = 24) -> int:
        """Очистити старі статуси розсилки (старші за вказану кількість годин)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Спочатку подивимося що є в базі
                cursor.execute("""
                    SELECT id, account_phone, status, started_at, message_text 
                    FROM broadcast_status 
                    WHERE status IN ('pending', 'running')
                    ORDER BY started_at DESC
                """)
                old_records = cursor.fetchall()
                
                if old_records:
                    print(f"DEBUG: Знайдено {len(old_records)} записів зі статусом pending/running:")
                    for record in old_records:
                        print(f"  - ID: {record[0]}, Phone: {record[1]}, Status: {record[2]}, Started: {record[3]}")
                
                # Очищаємо тільки записи старші за вказану кількість годин
                cursor.execute("""
                    UPDATE broadcast_status 
                    SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                    WHERE status IN ('pending', 'running') 
                    AND started_at < datetime('now', '-{} hours')
                """.format(hours_old))
                updated_count = cursor.rowcount
                conn.commit()
                
                if updated_count > 0:
                    print(f"DEBUG: Очищено {updated_count} старих статусів розсилки (старші за {hours_old} годин)")
                
                return updated_count
        except Exception as e:
            logger.error(f"❌ Помилка при очищенні старих статусів розсилки: {e}")
            return 0
    
    def clear_account_broadcast_status(self, account_phone: str) -> int:
        """Очистити статуси розсилки для конкретного аккаунта"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE broadcast_status 
                    SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                    WHERE account_phone = ? AND status IN ('pending', 'running')
                """, (account_phone,))
                updated_count = cursor.rowcount
                conn.commit()
                
                if updated_count > 0:
                    print(f"DEBUG: Очищено {updated_count} статусів розсилки для аккаунта {account_phone}")
                
                return updated_count
        except Exception as e:
            logger.error(f"❌ Помилка при очищенні статусів розсилки для аккаунта {account_phone}: {e}")
            return 0
    
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
    
    async def send_sticker_as_file(self, client, entity, file_id, file_path):
        """Відправляє стікер як файл"""
        try:
            # Спочатку намагаємося відправити завантажений файл
            if file_path and os.path.exists(file_path):
                await client.send_file(entity, file_path)
                logger.info(f"✅ Стікер відправлено як файл: {file_path}")
                return True
            else:
                # Fallback: завантажуємо стікер через Bot API
                from utils import download_media_file
                
                # Створюємо шлях для стікера
                sticker_path = f"media_files/sticker_{file_id}.webp"
                
                # Завантажуємо стікер через Bot API
                bot = self.get_bot()  # Отримуємо бота
                await download_media_file(bot, file_id, sticker_path)
                
                # Відправляємо стікер як файл
                await client.send_file(entity, sticker_path)
                logger.info(f"✅ Стікер відправлено як файл (fallback): {sticker_path}")
                return True
                
        except Exception as e:
            logger.warning(f"⚠️ Помилка при відправці стікера: {e}")
            return False
    
    # Методи для роботи з шаблонами
    def add_template(self, name: str, message_type: str, text: str = None, 
                    file_id: str = None, file_path: str = None, 
                    file_name: str = None, file_size: int = None) -> int:
        """Додати новий шаблон"""
        try:
            print(f"DEBUG: add_template called with:")
            print(f"DEBUG: name: {name}")
            print(f"DEBUG: message_type: {message_type}")
            print(f"DEBUG: text: {text}")
            print(f"DEBUG: file_id: {file_id}")
            print(f"DEBUG: file_path: {file_path}")
            print(f"DEBUG: file_name: {file_name}")
            print(f"DEBUG: file_size: {file_size}")
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO templates 
                    (name, message_type, text, file_id, file_path, file_name, file_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (name, message_type, text, file_id, file_path, file_name, file_size))
                conn.commit()
                template_id = cursor.lastrowid
                print(f"DEBUG: Template saved with ID: {template_id}")
                return template_id
        except Exception as e:
            logger.error(f"❌ Помилка при додаванні шаблону: {e}")
            return 0
    
    def get_templates(self) -> list:
        """Отримати всі шаблони"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM templates 
                    ORDER BY created_at DESC
                """)
                templates = []
                for row in cursor.fetchall():
                    templates.append(dict(row))
                return templates
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні шаблонів: {e}")
            return []
    
    def get_template(self, template_id: int) -> dict:
        """Отримати конкретний шаблон за ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM templates WHERE id = ?", (template_id,))
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
        except Exception as e:
            logger.error(f"❌ Помилка при отриманні шаблону: {e}")
            return None
    
    def update_template(self, template_id: int, name: str = None, message_type: str = None,
                       text: str = None, file_id: str = None, file_path: str = None,
                       file_name: str = None, file_size: int = None) -> bool:
        """Оновити шаблон"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                updates = []
                params = []
                
                if name is not None:
                    updates.append("name = ?")
                    params.append(name)
                if message_type is not None:
                    updates.append("message_type = ?")
                    params.append(message_type)
                if text is not None:
                    updates.append("text = ?")
                    params.append(text)
                if file_id is not None:
                    updates.append("file_id = ?")
                    params.append(file_id)
                if file_path is not None:
                    updates.append("file_path = ?")
                    params.append(file_path)
                if file_name is not None:
                    updates.append("file_name = ?")
                    params.append(file_name)
                if file_size is not None:
                    updates.append("file_size = ?")
                    params.append(file_size)
                
                if updates:
                    updates.append("updated_at = CURRENT_TIMESTAMP")
                    params.append(template_id)
                    query = f"UPDATE templates SET {', '.join(updates)} WHERE id = ?"
                    cursor.execute(query, params)
                    conn.commit()
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ Помилка при оновленні шаблону: {e}")
            return False
    
    def delete_template(self, template_id: int) -> bool:
        """Видалити шаблон"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM templates WHERE id = ?", (template_id,))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"❌ Помилка при видаленні шаблону: {e}")
            return False
