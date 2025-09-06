import asyncio
import getpass
import sqlite3
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberInvalidError, PhoneCodeExpiredError
from dotenv import load_dotenv

# Завантажуємо змінні з .env файлу
load_dotenv()

# API дані з .env файлу
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Перевірка конфігурації
print(f"🔍 API_ID: {API_ID}")
print(f"🔍 API_HASH: {'*' * len(API_HASH) if API_HASH else 'НЕ ВСТАНОВЛЕНО'}")

if not API_ID or not API_HASH:
    print("❌ Помилка: API_ID або API_HASH не встановлено!")
    print("Створіть .env файл з правильними даними:")
    exit(1)

# База даних (та сама що й у бота)
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
            print(f"❌ Помилка при додаванні аккаунта: {e}")
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
            print(f"❌ Помилка при отриманні аккаунтів: {e}")
            return []

async def main():
    # Ініціалізація бази даних
    db = Database()
    
    # Запитуємо номер телефону
    phone_number = input("📱 Введіть номер телефону (формат +380XXXXXXXXX): ").strip()
    
    # Перевіряємо формат номера
    if not phone_number.startswith('+') or len(phone_number) < 10:
        print("❌ Невірний формат номера телефону!")
        return
    
    # Перевіряємо чи аккаунт вже існує
    accounts = db.get_accounts()
    for account in accounts:
        if account['phone_number'] == phone_number:
            print(f"✅ Аккаунт {phone_number} вже зареєстрований!")
            return
    
    # Створюємо клієнт
    session_name = f"sessions/temp_{phone_number.replace('+', '').replace('-', '')}"
    client = TelegramClient(session_name, API_ID, API_HASH)

    try:
        print("🔗 Підключення до Telegram...")
        await client.connect()
        print("✅ Підключення успішне!")

        if not await client.is_user_authorized():
            print(f"📱 Відправка коду підтвердження на {phone_number}...")
            try:
                await client.send_code_request(phone_number)
                print("✅ Код відправлено успішно!")
            except Exception as e:
                print(f"❌ Помилка при відправці коду: {e}")
                return

            code = input('🔢 Введіть код отриманий в Telegram: ').strip()
            try:
                await client.sign_in(phone=phone_number, code=code)
            except SessionPasswordNeededError:
                print("🔐 Увімкнено двофакторну автентифікацію (2FA).")
                password = getpass.getpass("🔑 Введіть пароль 2FA: ")
                await client.sign_in(password=password)
            except PhoneCodeInvalidError:
                print("❌ Невірний код!")
                return
            except PhoneCodeExpiredError:
                print("❌ Код застарів!")
                return

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
            print(f"""
✅ <b>Аккаунт успішно додано до бази даних!</b>

📱 <b>Номер:</b> {phone_number}
👤 <b>Ім'я:</b> {me.first_name or 'Не вказано'}
👤 <b>Прізвище:</b> {me.last_name or 'Не вказано'}
🔗 <b>Username:</b> @{me.username or 'Не вказано'}
🆔 <b>ID:</b> {me.id}
            """)
        else:
            print("❌ Помилка при збереженні в базу даних!")

    except PhoneNumberInvalidError:
        print("❌ Невірний номер телефону!")
    except Exception as e:
        print(f"❌ Помилка: {e}")
    finally:
        await client.disconnect()

if __name__ == '__main__':
    print("🤖 Скрипт авторизації аккаунтів для бота")
    print("=" * 50)
    asyncio.run(main())
