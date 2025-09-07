import logging
import os
from aiogram import Router
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError, PhoneNumberInvalidError
from states import RegistrationStates
from utils import show_accounts_list, start_registration_process

# Налаштування логування
logger = logging.getLogger(__name__)

# Створюємо роутер для реєстрації
router = Router()

# Глобальні змінні (будуть імпортовані з основного файлу)
API_ID = 0
API_HASH = ""
db = None

def init_registration_module(api_id, api_hash, database):
    """Ініціалізація модуля реєстрації"""
    global API_ID, API_HASH, db
    API_ID = api_id
    API_HASH = api_hash
    db = database

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

@router.message(Command("accounts"))
async def cmd_accounts(message: Message):
    """Обробник команди /accounts - показує список всіх аккаунтів"""
    await show_accounts_list(message, db)

@router.message(Command("delete_account"))
async def cmd_delete_account(message: Message, state: FSMContext):
    """Обробник команди /delete_account - видаляє аккаунт"""
    await message.answer("🔄 Введіть номер телефону аккаунта для видалення: ")
    await state.set_state(RegistrationStates.waiting_for_phone_to_delete)
