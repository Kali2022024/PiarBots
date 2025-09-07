import asyncio
import logging
from aiogram import Router
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from telethon import TelegramClient
from states import BroadcastStates, GroupStates

# Налаштування логування
logger = logging.getLogger(__name__)

# Створюємо роутер для розсилки
router = Router()

# Глобальні змінні (будуть імпортовані з основного файлу)
db = None

def init_broadcast_module(database):
    """Ініціалізація модуля розсилки"""
    global db
    db = database

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
        await callback.message.answer("🚀 Розсилання запущено! Ви можете відстежити прогрес через 'Статус розсилання'.")
        
        # Запускаємо розсилання в фоновому режимі
        asyncio.create_task(send_broadcast_message(account_phone, message_text, selected_groups, status_id, callback.message))
    else:
        await callback.message.answer("❌ Помилка при створенні статусу розсилання.")
    
    await state.clear()
    await callback.answer()

@router.callback_query(lambda c: c.data == "broadcast_status")
async def broadcast_status_callback(callback: CallbackQuery):
    """Показ статусу розсилання"""
    statuses = db.get_broadcast_statuses()
    
    if not statuses:
        await callback.message.answer("📊 Немає активних розсилань.")
        await callback.answer()
        return
    
    status_text = "📊 <b>Статус розсилання:</b>\n\n"
    
    for status in statuses[:5]:  # Показуємо тільки останні 5
        status_emoji = "🟢" if status['status'] == 'completed' else "🔴" if status['status'] == 'failed' else "🟡"
        status_text += f"{status_emoji} <b>{status['phone_number']}</b>\n"
        status_text += f"📝 {status['message_text'][:50]}...\n"
        status_text += f"📊 {status['sent_count']}/{status['total_groups']} відправлено\n"
        status_text += f"⏰ {status['started_at']}\n\n"
    
    await callback.message.answer(status_text, parse_mode='HTML')
    await callback.answer()
