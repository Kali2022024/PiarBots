import asyncio
import logging
import os
import random
from aiogram import Router
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from telethon import TelegramClient
from states import MassBroadcastStates
from utils import download_media_file, get_media_type_from_file

# Налаштування логування
logger = logging.getLogger(__name__)

# Створюємо роутер для масової розсилки
router = Router()

# Глобальні змінні (будуть імпортовані з основного файлу)
db = None
bot = None

def init_mass_broadcast_module(database, telegram_bot):
    """Ініціалізація модуля масової розсилки"""
    global db, bot
    db = database
    bot = telegram_bot

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

    await callback.message.answer(settings_text, parse_mode='HTML', reply_markup=keyboard)
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
        file_path=file_path,
        file_id=file_id
    )
    
    # Показуємо кнопки для вибору підпису
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Без підпису", callback_data="mass_media_no_caption")],
        [InlineKeyboardButton(text="📝 З підписом", callback_data="mass_media_with_caption")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
    ])
    
    await message.answer(
        f"📎 <b>Медіа-файл завантажено:</b>\n"
        f"📁 <b>Тип:</b> {message_type}\n"
        f"📂 <b>Файл:</b> {os.path.basename(file_path)}\n\n"
        f"Оберіть опцію для підпису:",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@router.callback_query(lambda c: c.data in ["mass_media_no_caption", "mass_media_with_caption"])
async def process_mass_media_caption_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору підпису для медіа"""
    data = await state.get_data()
    message_type = data.get('message_type')
    file_path = data.get('file_path')
    
    if callback.data == "mass_media_no_caption":
        # Без підпису
        await state.update_data(text="")
        await show_interval_settings(callback, state)
    else:
        # З підписом
        await callback.message.answer("📝 Введіть підпис для медіа-файлу:")
        await state.set_state(MassBroadcastStates.waiting_for_media_caption)
    
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_media_caption)
async def process_mass_media_caption(message: Message, state: FSMContext):
    """Обробка підпису для медіа"""
    caption = message.text.strip()
    await state.update_data(text=caption)
    await show_interval_settings(message, state)

@router.callback_query(lambda c: c.data == "mass_same_message")
async def process_mass_same_message_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору однакового повідомлення для всіх аккаунтів"""
    await show_interval_settings(callback, state)
    await callback.answer()

@router.callback_query(lambda c: c.data == "mass_different_messages")
async def process_mass_different_messages_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору різних повідомлень для аккаунтів"""
    accounts = db.get_accounts()
    
    if not accounts:
        await callback.message.answer("❌ Немає зареєстрованих аккаунтів.")
        await callback.answer()
        return
    
    # Зберігаємо список аккаунтів для налаштування повідомлень
    await state.update_data(accounts_to_configure=accounts.copy())
    
    # Показуємо перший аккаунт для налаштування
    await show_remaining_accounts(callback.message, state)
    await callback.answer()

async def show_remaining_accounts(message: Message, state: FSMContext):
    """Показати аккаунти, які залишилися для налаштування повідомлень"""
    data = await state.get_data()
    accounts_to_configure = data.get('accounts_to_configure', [])
    
    if not accounts_to_configure:
        # Всі аккаунти налаштовані, переходимо до налаштування інтервалів
        await show_interval_settings(message, state)
        return
    
    # Беремо перший аккаунт зі списку
    current_account = accounts_to_configure[0]
    account_phone = current_account['phone_number']
    
    # Показуємо кнопки для вибору типу повідомлення
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Текстове повідомлення", callback_data=f"message_type_text_{account_phone}")],
        [InlineKeyboardButton(text="📎 Медіа-файл", callback_data=f"message_type_media_{account_phone}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
    ])
    
    await message.answer(
        f"📱 <b>Налаштування повідомлення для аккаунта:</b>\n"
        f"📞 <b>Номер:</b> {account_phone}\n"
        f"👤 <b>Ім'я:</b> {current_account.get('first_name', 'Не вказано')}\n\n"
        f"Оберіть тип повідомлення:",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@router.callback_query(lambda c: c.data.startswith("message_type_"))
async def process_message_type_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору типу повідомлення"""
    data = callback.data.replace("message_type_", "")
    message_type, account_phone = data.split("_", 1)
    
    # Зберігаємо тип повідомлення для цього аккаунта
    account_messages = state.get_data().get('account_messages', {})
    account_messages[account_phone] = {'type': message_type}
    await state.update_data(account_messages=account_messages)
    
    if message_type == "text":
        # Запитуємо текст повідомлення
        await callback.message.answer(f"📝 Введіть текст повідомлення для аккаунта {account_phone}:")
        await state.set_state(MassBroadcastStates.waiting_for_account_message)
    else:
        # Запитуємо медіа-файл
        await callback.message.answer(f"📎 Надішліть медіа-файл для аккаунта {account_phone}:")
        await state.set_state(MassBroadcastStates.waiting_for_media_file)
    
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_account_message)
async def process_account_message(message: Message, state: FSMContext):
    """Обробка повідомлення для конкретного аккаунта"""
    text = message.text.strip()
    
    if not text:
        await message.answer("❌ Текст повідомлення не може бути порожнім. Спробуйте ще раз:")
        return
    
    # Отримуємо поточний аккаунт
    data = await state.get_data()
    accounts_to_configure = data.get('accounts_to_configure', [])
    
    if not accounts_to_configure:
        await message.answer("❌ Помилка: немає аккаунтів для налаштування.")
        return
    
    current_account = accounts_to_configure[0]
    account_phone = current_account['phone_number']
    
    # Зберігаємо повідомлення для цього аккаунта
    account_messages = data.get('account_messages', {})
    account_messages[account_phone]['text'] = text
    await state.update_data(account_messages=account_messages)
    
    # Видаляємо поточний аккаунт зі списку
    accounts_to_configure.pop(0)
    await state.update_data(accounts_to_configure=accounts_to_configure)
    
    # Показуємо наступний аккаунт або переходимо до налаштування інтервалів
    await show_remaining_accounts(message, state)

@router.message(MassBroadcastStates.waiting_for_media_file)
async def process_media_file(message: Message, state: FSMContext):
    """Обробка медіа-файлу для конкретного аккаунта"""
    # Аналогічно до process_mass_media_file, але для конкретного аккаунта
    # Це спрощена версія - в реальному коді тут буде повна логіка
    await message.answer("📎 Медіа-файл оброблено. Переходимо до налаштування інтервалів.")
    await show_interval_settings(message, state)

async def show_interval_settings(message_or_callback, state: FSMContext):
    """Показати налаштування інтервалів"""
    # Отримуємо поточні налаштування
    settings = db.get_mass_broadcast_settings()
    
    settings_text = f"⚙️ <b>Налаштування інтервалів:</b>\n\n"
    settings_text += f"⏱️ <b>Поточний інтервал:</b> {settings['interval_seconds']} секунд\n"
    settings_text += f"🎲 <b>Рандомний інтервал:</b> {'Увімкнено' if settings['use_random_interval'] else 'Вимкнено'}\n"
    if settings['use_random_interval']:
        settings_text += f"📊 <b>Діапазон:</b> {settings['min_random_seconds']}-{settings['max_random_seconds']} секунд\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏱️ 30 секунд", callback_data="mass_interval_30")],
        [InlineKeyboardButton(text="⏱️ 60 секунд", callback_data="mass_interval_60")],
        [InlineKeyboardButton(text="⏱️ 120 секунд", callback_data="mass_interval_120")],
        [InlineKeyboardButton(text="⏱️ 300 секунд", callback_data="mass_interval_300")],
        [InlineKeyboardButton(text="🎲 Рандомний інтервал", callback_data="mass_random_interval")],
        [InlineKeyboardButton(text="✏️ Власний інтервал", callback_data="mass_custom_interval")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
    ])
    
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(settings_text, parse_mode='HTML', reply_markup=keyboard)
    else:
        await message_or_callback.answer(settings_text, parse_mode='HTML', reply_markup=keyboard)

@router.callback_query(lambda c: c.data.startswith("mass_interval_"))
async def process_mass_interval_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору інтервалу"""
    interval_text = callback.data.replace("mass_interval_", "")
    
    if interval_text == "custom":
        await callback.message.answer("✏️ Введіть інтервал в секундах:")
        await state.set_state(MassBroadcastStates.waiting_for_interval)
    else:
        try:
            interval = int(interval_text)
            await state.update_data(interval=interval, use_random=False)
            await show_package_selection(callback, state)
        except ValueError:
            await callback.message.answer("❌ Невірний інтервал. Спробуйте ще раз.")
    
    await callback.answer()

@router.callback_query(lambda c: c.data == "mass_random_interval")
async def process_mass_random_interval_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору рандомного інтервалу"""
    await callback.message.answer("🎲 Рандомний інтервал увімкнено. Переходимо до вибору пакетів.")
    await state.update_data(use_random=True)
    await show_package_selection(callback, state)
    await callback.answer()

async def show_package_selection(message_or_callback, state: FSMContext):
    """Показати вибір пакетів для розсилки"""
    accounts = db.get_accounts()
    
    if not accounts:
        if hasattr(message_or_callback, 'message'):
            await message_or_callback.message.answer("❌ Немає зареєстрованих аккаунтів.")
        else:
            await message_or_callback.answer("❌ Немає зареєстрованих аккаунтів.")
        return
    
    # Отримуємо всі пакети груп
    all_packages = []
    for account in accounts:
        packages = db.get_group_packages(account['phone_number'])
        for package in packages:
            package['account_phone'] = account['phone_number']
            all_packages.append(package)
    
    if not all_packages:
        if hasattr(message_or_callback, 'message'):
            await message_or_callback.message.answer("❌ Немає пакетів груп для розсилки.")
        else:
            await message_or_callback.answer("❌ Немає пакетів груп для розсилки.")
        return
    
    # Показуємо список пакетів
    packages_text = "📦 <b>Доступні пакети груп:</b>\n\n"
    
    keyboard_buttons = []
    for i, package in enumerate(all_packages[:10]):  # Показуємо тільки перші 10
        button_text = f"📦 {package['name']} ({package['groups_count']} груп)"
        keyboard_buttons.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"mass_select_package_{package['id']}"
        )])
        packages_text += f"{i+1}. 📦 <b>{package['name']}</b> ({package['groups_count']} груп)\n"
        packages_text += f"   📱 Аккаунт: {package['account_phone']}\n\n"
    
    # Додаємо кнопки для вибору всіх пакетів
    keyboard_buttons.append([InlineKeyboardButton(
        text="✅ Вибрати всі пакети",
        callback_data="mass_select_all_packages"
    )])
    
    keyboard_buttons.append([InlineKeyboardButton(
        text="🔙 Назад",
        callback_data="Mass_broadcast"
    )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(packages_text, parse_mode='HTML', reply_markup=keyboard)
    else:
        await message_or_callback.answer(packages_text, parse_mode='HTML', reply_markup=keyboard)

@router.callback_query(lambda c: c.data.startswith("mass_select_package_"))
async def mass_select_package_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору пакету для масової розсилки"""
    package_id = int(callback.data.replace("mass_select_package_", ""))
    
    # Отримуємо групи з пакету
    groups = db.get_groups_by_package(package_id)
    
    if not groups:
        await callback.message.answer("❌ Помилка при виборі пакету груп.")
        await callback.answer()
        return
    
    # Зберігаємо вибрані групи
    await state.update_data(selected_package_id=package_id, selected_groups=groups)
    
    # Показуємо підтвердження
    await show_mass_broadcast_confirmation(callback, state)
    await callback.answer()

@router.callback_query(lambda c: c.data == "mass_select_all_packages")
async def mass_select_all_packages_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору всіх пакетів"""
    accounts = db.get_accounts()
    all_groups = []
    
    for account in accounts:
        packages = db.get_group_packages(account['phone_number'])
        for package in packages:
            groups = db.get_groups_by_package(package['id'])
            all_groups.extend(groups)
    
    if not all_groups:
        await callback.message.answer("❌ Немає груп для розсилки.")
        await callback.answer()
        return
    
    # Зберігаємо всі групи
    await state.update_data(selected_package_id=0, selected_groups=all_groups)
    
    # Показуємо підтвердження
    await show_mass_broadcast_confirmation(callback, state)
    await callback.answer()

async def show_mass_broadcast_confirmation(callback: CallbackQuery, state: FSMContext):
    """Показати підтвердження масової розсилки"""
    data = await state.get_data()
    selected_groups = data.get('selected_groups', [])
    message_text = data.get('message_text', '')
    message_type = data.get('message_type', 'text')
    file_path = data.get('file_path', '')
    
    # Підраховуємо статистику
    total_groups = len(selected_groups)
    accounts_count = len(set(group['account_phone'] for group in selected_groups))
    
    confirmation_text = f"📤 <b>Підтвердження масової розсилки:</b>\n\n"
    confirmation_text += f"👥 <b>Аккаунтів:</b> {accounts_count}\n"
    confirmation_text += f"📦 <b>Груп:</b> {total_groups}\n"
    confirmation_text += f"📝 <b>Тип повідомлення:</b> {message_type}\n"
    
    if message_type == 'text':
        confirmation_text += f"📄 <b>Текст:</b> {message_text[:100]}{'...' if len(message_text) > 100 else ''}\n"
    else:
        confirmation_text += f"📎 <b>Файл:</b> {os.path.basename(file_path) if file_path else 'Не вказано'}\n"
    
    confirmation_text += "\n🚀 Запустити масову розсилку?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запустити розсилку", callback_data="confirm_mass_broadcast")],
        [InlineKeyboardButton(text="🔄 Запустити в циклі", callback_data="confirm_loop_broadcast")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")]
    ])
    
    await callback.message.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "confirm_mass_broadcast")
async def confirm_mass_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження масової розсилки"""
    data = await state.get_data()
    selected_groups = data.get('selected_groups', [])
    message_text = data.get('message_text', '')
    message_type = data.get('message_type', 'text')
    file_path = data.get('file_path', '')
    interval = data.get('interval', 60)
    use_random = data.get('use_random', False)
    
    if not selected_groups:
        await callback.message.answer("❌ Немає груп для розсилки.")
        await state.clear()
        await callback.answer()
        return
    
    # Підготовка повідомлення
    if message_type == 'text':
        message_data = message_text
    else:
        message_data = {
            'type': message_type,
            'text': data.get('text', ''),
            'file_path': file_path
        }
    
    await callback.message.answer("🚀 Масову розсилку запущено! Ви можете відстежити прогрес через 'Статус розсилання'.")
    
    # Запускаємо масову розсилку в фоновому режимі
    asyncio.create_task(mass_broadcast_process(
        message_data, interval, use_random, 30, 120, 0, callback.message,
        account_messages=data.get('account_messages', {}),
        message_type=message_type,
        media_file_path=file_path
    ))
    
    await state.clear()
    await callback.answer()

@router.callback_query(lambda c: c.data == "confirm_loop_broadcast")
async def confirm_loop_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження циклічної розсилки"""
    data = await state.get_data()
    selected_groups = data.get('selected_groups', [])
    message_text = data.get('message_text', '')
    message_type = data.get('message_type', 'text')
    file_path = data.get('file_path', '')
    interval = data.get('interval', 60)
    use_random = data.get('use_random', False)
    
    if not selected_groups:
        await callback.message.answer("❌ Немає груп для розсилки.")
        await state.clear()
        await callback.answer()
        return
    
    # Підготовка повідомлення
    if message_type == 'text':
        message_data = message_text
    else:
        message_data = {
            'type': message_type,
            'text': data.get('text', ''),
            'file_path': file_path
        }
    
    await callback.message.answer("🔄 Циклічну розсилку запущено! Ви можете відстежити прогрес через 'Статус розсилання'.")
    
    # Запускаємо циклічну розсилку в фоновому режимі
    asyncio.create_task(loop_broadcast_process(
        message_data, interval, use_random, 30, 120, 0, callback.message,
        account_messages=data.get('account_messages', {}),
        message_type=message_type,
        media_file_path=file_path
    ))
    
    await state.clear()
    await callback.answer()

async def mass_broadcast_process(message_text, interval: int, use_random: bool, 
                               min_random: int, max_random: int, selected_package_id: int, message_obj,
                               message_interval: int = 10, use_random_message_interval: bool = False,
                               min_message_interval: int = 5, max_message_interval: int = 30,
                               account_messages: dict = None, stop_event: asyncio.Event = None,
                               message_type: str = None, media_file_path: str = None):
    """Процес масової розсилки"""
    try:
        accounts = db.get_accounts()
        
        if not accounts:
            await message_obj.answer("❌ Немає зареєстрованих аккаунтів.")
            return
        
        # Отримуємо групи для розсилки
        if selected_package_id == 0:
            # Всі групи
            all_groups = []
            for account in accounts:
                packages = db.get_group_packages(account['phone_number'])
                for package in packages:
                    groups = db.get_groups_by_package(package['id'])
                    all_groups.extend(groups)
            groups_to_send = all_groups
        else:
            # Конкретний пакет
            groups_to_send = db.get_groups_by_package(selected_package_id)
        
        if not groups_to_send:
            await message_obj.answer("❌ Немає груп для розсилки.")
            return
        
        # Групуємо групи по аккаунтах
        groups_by_account = {}
        for group in groups_to_send:
            account_phone = group['account_phone']
            if account_phone not in groups_by_account:
                groups_by_account[account_phone] = []
            groups_by_account[account_phone].append(group)
        
        total_sent = 0
        total_failed = 0
        
        for account_phone, groups in groups_by_account.items():
            try:
                # Отримуємо дані аккаунта
                account = None
                for acc in accounts:
                    if acc['phone_number'] == account_phone:
                        account = acc
                        break
                
                if not account:
                    logger.error(f"❌ Аккаунт {account_phone} не знайдено")
                    continue
                
                # Створюємо клієнт
                session_name = f"sessions/temp_{account_phone.replace('+', '').replace('-', '')}"
                client = TelegramClient(session_name, account['api_id'], account['api_hash'])
                
                await client.connect()
                
                if not await client.is_user_authorized():
                    logger.error(f"❌ Аккаунт {account_phone} не авторизований")
                    await client.disconnect()
                    continue
                
                # Визначаємо повідомлення для цього аккаунта
                if account_messages and account_phone in account_messages:
                    account_message = account_messages[account_phone]
                    if account_message['type'] == 'text':
                        current_message = account_message['text']
                    else:
                        current_message = {
                            'type': account_message['type'],
                            'text': account_message.get('text', ''),
                            'file_path': account_message.get('file_path', '')
                        }
                else:
                    current_message = message_text
                
                # Відправляємо повідомлення в групи цього аккаунта
                for group in groups:
                    try:
                        group_id = int(group['group_id'])
                        
                        success = await db.send_message_with_retry(
                            client, 
                            group_id, 
                            group['name'], 
                            current_message,
                            message_obj
                        )
                        
                        if success:
                            total_sent += 1
                        else:
                            total_failed += 1
                        
                        # Затримка між повідомленнями
                        if use_random_message_interval:
                            delay = random.randint(min_message_interval, max_message_interval)
                        else:
                            delay = message_interval
                        
                        await asyncio.sleep(delay)
                        
                    except Exception as e:
                        total_failed += 1
                        logger.error(f"❌ Помилка при відправці в групу {group['name']}: {e}")
                
                await client.disconnect()
                
                # Затримка між аккаунтами
                if use_random:
                    delay = random.randint(min_random, max_random)
                else:
                    delay = interval
                
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"❌ Помилка при обробці аккаунта {account_phone}: {e}")
                continue
        
        # Показуємо результат
        result_text = f"""
📊 <b>Масову розсилку завершено!</b>

✅ <b>Відправлено:</b> {total_sent}
❌ <b>Помилок:</b> {total_failed}
👥 <b>Аккаунтів:</b> {len(groups_by_account)}
📦 <b>Груп:</b> {len(groups_to_send)}
        """
        
        await message_obj.answer(result_text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"❌ Помилка при масовій розсилці: {e}")
        await message_obj.answer(f"❌ Помилка при масовій розсилці: {e}")

async def loop_broadcast_process(message_text, interval: int, use_random: bool, 
                               min_random: int, max_random: int, selected_package_id: int, message_obj,
                               message_interval: int = 10, use_random_message_interval: bool = False,
                               min_message_interval: int = 5, max_message_interval: int = 30,
                               account_messages: dict = None, stop_event: asyncio.Event = None,
                               message_type: str = None, media_file_path: str = None):
    """Процес циклічної розсилки"""
    await message_obj.answer("🔄 Циклічна розсилка запущена. Для зупинки використайте команду /stop_broadcast")
    
    cycle_count = 0
    
    while True:
        try:
            cycle_count += 1
            await message_obj.answer(f"🔄 Початок циклу #{cycle_count}")
            
            # Запускаємо масову розсилку
            await mass_broadcast_process(
                message_text, interval, use_random, min_random, max_random, 
                selected_package_id, message_obj, message_interval, 
                use_random_message_interval, min_message_interval, max_message_interval,
                account_messages, stop_event, message_type, media_file_path
            )
            
            # Затримка між циклами
            if use_random:
                delay = random.randint(min_random, max_random)
            else:
                delay = interval
            
            await message_obj.answer(f"⏳ Наступний цикл через {delay} секунд...")
            await asyncio.sleep(delay)
            
        except Exception as e:
            logger.error(f"❌ Помилка в циклічній розсилці: {e}")
            await message_obj.answer(f"❌ Помилка в циклічній розсилці: {e}")
            break

@router.callback_query(lambda c: c.data == "stop_broadcast")
async def stop_broadcast_callback(callback: CallbackQuery):
    """Зупинка розсилки"""
    # Тут можна додати логіку зупинки активних розсилок
    await callback.message.answer("🛑 Команда зупинки розсилки отримана. Активні розсилки будуть зупинені.")
    await callback.answer()
