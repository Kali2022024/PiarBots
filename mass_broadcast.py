import asyncio
import logging
import os
import random
from aiogram import Router
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from states import MassBroadcastStates
from utils import download_media_file

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

def get_media_type_names():
    """Повертає словник назв типів медіа"""
    return {
        "photo": "🖼️ фото",
        "audio": "🎵 аудіо", 
        "video": "🎬 відео",
        "document": "📄 документ",
        "animation": "🎬 гіфка",
        "sticker": "🎭 стікер",
        "voice": "🎤 голосове"
    }

async def handle_media_type_selection(message_type: str, phone: str = None, is_mass_broadcast: bool = False):
    """Обробляє вибір типу медіа та показує відповідні кнопки"""
    media_type_names = get_media_type_names()
    
    if message_type in ("voice", "sticker"):
        # Для голосових та стікерів тільки без підпису
        if is_mass_broadcast:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📎 Без підпису", callback_data="mass_media_no_caption")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
            ])
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📎 Без підпису", callback_data="media_no_caption")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"mass_account_message_{phone}")]
            ])
    else:
        # Для інших типів медіа - з підписом або без
        if is_mass_broadcast:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📎 Без підпису", callback_data="mass_media_no_caption")],
                [InlineKeyboardButton(text="📝 З підписом", callback_data="mass_media_with_caption")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
            ])
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📎 Без підпису", callback_data="media_no_caption")],
                [InlineKeyboardButton(text="📝 З підписом", callback_data="media_with_caption")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"mass_account_message_{phone}")]
            ])
    
    return keyboard, media_type_names[message_type]


async def process_media_file_common(message, message_type, phone, media_dir):
    """Загальна функція для обробки медіа-файлів"""
    file_id = None
    file_path = None
    
    # Визначаємо тип медіа та отримуємо file_id
    if message_type == "photo" and message.photo:
        file_id = message.photo[-1].file_id
        file_path = f"{media_dir}/photo_{file_id}.jpg"
    elif message_type == "audio" and message.audio:
        file_id = message.audio.file_id
        file_path = f"{media_dir}/audio_{file_id}.mp3"
    elif message_type == "video" and message.video:
        file_id = message.video.file_id
        file_path = f"{media_dir}/video_{file_id}.mp4"
    elif message_type == "document" and message.document:
        file_id = message.document.file_id
        file_path = f"{media_dir}/document_{file_id}"
    elif message_type == "sticker" and message.sticker:
        file_id = message.sticker.file_id
        file_path = f"{media_dir}/sticker_{file_id}.webp"
    elif message_type == "voice" and message.voice:
        file_id = message.voice.file_id
        file_path = f"{media_dir}/voice_{file_id}.ogg"
    elif message_type == "animation" and message.animation:
        file_id = message.animation.file_id
        file_path = f"{media_dir}/animation_{file_id}.mp4"
    
    if not file_id:
        return None, None, "❌ Неправильний тип файлу. Спробуйте ще раз."
    
    # Завантажуємо всі типи медіа-файлів
    success = await download_media_file(bot, file_id, file_path)
    
    if not success:
        return None, None, "❌ Помилка завантаження файлу. Спробуйте ще раз."
    
    # Повертаємо file_id та file_path
    return file_id, file_path

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
    if message.photo or message.video or message.audio or message.document or message.animation or message.sticker or message.voice:
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
    # Визначаємо тип медіа
    if message.photo:
        message_type = "photo"
    elif message.video:
        message_type = "video"
    elif message.audio:
        message_type = "audio"
    elif message.document:
        message_type = "document"
    elif message.animation:
        message_type = "animation"
    elif message.sticker:
        message_type = "sticker"
    elif message.voice:
        message_type = "voice"
    else:
        await message.answer("❌ Непідтримуваний тип медіа-файлу.")
        return
    
    # Створюємо папку для медіа-файлів
    media_dir = "media_files"
    if not os.path.exists(media_dir):
        os.makedirs(media_dir)
    
    # Отримуємо дані з state
    data = await state.get_data()
    phone = data.get('selected_phone', 'mass_broadcast')
    
    # Обробляємо медіа-файл
    file_id, file_path = await process_media_file_common(message, message_type, phone, media_dir)
    
    if not file_id or not file_path:
        await message.answer("❌ Помилка обробки медіа-файлу. Спробуйте ще раз.")
        return
    
    # Зберігаємо інформацію про медіа
    update_data = {
        'message_type': message_type,
        'media_file_path': file_path,
        'media_file_id': file_id
    }

    
    await state.update_data(**update_data)
    
    # Показуємо кнопки для вибору підпису
    keyboard, media_type_display = await handle_media_type_selection(message_type, is_mass_broadcast=True)
    
    if message_type == 'sticker':
        await message.answer(
            f"📎 <b>Завантажено {media_type_display}</b>\n\n"
            f"🆔 <b>ID стікера:</b> {file_id}\n\n"
            f"Оберіть опцію для підпису:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    else:
        await message.answer(
            f"📎 <b>Завантажено {media_type_display}</b>\n\n"
            f"📁 <b>Файл:</b> {os.path.basename(file_path)}\n\n"
            f"Оберіть опцію для підпису:",
            parse_mode='HTML',
            reply_markup=keyboard
        )

@router.callback_query(lambda c: c.data in ["mass_media_no_caption", "mass_media_with_caption"])
async def process_mass_media_caption_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору підпису для медіа"""
    has_caption = callback.data == "mass_media_with_caption"
    data = await state.get_data()
    message_type = data.get('message_type')
    
    # Зберігаємо інформацію про підпис
    await state.update_data(has_caption=has_caption)
    
    media_type_names = get_media_type_names()
    
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
    """Обробка підпису для медіа"""
    caption = message.text.strip()
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    file_path = data.get('media_file_path')
    file_id = data.get('media_file_id')
        
    if not caption:
        await message.answer("❌ Підпис не може бути порожнім. Спробуйте ще раз:")
        return
    
    if phone:
        # Це підпис для конкретного аккаунта
        # Зберігаємо повідомлення з підписом
        await save_account_message(state, phone, message_type, file_path, caption, file_id)
        
        # Видаляємо аккаунт зі списку після завершення налаштування
        accounts_to_configure = data.get('accounts_to_configure', [])
        accounts_to_configure = [acc for acc in accounts_to_configure if acc['phone_number'] != phone]
        await state.update_data(accounts_to_configure=accounts_to_configure)
        
        await message.answer(f"✅ Підпис для аккаунта {phone} збережено!")
        await show_remaining_accounts(message, state)
    else:
        # Це загальний підпис для масової розсилки
        await state.update_data(text=caption)
        await show_interval_settings(message, state)

@router.callback_query(lambda c: c.data in ["media_no_caption", "media_with_caption"])
async def process_media_caption_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору підпису для медіа"""
    has_caption = callback.data == "media_with_caption"
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    
    # Зберігаємо інформацію про підпис
    await state.update_data(has_caption=has_caption)
    
    media_type_names = get_media_type_names()
    
    
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
    account_messages = data.get('account_messages', {})
    
    if not accounts_to_configure:
        # Всі аккаунти налаштовані, переходимо до налаштування інтервалів
        await show_interval_settings(message, state)
        return
    
    # Показуємо список аккаунтів для вибору
    keyboard_buttons = []
    for account in accounts_to_configure:
        button_text = f"📱 {account['phone_number']}"
        keyboard_buttons.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"mass_account_message_{account['phone_number']}"
        )])
    
    # Додаємо кнопку запуску розсилки якщо є налаштовані повідомлення
    if len(account_messages) > 0:
        keyboard_buttons.append([InlineKeyboardButton(text="🚀 Запустити розсилку", callback_data="start_different_messages_broadcast")])
    
    keyboard_buttons.append([InlineKeyboardButton(text="✅ Завершити налаштування", callback_data="mass_finish_messages")])
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await message.answer(
        f"✅ <b>Повідомлення налаштовано для {len(account_messages)} аккаунтів</b>\n\n"
        f"📱 Залишилося налаштувати: {len(accounts_to_configure)} аккаунтів\n\n"
        f"Оберіть наступний аккаунт або запустіть розсилку:",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@router.callback_query(lambda c: c.data.startswith("mass_account_message_"))
async def process_mass_account_message_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору аккаунта для налаштування повідомлення"""
    account_phone = callback.data.replace("mass_account_message_", "")
    
    # Зберігаємо вибраний аккаунт
    await state.update_data(selected_account_for_message=account_phone)
    
    # Показуємо кнопки для вибору типу повідомлення
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Текстове повідомлення", callback_data="message_type_text")],
        [InlineKeyboardButton(text="🖼️ Фото", callback_data="message_type_photo")],
        [InlineKeyboardButton(text="🎵 Аудіо", callback_data="message_type_audio")],
        [InlineKeyboardButton(text="🎬 Відео", callback_data="message_type_video")],
        [InlineKeyboardButton(text="📄 Документ", callback_data="message_type_document")],
        [InlineKeyboardButton(text="🎬 Гіфка", callback_data="message_type_animation")],
        [InlineKeyboardButton(text="🎭 Стікер", callback_data="message_type_sticker")],
        [InlineKeyboardButton(text="🎤 Голосове", callback_data="message_type_voice")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="mass_different_messages")]
    ])
    
    await callback.message.answer(
        f"📱 <b>Налаштування повідомлення для аккаунта:</b>\n"
        f"📞 <b>Номер:</b> {account_phone}\n\n"
        f"Оберіть тип повідомлення:",
        parse_mode='HTML',
        reply_markup=keyboard
    )
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
        # Показуємо опції для медіа
        keyboard, media_type_display = await handle_media_type_selection(message_type, phone)
        
        await callback.message.answer(
            f"📎 <b>Завантажте {media_type_display} для аккаунта {phone}:</b>\n\n"
            f"Оберіть опцію:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
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
    phone = data.get('selected_account_for_message')
    accounts_to_configure = data.get('accounts_to_configure', [])
    
    if not phone:
        await message.answer("❌ Помилка: немає поточного аккаунта.")
        return
    
    # Зберігаємо повідомлення для цього аккаунта
    await save_account_message(state, phone, 'text', None, text)
    
    # Видаляємо поточний аккаунт зі списку
    accounts_to_configure = [acc for acc in accounts_to_configure if acc['phone_number'] != phone]
    await state.update_data(accounts_to_configure=accounts_to_configure)
    
    await message.answer(f"✅ Повідомлення для аккаунта {phone} збережено!")
    
    # Показуємо наступний аккаунт або переходимо до налаштування інтервалів
    await show_remaining_accounts(message, state)

@router.message(MassBroadcastStates.waiting_for_media_file)
async def process_media_file(message: Message, state: FSMContext):
    """Обробка завантаження медіа-файлу"""
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    has_caption = data.get('has_caption', False)
    
    # Створюємо папку для медіа-файлів
    media_dir = "media_files"
    if not os.path.exists(media_dir):
        os.makedirs(media_dir)
    
    # Обробляємо медіа-файл
    file_id, file_path = await process_media_file_common(message, message_type, phone, media_dir)
    
    
    # Зберігаємо інформацію про файл
    update_data = {
        'media_file_path': file_path,
        'media_file_id': file_id
    }
    
    
    await state.update_data(**update_data)
    
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
        await save_account_message(state, phone, message_type, file_path, None, file_id)
        
        # Видаляємо аккаунт зі списку після завершення налаштування
        accounts_to_configure = data.get('accounts_to_configure', [])
        accounts_to_configure = [acc for acc in accounts_to_configure if acc['phone_number'] != phone]
        await state.update_data(accounts_to_configure=accounts_to_configure)
        
        await message.answer(f"✅ Медіа-файл для аккаунта {phone} збережено без підпису!")
        await show_remaining_accounts(message, state)

@router.message(MassBroadcastStates.waiting_for_media_caption)
async def process_media_caption(message: Message, state: FSMContext):
    """Обробка введення підпису для медіа"""
    caption = message.text.strip()
    data = await state.get_data()
    phone = data.get('selected_account_for_message')
    message_type = data.get('message_type')
    file_path = data.get('media_file_path')
    file_id = data.get('media_file_id')
    
    if not caption:
        await message.answer("❌ Підпис не може бути порожнім. Спробуйте ще раз:")
        return
    
    # Зберігаємо повідомлення з підписом
    await save_account_message(state, phone, message_type, file_path, caption, file_id)
    
    # Видаляємо аккаунт зі списку після завершення налаштування
    accounts_to_configure = data.get('accounts_to_configure', [])
    accounts_to_configure = [acc for acc in accounts_to_configure if acc['phone_number'] != phone]
    await state.update_data(accounts_to_configure=accounts_to_configure)
    
    await message.answer(f"✅ Підпис для аккаунта {phone} збережено!")
    await show_remaining_accounts(message, state)

async def save_account_message(state: FSMContext, phone: str, message_type: str, file_path: str = None, text: str = None, file_id: str = None):
    """Зберігає повідомлення для аккаунта"""
    data = await state.get_data()
    account_messages = data.get('account_messages', {})
    
    # Створюємо структуру повідомлення
    message_data = {
        'type': message_type,
        'text': text,
        'file_path': file_path,  # Зберігаємо file_path для всіх типів медіа
        'file_id': file_id
    }
    
    
    account_messages[phone] = message_data
    await state.update_data(account_messages=account_messages)

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

@router.callback_query(lambda c: c.data == "mass_finish_messages")
async def process_mass_finish_messages_callback(callback: CallbackQuery, state: FSMContext):
    """Завершення налаштування повідомлень"""
    await show_interval_settings(callback, state)
    await callback.answer()

async def show_interval_settings(message_or_callback, state: FSMContext):
    """Показати налаштування інтервалів"""
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
    """Обробка вибору інтервалу"""
    interval_text = callback.data.replace("mass_interval_", "")
    
    if interval_text == "custom":
        await callback.message.answer("✏️ Введіть інтервал в секундах:")
        await state.set_state(MassBroadcastStates.waiting_for_interval)
    else:
        try:
            interval = int(interval_text)
            await state.update_data(interval=interval, use_random=False)
            logger.info(f"💾 Збережено фіксований інтервал між аккаунтами: {interval} сек")
            
            # Перевіряємо чи дані збереглися
            data_check = await state.get_data()
            logger.info(f"🔍 Перевірка збереження: interval={data_check.get('interval')}, use_random={data_check.get('use_random')}")
            
            await show_package_selection(callback, state)
        except ValueError:
            await callback.message.answer("❌ Невірний інтервал. Спробуйте ще раз.")
    
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_interval)
async def process_custom_interval(message: Message, state: FSMContext):
    """Обробка введення власного інтервалу (використовує існуючу логіку)"""
    try:
        interval = int(message.text.strip())
        
        if interval < 10 or interval > 86400:
            await message.answer("❌ Інтервал повинен бути від 10 до 86400 секунд.")
            return
        
        await state.update_data(interval=interval, use_random=False)
        logger.info(f"💾 Збережено власний інтервал між аккаунтами: {interval} сек")
        
        # Перевіряємо чи дані збереглися
        data_check = await state.get_data()
        logger.info(f"🔍 Перевірка збереження: interval={data_check.get('interval')}, use_random={data_check.get('use_random')}")
        
        await message.answer(f"✅ Встановлено інтервал: {interval} секунд")
        await show_package_selection(message, state)
        
    except ValueError:
        await message.answer("❌ Неправильний формат. Введіть число від 10 до 86400.")

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
    await show_mass_broadcast_confirmation(callback, state)

@router.message(MassBroadcastStates.waiting_for_message_interval_config)
async def process_custom_mass_message_interval(message: Message, state: FSMContext):
    """Обробка введення власного діапазону інтервалів між повідомленнями"""
    try:
        # Парсимо введений діапазон
        parts = message.text.strip().split(',')
        if len(parts) != 2:
            await message.answer("❌ Неправильний формат. Використовуйте: min,max (наприклад: 5,15)")
            return
        
        min_interval = int(parts[0].strip())
        max_interval = int(parts[1].strip())
        
        # Перевіряємо діапазон
        if min_interval < 1 or max_interval > 300 or min_interval >= max_interval:
            await message.answer("❌ Неправильний діапазон. Мінімум: 1, максимум: 300, min < max")
            return
        
        # Зберігаємо налаштування
        await state.update_data(
            message_interval=10,  # Фіксований інтервал (не використовується при рандомному)
            use_random_message_interval=True,
            min_message_interval=min_interval,
            max_message_interval=max_interval
        )
        
        await message.answer(f"✅ Встановлено власний інтервал між повідомленнями: {min_interval}-{max_interval} секунд")
        await show_mass_broadcast_confirmation(message, state)
        
    except ValueError:
        await message.answer("❌ Неправильний формат чисел. Використовуйте: min,max (наприклад: 5,15)")

@router.message(MassBroadcastStates.waiting_for_random_settings)
async def process_random_settings(message: Message, state: FSMContext):
    """Обробка налаштувань рандомного інтервалу"""
    try:
        settings_text = message.text.strip()
        min_interval, max_interval = map(int, settings_text.split(','))
        
        if 10 <= min_interval <= max_interval <= 86400:
            await state.update_data(
                interval=min_interval,  # Додаємо interval для сумісності
                use_random=True,
                min_random=min_interval,
                max_random=max_interval
            )
            logger.info(f"💾 Збережено рандомний інтервал між аккаунтами: {min_interval}-{max_interval} сек")
            
            # Перевіряємо чи дані збереглися
            data_check = await state.get_data()
            logger.info(f"🔍 Перевірка збереження: interval={data_check.get('interval')}, use_random={data_check.get('use_random')}, min_random={data_check.get('min_random')}, max_random={data_check.get('max_random')}")
            
            await message.answer(f"✅ Встановлено рандомний інтервал: {min_interval}-{max_interval} секунд")
            await show_package_selection(message, state)
        else:
            await message.answer("❌ Невірний діапазон. Мінімум: 10, максимум: 86400. Спробуйте ще раз:")
    except ValueError:
        await message.answer("❌ Введіть два числа через кому (наприклад: 30,120). Спробуйте ще раз:")

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
                        'type': 'group' if dialog.is_group else 'channel',
                        'account_phone': account_phone
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

async def show_message_interval_settings(message_or_callback, state: FSMContext):
    """Показати налаштування інтервалів між повідомленнями"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Швидкий (5-15 сек)", callback_data="mass_msg_interval_5_15")],
        [InlineKeyboardButton(text="🕐 Середній (10-30 сек)", callback_data="mass_msg_interval_10_30")],
        [InlineKeyboardButton(text="🕑 Повільний (20-60 сек)", callback_data="mass_msg_interval_20_60")],
        [InlineKeyboardButton(text="✏️ Власний діапазон", callback_data="mass_msg_interval_custom")],
        [InlineKeyboardButton(text="⏭️ Пропустити", callback_data="skip_message_intervals")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="Mass_broadcast")]
    ])
    
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(
            "📨 <b>Налаштування інтервалів між повідомленнями:</b>\n\n"
            "⚡ <b>Швидкий:</b> 5-15 секунд\n"
            "🕐 <b>Середній:</b> 10-30 секунд\n"
            "🕑 <b>Повільний:</b> 20-60 секунд\n"
            "✏️ <b>Власний:</b> введіть min-max через кому\n\n"
            "💡 <b>Примітка:</b> Це інтервал між відправкою повідомлень в різні групи одним аккаунтом\n\n"
            "⏭️ <b>Пропустити:</b> використовувати стандартні налаштування",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    else:
        await message_or_callback.answer(
            "📨 <b>Налаштування інтервалів між повідомленнями:</b>\n\n"
            "⚡ <b>Швидкий:</b> 5-15 секунд\n"
            "🕐 <b>Середній:</b> 10-30 секунд\n"
            "🕑 <b>Повільний:</b> 20-60 секунд\n"
            "✏️ <b>Власний:</b> введіть min-max через кому\n\n"
            "💡 <b>Примітка:</b> Це інтервал між відправкою повідомлень в різні групи одним аккаунтом\n\n"
            "⏭️ <b>Пропустити:</b> використовувати стандартні налаштування",
            parse_mode='HTML',
            reply_markup=keyboard
        )

@router.callback_query(lambda c: c.data == "skip_message_intervals")
async def skip_message_intervals_callback(callback: CallbackQuery, state: FSMContext):
    """Пропустити налаштування інтервалів між повідомленнями"""
    # Встановлюємо стандартні налаштування
    await state.update_data(
        message_interval=10,
        use_random_message_interval=False,
        min_message_interval=5,
        max_message_interval=30
    )
    
    await callback.message.answer("✅ Використовуємо стандартні налаштування інтервалів між повідомленнями (10 секунд)")
    await show_mass_broadcast_confirmation(callback, state)
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
    
    # Додаємо кнопку для вибору всіх чатів
    keyboard_buttons.append([InlineKeyboardButton(
        text="🌐 Всі чати на аккаунті",
        callback_data="mass_select_all_chats"
    )])
    
    # Додаємо кнопку для відправки в одну групу
    keyboard_buttons.append([InlineKeyboardButton(
        text="🎯 Відправити в одну групу",
        callback_data="mass_send_to_single_group"
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
    
    # Отримуємо басейни з пакету
    groups = db.get_groups_by_package(package_id)
    
    if not groups:
        await callback.message.answer("❌ Помилка при виборі пакету груп.")
        await callback.answer()
        return
    
    # Зберігаємо вибрані басейни
    await state.update_data(selected_package_id=package_id, selected_groups=groups)
    
    # Перевіряємо чи вже налаштовані інтервали між повідомленнями
    data = await state.get_data()
    if not data.get('message_interval') and not data.get('use_random_message_interval'):
        # Показуємо налаштування інтервалів між повідомленнями
        await show_message_interval_settings(callback, state)
    else:
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
    
    # Зберігаємо всі басейни
    await state.update_data(selected_package_id=0, selected_groups=all_groups)
    
    # Перевіряємо чи вже налаштований основний інтервал між аккаунтами
    data = await state.get_data()
    if data.get('interval'):
        await show_message_interval_settings(callback, state)
        await callback.answer()
    else:
        # Показуємо підтвердження
        await show_interval_settings(callback, state)
        await callback.answer()

@router.callback_query(lambda c: c.data == "mass_select_all_chats")
async def mass_select_all_chats_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору всіх чатів на аккаунті для масової розсилки"""
    # Зберігаємо вибір всіх чатів
    await state.update_data(selected_package_id="all_chats")  # "all_chats" означає всі чати на аккаунті
    
    # Перевіряємо чи вже налаштований основний інтервал між аккаунтами
    data = await state.get_data()
    if data.get('interval'):
        # Інтервал вже налаштований, переходимо до налаштування інтервалів між повідомленнями
        logger.info("📋 Інтервал вже налаштований, показуємо налаштування інтервалів між повідомленнями")
        await show_message_interval_settings(callback, state)
    else:
        # Інтервал не налаштований, показуємо налаштування основного інтервалу
        logger.info("📋 Інтервал не налаштований, показуємо налаштування основного інтервалу між аккаунтами")
        await show_interval_settings(callback, state)
    
    await callback.answer()

@router.callback_query(lambda c: c.data == "mass_send_to_single_group")
async def mass_send_to_single_group_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки відправки в одну групу"""
    await callback.message.answer("🎯 Введіть ID групи для відправки:")
    await state.set_state(MassBroadcastStates.waiting_for_single_group_id)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_single_group_id)
async def process_mass_single_group_id(message: Message, state: FSMContext):
    """Обробка ID однієї групи для відправки"""
    group_id = message.text.strip()
    data = await state.get_data()
    
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
        'package_name': 'Одна група',
        'account_phone': None  # Будемо визначати пізніше
    }
    
    # Зберігаємо вибрану групу
    await state.update_data(selected_groups=[fake_group])
    
    # Перевіряємо чи вже налаштований основний інтервал між аккаунтами
    data = await state.get_data()
    logger.info(f"🔍 process_mass_single_group_id: interval={data.get('interval')}, use_random={data.get('use_random')}")
    
    if data.get('interval'):
        # Інтервал вже налаштований, переходимо до налаштування інтервалів між повідомленнями
        logger.info("📋 Інтервал вже налаштований, показуємо налаштування інтервалів між повідомленнями")
        await show_message_interval_settings(message, state)
    else:
        # Інтервал не налаштований, показуємо налаштування основного інтервалу
        logger.info("📋 Інтервал не налаштований, показуємо налаштування основного інтервалу між аккаунтами")
        await show_interval_settings(message, state)

async def show_mass_broadcast_confirmation(message_or_callback, state: FSMContext):
    """Показати підтвердження масової розсилки"""
    data = await state.get_data()
    selected_groups = data.get('selected_groups', [])
    selected_package_id = data.get('selected_package_id')
    message_text = data.get('message_text', '')
    message_type = data.get('message_type', 'text')
    file_path = data.get('media_file_path', '')
    account_messages = data.get('account_messages', {})
    
    # Підраховуємо статистику
    if selected_package_id == "all_chats":
        # Для "всіх чатів" показуємо спеціальне повідомлення
        total_groups = "всі чати на аккаунтах"
    else:
        total_groups = len(selected_groups)
    
    # Якщо є налаштовані повідомлення для аккаунтів, рахуємо тільки їх
    if account_messages:
        configured_accounts = list(account_messages.keys())
        accounts_count = len(configured_accounts)
        confirmation_text = f"📤 <b>Підтвердження масової розсилки:</b>\n\n"
        confirmation_text += f"👥 <b>Налаштованих аккаунтів:</b> {accounts_count}\n"
        confirmation_text += f"📱 <b>Аккаунти:</b> {', '.join(configured_accounts)}\n"
    else:
        accounts_count = len(set(group['account_phone'] for group in selected_groups if group['account_phone']))
        confirmation_text = f"📤 <b>Підтвердження масової розсилки:</b>\n\n"
        confirmation_text += f"👥 <b>Аккаунтів:</b> {accounts_count}\n"
    
    confirmation_text += f"📦 <b>Груп:</b> {total_groups}\n"
    confirmation_text += f"📝 <b>Тип повідомлення:</b> {message_type}\n"
    
    # Показуємо інтервал циклічної розсилки, якщо він налаштований
    if data.get('use_random_cycle_interval'):
        cycle_min = data.get('cycle_interval_min')
        cycle_max = data.get('cycle_interval_max')
        if cycle_min and cycle_max:
            confirmation_text += f"🔄 <b>Інтервал циклу:</b> {cycle_min}-{cycle_max} сек (рандом)\n"
    elif data.get('cycle_interval'):
        confirmation_text += f"🔄 <b>Інтервал циклу:</b> {data.get('cycle_interval')} сек\n"
    
    if message_type == 'text':
        confirmation_text += f"📄 <b>Текст:</b> {message_text[:100]}{'...' if len(message_text) > 100 else ''}\n"
    else:
        confirmation_text += f"📎 <b>Файл:</b> {os.path.basename(file_path) if file_path else 'Не вказано'}\n"
    
    confirmation_text += "\n🚀 Запустити масову розсилку?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запустити розсилку", callback_data="confirm_mass_broadcast")],
        [InlineKeyboardButton(text="🔄 Запустити в циклі", callback_data="confirm_loop_broadcast")],
        [InlineKeyboardButton(text="⚙️ Налаштувати інтервал циклу", callback_data="set_cycle_interval")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="Mass_broadcast")]
    ])
    
    if hasattr(message_or_callback, 'message'):
        await message_or_callback.message.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)
    else:
        await message_or_callback.answer(confirmation_text, parse_mode='HTML', reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "confirm_mass_broadcast")
async def confirm_mass_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження масової розсилки"""
    data = await state.get_data()
    selected_groups = data.get('selected_groups', [])
    selected_package_id = data.get('selected_package_id')
    message_text = data.get('message_text', '')
    message_type = data.get('message_type', 'text')
    file_path = data.get('media_file_path', '')
    interval = data.get('interval', 60)
    
    # Перевіряємо чи є групи для розсилки
    if not selected_groups and selected_package_id != "all_chats":
        await callback.message.answer("❌ Немає груп для розсилки.")
        await state.clear()
        await callback.answer()
        return
    
    # Підготовка повідомлення
    if message_type == 'text':
        message_data = message_text
    else:
        # Перевіряємо чи є file_path для всіх типів медіа (крім стікерів)
        if message_type != 'sticker' and not file_path:
            await callback.message.answer("❌ Помилка: не вказано шлях до файлу.")
            await callback.answer()
            return
        elif message_type == 'sticker' and not data.get('media_file_id'):
            await callback.message.answer("❌ Помилка: не вказано ID стікера.")
            await callback.answer()
            return
        
        message_data = {
            'type': message_type,
            'text': data.get('text', ''),
            'file_path': file_path,
            'file_id': data.get('media_file_id')
        }
    
    # Очищуємо флаг зупинки перед запуском
    await state.update_data(stop_broadcast=False)
    
    await callback.message.answer("🚀 Масову розсилку запущено! Ви можете відстежити прогрес через 'Статус розсилання'.\n\n"
                                 "🛑 Для зупинки використайте команду /stop_message")
    
    # Зберігаємо дані основного інтервалу між аккаунтами
    account_interval_data = {
        'interval': data.get('interval'),
        'use_random': data.get('use_random'),
        'min_random': data.get('min_random'),
        'max_random': data.get('max_random')
    }
    
    # Запускаємо масову розсилку в фоновому режимі
    asyncio.create_task(mass_broadcast_process(
        message_data, interval, data.get('use_random', False), 
        data.get('min_random', 30), data.get('max_random', 120), 
        data.get('selected_package_id', 0), callback.message,
        data.get('message_interval', 10), data.get('use_random_message_interval', False), 
        data.get('min_message_interval', 5), data.get('max_message_interval', 30),
        account_messages=data.get('account_messages', {}),
        message_type=message_type,
        media_file_path=file_path,
        media_file_id=data.get('media_file_id'),
        selected_groups=data.get('selected_groups', []),
        state=state,
        account_interval_data=account_interval_data
    ))
    
    await state.clear()
    await callback.answer()



@router.callback_query(lambda c: c.data == "confirm_loop_broadcast")
async def confirm_loop_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Підтвердження циклічної розсилки"""
    data = await state.get_data()
    selected_groups = data.get('selected_groups', [])
    selected_package_id = data.get('selected_package_id')
    message_text = data.get('message_text', '')
    message_type = data.get('message_type', 'text')
    file_path = data.get('media_file_path', '')
    # Логуємо всі дані FSM перед визначенням інтервалу
    logger.info(f"🔍 FSM дані в confirm_loop_broadcast_callback: use_random_cycle_interval={data.get('use_random_cycle_interval')}, cycle_interval={data.get('cycle_interval')}, cycle_interval_min={data.get('cycle_interval_min')}, cycle_interval_max={data.get('cycle_interval_max')}, interval={data.get('interval')}")
    
    # Визначаємо інтервал для циклічної розсилки
    if data.get('use_random_cycle_interval'):
        # Використовуємо рандомний інтервал циклу
        cycle_min = data.get('cycle_interval_min', 30)
        cycle_max = data.get('cycle_interval_max', 120)
        interval = random.randint(cycle_min, cycle_max)
        logger.info(f"🎲 Використовуємо рандомний інтервал циклу: {interval} сек (діапазон: {cycle_min}-{cycle_max})")
    else:
        # Використовуємо фіксований інтервал
        interval = data.get('cycle_interval', data.get('interval', 60))
        logger.info(f"⏳ Використовуємо інтервал: {interval} сек (cycle_interval={data.get('cycle_interval')}, interval={data.get('interval')})")
    
    # Перевіряємо чи є басейни для розсилки
    if not selected_groups and selected_package_id != "all_chats":
        await callback.message.answer("❌ Немає груп для розсилки.")
        await state.clear()
        await callback.answer()
        return
    
    # Підготовка повідомлення
    if message_type == 'text':
        message_data = message_text
    else:
        # Перевіряємо чи є file_path для всіх типів медіа (крім стікерів)
        if message_type != 'sticker' and not file_path:
            await callback.message.answer("❌ Помилка: не вказано шлях до файлу.")
            await callback.answer()
            return
        elif message_type == 'sticker' and not data.get('media_file_id'):
            await callback.message.answer("❌ Помилка: не вказано ID стікера.")
            await callback.answer()
            return
        
        message_data = {
            'type': message_type,
            'text': data.get('text', ''),
            'file_path': file_path,
            'file_id': data.get('media_file_id')
        }
    
    # Очищуємо флаг зупинки перед запуском
    await state.update_data(stop_broadcast=False)
    
    await callback.message.answer("🔄 Циклічну розсилку запущено! Ви можете відстежити прогрес через 'Статус розсилання'.\n\n"
                                 "🛑 Для зупинки використайте команду /stop_message")
    
    # Зберігаємо дані інтервалу циклу та основного інтервалу перед очищенням стану
    cycle_interval_data = {
        'cycle_interval': data.get('cycle_interval'),
        'cycle_interval_min': data.get('cycle_interval_min'),
        'cycle_interval_max': data.get('cycle_interval_max'),
        'use_random_cycle_interval': data.get('use_random_cycle_interval')
    }
    
    # Зберігаємо дані основного інтервалу між аккаунтами
    account_interval_data = {
        'interval': data.get('interval'),
        'use_random': data.get('use_random'),
        'min_random': data.get('min_random'),
        'max_random': data.get('max_random')
    }
    
    # Запускаємо циклічну розсилку в фоновому режимі
    asyncio.create_task(loop_broadcast_process(
        message_data, interval, data.get('use_random', False), 
        data.get('min_random', 30), data.get('max_random', 120), 
        data.get('selected_package_id', 0), callback.message,
        data.get('message_interval', 10), data.get('use_random_message_interval', False), 
        data.get('min_message_interval', 5), data.get('max_message_interval', 30),
        account_messages=data.get('account_messages', {}),
        message_type=message_type,
        media_file_path=file_path,
        media_file_id=data.get('media_file_id'),
        selected_groups=data.get('selected_groups', []),
        state=state,
        cycle_interval_data=cycle_interval_data,
        account_interval_data=account_interval_data
    ))
    
    await state.clear()
    await callback.answer()

@router.callback_query(lambda c: c.data == "set_cycle_interval")
async def set_cycle_interval_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка налаштування інтервалу для циклічної розсилки"""
    await callback.message.answer(
        "⚙️ <b>Налаштування інтервалу між аккаунтами для циклічної розсилки:</b>\n\n"
        "Введіть інтервал в секундах через кому(мінімум 10, максимум 3600):\n\n"
        "💡 <b>Рекомендовані значення:</b>\n"
        "• 30-60 сек - швидка розсилка\n"
        "• 60-120 сек - середня швидкість\n"
        "• 120-300 сек - повільна розсилка",
        parse_mode='HTML'
    )
    await state.set_state(MassBroadcastStates.waiting_for_cycle_interval)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_cycle_interval)
async def process_cycle_interval(message: Message, state: FSMContext):
    """Обробка введення інтервалу для циклічної розсилки"""
    try:
        text = message.text.strip()
        
        # Перевіряємо чи це діапазон (містить кому)
        if ',' in text:
            # Діапазон інтервалів
            parts = text.split(',')
            if len(parts) != 2:
                await message.answer("❌ Неправильний формат. Використовуйте: min,max (наприклад: 30,120)")
                return
            
            min_interval = int(parts[0].strip())
            max_interval = int(parts[1].strip())
            
            if min_interval < 10 or max_interval > 3600 or min_interval >= max_interval:
                await message.answer("❌ Неправильний діапазон. Мінімум: 10, максимум: 3600, min < max")
                return
            
            # Зберігаємо діапазон для циклічної розсилки
            await state.update_data(
                cycle_interval_min=min_interval,
                cycle_interval_max=max_interval,
                use_random_cycle_interval=True
            )
            
            logger.info(f"💾 Збережено рандомний інтервал циклу: {min_interval}-{max_interval} сек, use_random_cycle_interval=True")
            await message.answer(f"✅ Встановлено рандомний інтервал між аккаунтами для циклічної розсилки: {min_interval}-{max_interval} секунд")
            
        else:
            # Фіксований інтервал
            interval = int(text)
            
            if interval < 10 or interval > 3600:
                await message.answer("❌ Інтервал повинен бути від 10 до 3600 секунд.")
                return
            
            # Зберігаємо фіксований інтервал для циклічної розсилки
            await state.update_data(
                cycle_interval=interval,
                use_random_cycle_interval=False
            )
            
            logger.info(f"💾 Збережено фіксований інтервал циклу: {interval} сек, use_random_cycle_interval=False")
            await message.answer(f"✅ Встановлено фіксований інтервал між аккаунтами для циклічної розсилки: {interval} секунд")
        
        # Повертаємося до меню підтвердження
        await show_mass_broadcast_confirmation(message, state)
        
    except ValueError:
        await message.answer("❌ Неправильний формат. Введіть число або діапазон через кому (наприклад: 60 або 30,120).")

async def mass_broadcast_process(message_text, interval: int, use_random: bool, 
                               min_random: int, max_random: int, selected_package_id: int, message_obj,
                               message_interval: int = 10, use_random_message_interval: bool = False,
                               min_message_interval: int = 5, max_message_interval: int = 30,
                               account_messages: dict = None, stop_event: asyncio.Event = None,
                               message_type: str = None, media_file_path: str = None, media_file_id: str = None, selected_groups: list = None,
                               state: FSMContext = None, media_caption: str = None, account_interval_data: dict = None):
    """Процес масової розсилки"""
    
    logger.info(f"🚀 Початок масової розсилки")
    logger.info(f"📊 Параметри розсилки:")
    logger.info(f"   - Інтервал між аккаунтами: {interval} сек (рандом: {use_random})")
    logger.info(f"   - Інтервал між повідомленнями: {message_interval} сек (рандом: {use_random_message_interval})")
    if use_random_message_interval:
        logger.info(f"   - Діапазон інтервалів між повідомленнями: {min_message_interval}-{max_message_interval} сек")
    logger.info(f"   - Тип повідомлення: {message_type}")
    logger.info(f"   - Налаштовані повідомлення для аккаунтів: {len(account_messages) if account_messages else 0}")
    
    # Створюємо статус розсилки
    status_id = None
    try:
        # Підготовка тексту для статусу
        if isinstance(message_text, str):
            status_text = message_text[:100] + "..." if len(message_text) > 100 else message_text
        else:
            status_text = f"{message_text.get('type', 'unknown')} message"
        
        # Отримуємо загальну кількість груп
        if selected_groups:
            total_groups = len(selected_groups)
        elif selected_package_id == "all_chats":
            # Для всіх чатів рахуємо приблизну кількість
            accounts = db.get_accounts()
            total_groups = len(accounts) * 10  # Приблизна оцінка
        else:
            total_groups = 0
        
        # Створюємо статус для першого аккаунта (якщо є)
        accounts = db.get_accounts()
        if accounts:
            first_account = accounts[0]['phone_number']
            status_id = db.create_broadcast_status(first_account, status_text, total_groups)
            
    except Exception as e:
        logger.error(f"❌ Помилка при створенні статусу розсилки: {e}")
    
    try:
        accounts = db.get_accounts()
        
        if not accounts:
            await message_obj.answer("❌ Немає зареєстрованих аккаунтів.")
            return
        
        # Якщо є налаштовані повідомлення для аккаунтів, фільтруємо аккаунти
        if account_messages:
            # Використовуємо тільки налаштовані аккаунти
            configured_accounts = [acc for acc in accounts if acc['phone_number'] in account_messages]
            accounts = configured_accounts
            logger.info(f"📱 Фільтровано аккаунти для налаштованих повідомлень: {len(accounts)} аккаунтів")
            for phone in account_messages.keys():
                logger.info(f"   - {phone}: {account_messages[phone].get('type', 'text') if isinstance(account_messages[phone], dict) else 'text'}")
        
        # Отримуємо групи для розсилки
        if selected_groups:
            # Використовуємо басейни з параметра (включаючи одну групу)
            groups_to_send = selected_groups
        elif selected_package_id == "all_chats":
            # Всі чати на аккаунтах
            all_groups = []
            for account in accounts:
                try:
                    groups = await get_all_chats_for_account(account['phone_number'])
                    all_groups.extend(groups)
                except Exception as e:
                    logger.error(f"❌ Помилка при отриманні чатів для аккаунта {account['phone_number']}: {e}")
                    continue
            groups_to_send = all_groups
        elif selected_package_id == 0:
            # Всі басейни
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
        
        # Перевіряємо чи це відправка в одну групу
        if len(groups_to_send) == 1 and groups_to_send[0].get('package_name') == 'Одна група':
            # Це відправка в одну групу - використовуємо всі налаштовані аккаунти
            single_group = groups_to_send[0]
            groups_by_account = {}
            for account in accounts:
                # Створюємо копію групи для кожного аккаунта
                group_copy = single_group.copy()
                group_copy['account_phone'] = account['phone_number']
                groups_by_account[account['phone_number']] = [group_copy]
        else:
            # Звичайна логіка для пакетів груп
            # Групуємо басейни по аккаунтах
            groups_by_account = {}
            for group in groups_to_send:
                account_phone = group['account_phone']
                if account_phone not in groups_by_account:
                    groups_by_account[account_phone] = []
                groups_by_account[account_phone].append(group)
        
        if not groups_by_account:
            await message_obj.answer("❌ Немає груп для розсилки.")
            return
        
        logger.info(f"📦 Підготовлено груп для розсилки: {len(groups_to_send)} груп")
        logger.info(f"👥 Розподілено по аккаунтах: {len(groups_by_account)} аккаунтів")
        for account_phone, groups in groups_by_account.items():
            logger.info(f"   - {account_phone}: {len(groups)} груп")
        
        total_sent = 0
        total_failed = 0
        
        for account_phone, groups in groups_by_account.items():
            # Перевіряємо флаг зупинки перед обробкою кожного аккаунта
            if state:
                data = await state.get_data()
                if data.get('stop_broadcast', False):
                    logger.info("🛑 Отримано команду зупинки розсилки")
                    await message_obj.answer("🛑 Масову розсилку зупинено користувачем.")
                    return
            
            try:
                logger.info(f"📱 Обробляємо аккаунт: {account_phone} ({len(groups)} груп)")
                
                # Отримуємо дані аккаунта
                account = None
                for acc in accounts:
                    if acc['phone_number'] == account_phone:
                        account = acc
                        break
                
                if not account:
                    logger.error(f"❌ Аккаунт {account_phone} не знайдено в списку аккаунтів")
                    continue
                
                # Створюємо клієнт
                session_name = f"sessions/temp_{account_phone.replace('+', '').replace('-', '')}"
                logger.info(f"🔗 Створюємо клієнт для аккаунта {account_phone}: {session_name}")
                client = TelegramClient(session_name, account['api_id'], account['api_hash'])
                
                logger.info(f"🔌 Підключаємося до Telegram для аккаунта {account_phone}")
                await client.connect()
                
                if not await client.is_user_authorized():
                    logger.error(f"❌ Аккаунт {account_phone} не авторизований")
                    await client.disconnect()
                    continue
                
                logger.info(f"✅ Аккаунт {account_phone} успішно авторизований")
                
                # Визначаємо повідомлення для цього аккаунта
                if account_messages and account_phone in account_messages:
                    logger.info(f"📝 Використовуємо налаштоване повідомлення для аккаунта {account_phone}")
                    current_message = account_messages[account_phone]
                else:
                    # Для загальної розсилки - перевіряємо чи це медіа
                    if message_type and media_file_path:
                        # Це медіа-повідомлення
                        logger.info(f"📎 Використовуємо медіа-повідомлення: {message_type}, файл: {media_file_path}")
                        current_message = {
                            'type': message_type,
                            'text': media_caption or '',  # Використовуємо підпис з параметра
                            'file_path': media_file_path,
                            'file_id': media_file_id  # file_id з параметрів функції
                        }
                    else:
                        # Це текстове повідомлення
                        logger.info(f"📝 Використовуємо текстове повідомлення для аккаунта {account_phone}")
                        current_message = message_text
                
                # Відправляємо повідомлення в басейни цього аккаунта
                logger.info(f"📤 Початок відправки повідомлень для аккаунта {account_phone}: {len(groups)} груп")
                for j, group in enumerate(groups):
                    # Перевіряємо флаг зупинки перед кожною групою
                    if state:
                        data = await state.get_data()
                        if data.get('stop_broadcast', False):
                            logger.info("🛑 Отримано команду зупинки розсилки")
                            await message_obj.answer("🛑 Масову розсилку зупинено користувачем.")
                            await client.disconnect()
                            return
                    
                    max_retries = 3
                    logger.info(f"📋 Обробляємо групу {j+1}/{len(groups)}: {group['name']} (ID: {group['group_id']})")
                    for attempt in range(max_retries):
                        try:
                            group_id = int(group['group_id'])
                            
                            logger.info(f"📤 Спроба {attempt + 1}/{max_retries} відправки в групу {group['name']} (ID: {group_id})")
                            logger.info(f"🔍 Оригінальний ID з бази: {group['group_id']}, Конвертований: {group_id}")
                        
                            # Повідомляємо про початок відправки
                            await message_obj.answer(f"📤 <b>Відправляємо повідомлення:</b>\n\n"
                                                   f"📱 <b>Аккаунт:</b> {account_phone}\n"
                                                   f"📝 <b>Група:</b> {group['name']}\n"
                                                   f"🆔 <b>ID групи:</b> {group_id}\n"
                                                   f"📈 <b>Прогрес:</b> {j+1}/{len(groups)}",
                                                   parse_mode='HTML')
                            
                            success = await db.send_message_with_retry(
                                client, 
                                str(group_id), 
                                group['name'], 
                                current_message,
                                message_obj
                            )
                            
                            if success:
                                total_sent += 1
                                logger.info(f"✅ Повідомлення успішно відправлено в групу {group['name']} ({group_id})")
                                logger.info(f"📊 Статистика: відправлено={total_sent}, невдало={total_failed}")
                                
                                # Оновлюємо статус розсилки
                                if status_id:
                                    try:
                                        db.update_broadcast_status(status_id, sent_count=total_sent, failed_count=total_failed)
                                    except Exception as e:
                                        logger.error(f"❌ Помилка оновлення статусу: {e}")
                            else:
                                total_failed += 1
                                logger.warning(f"⚠️ Не вдалося відправити повідомлення в групу {group['name']} ({group_id})")
                                logger.info(f"📊 Статистика: відправлено={total_sent}, невдало={total_failed}")
                                
                                # Оновлюємо статус розсилки
                                if status_id:
                                    try:
                                        db.update_broadcast_status(status_id, sent_count=total_sent, failed_count=total_failed)
                                    except Exception as e:
                                        logger.error(f"❌ Помилка оновлення статусу: {e}")
                            
                            # Затримка між повідомленнями
                            if j < len(groups) - 1:  # Не чекаємо після останнього повідомлення
                                # Перевіряємо флаг зупинки перед затримкою
                                if state:
                                    data = await state.get_data()
                                    if data.get('stop_broadcast', False):
                                        logger.info("🛑 Отримано команду зупинки розсилки")
                                        await message_obj.answer("🛑 Масову розсилку зупинено користувачем.")
                                        await client.disconnect()
                                        return
                                
                                if use_random_message_interval:
                                    delay = random.randint(min_message_interval, max_message_interval)
                                    await message_obj.answer(f"⏳ <b>Затримка між повідомленнями:</b>\n\n"
                                                           f"🕐 <b>Чекаємо:</b> {delay} секунд\n"
                                                           f"📊 <b>Діапазон:</b> {min_message_interval}-{max_message_interval} сек\n"
                                                           f"📝 <b>Група:</b> {group['name']}\n"
                                                           f"📈 <b>Прогрес:</b> {j+1}/{len(groups)}",
                                                           parse_mode='HTML')
                                else:
                                    delay = message_interval
                                    await message_obj.answer(f"⏳ <b>Затримка між повідомленнями:</b>\n\n"
                                                           f"🕐 <b>Чекаємо:</b> {delay} секунд\n"
                                                           f"📝 <b>Група:</b> {group['name']}\n"
                                                           f"📈 <b>Прогрес:</b> {j+1}/{len(groups)}",
                                                           parse_mode='HTML')
                                
                                logger.info(f"⏳ Затримка між повідомленнями: {delay} секунд")
                                await asyncio.sleep(delay)
                                
                            break  # Успішно відправлено, виходимо з циклу retry
                            
                        except FloodWaitError as flood_error:
                            # FloodWait обробляється в database.py
                            total_failed += 1
                            logger.error(f"❌ FloodWait Error в масовій розсилці: {flood_error}")
                            logger.error(f"⏳ FloodWait: {flood_error.seconds} секунд для групи {group['name']}")
                            logger.info(f"📊 Статистика після FloodWait: відправлено={total_sent}, невдало={total_failed}")
                            break
                        
                        except Exception as e:
                            error_msg = str(e)
                            logger.warning(f"⚠️ Спроба {attempt + 1}/{max_retries} невдала для групи {group['name']}: {error_msg}")
                            
                            # Перевіряємо тип помилки
                            if "Could not find the input entity" in error_msg:
                                logger.error(f"❌ Група {group['name']} (ID: {group_id}) не існує або недоступна для аккаунта {account_phone}")
                                logger.error(f"💡 Можливі причини: група видалена, аккаунт заблокований, група стала приватною")
                                await message_obj.answer(f"❌ <b>Група недоступна:</b>\n\n"
                                                       f"📝 <b>Група:</b> {group['name']}\n"
                                                       f"🆔 <b>ID:</b> {group_id}\n"
                                                       f"📱 <b>Аккаунт:</b> {account_phone}\n\n"
                                                       f"💡 <b>Можливі причини:</b>\n"
                                                       f"• Група була видалена\n"
                                                       f"• Аккаунт заблокований в групі\n"
                                                       f"• Група стала приватною",
                                                       parse_mode='HTML')
                                total_failed += 1
                                logger.info(f"📊 Статистика після помилки: відправлено={total_sent}, невдало={total_failed}")
                                break
                            elif "Chat admin privileges are required" in error_msg:
                                logger.warning(f"⚠️ Недостатньо прав для відправки в групу {group['name']}")
                                total_failed += 1
                                logger.info(f"📊 Статистика після помилки: відправлено={total_sent}, невдало={total_failed}")
                                break
                            elif any(restriction in error_msg for restriction in [
                                "CHAT_SEND_PHOTOS_FORBIDDEN", "CHAT_SEND_MEDIA_FORBIDDEN", 
                                "CHAT_SEND_VIDEOS_FORBIDDEN", "CHAT_SEND_AUDIOS_FORBIDDEN"
                            ]):
                                logger.warning(f"⚠️ Відправка медіа заборонена в групі {group['name']}")
                                total_failed += 1
                                logger.info(f"📊 Статистика після помилки: відправлено={total_sent}, невдало={total_failed}")
                                break
                            elif attempt < max_retries - 1:
                                # Затримка перед повторною спробою
                                retry_delay = random.randint(5, 15)
                                logger.info(f"⏳ Повторна спроба через {retry_delay} секунд")
                                await asyncio.sleep(retry_delay)
                                continue
                            else:
                                total_failed += 1
                                logger.error(f"❌ Помилка при відправці в групу {group['name']} після {max_retries} спроб: {e}")
                                logger.info(f"📊 Статистика після всіх спроб: відправлено={total_sent}, невдало={total_failed}")
                                break
                
                logger.info(f"🔌 Відключаємо клієнт для аккаунта {account_phone}")
                await client.disconnect()
                
                # Затримка між аккаунтами
                # Перевіряємо флаг зупинки перед затримкою між аккаунтами
                if state:
                    data = await state.get_data()
                    if data.get('stop_broadcast', False):
                        logger.info("🛑 Отримано команду зупинки розсилки")
                        await message_obj.answer("🛑 Масову розсилку зупинено користувачем.")
                        return
                
                # Використовуємо збережені дані інтервалу між аккаунтами
                if account_interval_data:
                    if account_interval_data.get('use_random'):
                        delay = random.randint(account_interval_data.get('min_random', 30), account_interval_data.get('max_random', 120))
                        logger.info(f"🎲 Рандомний інтервал між аккаунтами: {delay} сек (діапазон: {account_interval_data.get('min_random')}-{account_interval_data.get('max_random')})")
                    else:
                        delay = account_interval_data.get('interval', interval)
                        logger.info(f"⏳ Фіксований інтервал між аккаунтами: {delay} сек")
                else:
                    # Fallback до FSM даних
                    data = await state.get_data()
                    delay = data.get('interval', interval)
                    logger.info(f"⏳ Fallback інтервал між аккаунтами: {delay} сек (data.interval={data.get('interval')}, param.interval={interval})")
                await asyncio.sleep(delay)
                
                logger.info(f"✅ Завершено обробку аккаунта {account_phone}")
                
            except Exception as e:
                logger.error(f"❌ Помилка при обробці аккаунта {account_phone}: {e}")
                logger.info(f"📊 Статистика після помилки аккаунта: відправлено={total_sent}, невдало={total_failed}")
                continue
        
        # Підраховуємо загальну кількість груп
        total_groups = sum(len(groups) for groups in groups_by_account.values())
        
        # Розраховуємо відсоток успішності
        success_rate = (total_sent / (total_sent + total_failed) * 100) if (total_sent + total_failed) > 0 else 0
        
        logger.info("🏁 Масову розсилку завершено")
        logger.info(f"📊 Фінальна статистика: відправлено={total_sent}, невдало={total_failed}")
        logger.info(f"📈 Успішність: {success_rate:.1f}%")
        logger.info(f"👥 Аккаунтів: {len(groups_by_account)}")
        logger.info(f"📦 Груп: {total_groups}")
        
        # Завершуємо статус розсилки
        if status_id:
            try:
                db.update_broadcast_status(status_id, status='completed')
            except Exception as e:
                logger.error(f"❌ Помилка завершення статусу розсилки: {e}")
        
        # Показуємо результат
        result_text = f"""
📊 <b>Масову розсилку завершено!</b>

✅ <b>Відправлено:</b> {total_sent}
❌ <b>Помилок:</b> {total_failed}
📈 <b>Успішність:</b> {success_rate:.1f}%
👥 <b>Аккаунтів:</b> {len(groups_by_account)}
📦 <b>Груп:</b> {total_groups}

📋 <b>Деталі по аккаунтах:</b>
        """
        
        # Додаємо деталі по кожному аккаунту
        for account_phone, groups in groups_by_account.items():
            account_sent = 0
            account_failed = 0
            # Тут можна додати підрахунок по аккаунтах, якщо потрібно
            result_text += f"📱 {account_phone}: {len(groups)} груп\n"
        
        result_text += f"\n📊 <b>Статус розсилки:</b> Завершено"
        if status_id:
            result_text += f"\n🆔 <b>ID статусу:</b> {status_id}"
        
        await message_obj.answer(result_text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"❌ Критична помилка в масовій розсилці: {e}")
        logger.error(f"📊 Фінальна статистика: відправлено={total_sent if 'total_sent' in locals() else 0}, невдало={total_failed if 'total_failed' in locals() else 0}")
        await message_obj.answer(f"❌ Помилка в масовій розсилці: {e}")
        return

async def loop_broadcast_process(message_text, interval: int, use_random: bool, 
                               min_random: int, max_random: int, selected_package_id: int, message_obj,
                               message_interval: int = 10, use_random_message_interval: bool = False,
                               min_message_interval: int = 5, max_message_interval: int = 30,
                               account_messages: dict = None, stop_event: asyncio.Event = None,
                               message_type: str = None, media_file_path: str = None, media_file_id: str = None, selected_groups: list = None,
                               state: FSMContext = None, media_caption: str = None, cycle_interval_data: dict = None, account_interval_data: dict = None):
    """Процес циклічної розсилки"""
    
    logger.info("🚀 Початок циклічної розсилки")
    logger.info(f"📊 Параметри розсилки: interval={interval}, use_random={use_random}, package_id={selected_package_id}")
    logger.info(f"📱 Тип повідомлення: {message_type}, media_file_path={media_file_path}")
    
    await message_obj.answer("🔄 Циклічна розсилка запущена. Для зупинки використайте команду /stop_message")
    
    try:
        # Перевіряємо підключення до бази даних
        if db is None:
            logger.error("❌ База даних не ініціалізована!")
            await message_obj.answer("❌ Помилка: база даних не ініціалізована.")
            return
        
        logger.info("✅ Підключення до бази даних успішне")
        accounts = db.get_accounts()
        logger.info(f"📊 Отримано {len(accounts) if accounts else 0} аккаунтів з бази даних")
        
        if not accounts:
            logger.warning("⚠️ Немає зареєстрованих аккаунтів в базі даних")
            await message_obj.answer("❌ Немає зареєстрованих аккаунтів.")
            return
        
        # Якщо є налаштовані повідомлення для аккаунтів, фільтруємо аккаунти
        if account_messages:
            logger.info(f"🔧 Фільтруємо аккаунти за налаштованими повідомленнями: {len(account_messages)} аккаунтів")
            # Використовуємо тільки налаштовані аккаунти
            configured_accounts = [acc for acc in accounts if acc['phone_number'] in account_messages]
            accounts = configured_accounts
            logger.info(f"✅ Після фільтрації: {len(accounts)} аккаунтів")
        
        # Отримуємо басейни для розсилки
        logger.info(f"📦 Отримуємо групи для розсилки: selected_package_id={selected_package_id}, selected_groups={len(selected_groups) if selected_groups else 0}")
        if selected_groups:
            # Використовуємо басейни з параметра (включаючи одну групу)
            logger.info(f"📋 Використовуємо вибрані групи: {len(selected_groups)} груп")
            groups_to_send = selected_groups
        elif selected_package_id == "all_chats":
            # Всі чати на аккаунтах
            logger.info("📋 Отримуємо всі чати з аккаунтів")
            all_groups = []
            for account in accounts:
                try:
                    logger.info(f"🔍 Отримуємо чати для аккаунта: {account['phone_number']}")
                    groups = await get_all_chats_for_account(account['phone_number'])
                    all_groups.extend(groups)
                    logger.info(f"✅ Отримано {len(groups)} чатів для аккаунта {account['phone_number']}")
                except Exception as e:
                    logger.error(f"❌ Помилка при отриманні чатів для аккаунта {account['phone_number']}: {e}")
                    continue
            groups_to_send = all_groups
            logger.info(f"📊 Всього отримано {len(groups_to_send)} чатів з усіх аккаунтів")
        elif selected_package_id == 0:
            # Всі басейни
            logger.info("📋 Отримуємо всі басейни з усіх аккаунтів")
            all_groups = []
            for account in accounts:
                packages = db.get_group_packages(account['phone_number'])
                logger.info(f"📦 Аккаунт {account['phone_number']}: {len(packages)} пакетів")
                for package in packages:
                    groups = db.get_groups_by_package(package['id'])
                    all_groups.extend(groups)
                    logger.info(f"📋 Пакет {package['name']}: {len(groups)} груп")
            groups_to_send = all_groups
            logger.info(f"📊 Всього отримано {len(groups_to_send)} груп з усіх басейнів")
        else:
            # Конкретний пакет
            logger.info(f"📋 Отримуємо групи з конкретного пакету: {selected_package_id}")
            groups_to_send = db.get_groups_by_package(selected_package_id)
            logger.info(f"📊 Отримано {len(groups_to_send) if groups_to_send else 0} груп з пакету {selected_package_id}")
        
        # Перевіряємо чи це відправка в одну групу
        if len(groups_to_send) == 1 and groups_to_send[0].get('package_name') == 'Одна група':
            # Це відправка в одну групу - використовуємо всі налаштовані аккаунти
            logger.info("🎯 Відправка в одну групу - розподіляємо по всіх аккаунтах")
            single_group = groups_to_send[0]
            groups_by_account = {}
            for account in accounts:
                # Створюємо копію групи для кожного аккаунта
                group_copy = single_group.copy()
                group_copy['account_phone'] = account['phone_number']
                groups_by_account[account['phone_number']] = [group_copy]
            logger.info(f"📊 Розподілено одну групу по {len(groups_by_account)} аккаунтах")
        else:
            # Звичайна логіка для пакетів груп
            # Групуємо басейни по аккаунтах
            logger.info("📋 Групуємо групи по аккаунтах")
            groups_by_account = {}
            for group in groups_to_send:
                account_phone = group['account_phone']
                if account_phone not in groups_by_account:
                    groups_by_account[account_phone] = []
                groups_by_account[account_phone].append(group)
            logger.info(f"📊 Групи розподілені по {len(groups_by_account)} аккаунтах")
        
        if not groups_by_account:
            logger.error("❌ Немає груп для розсилки після обробки")
            await message_obj.answer("❌ Немає груп для розсилки.")
            return
        
        total_sent = 0
        total_failed = 0
        cycle_count = 0
        
        logger.info(f"🚀 Початок циклічної розсилки: {len(accounts)} аккаунтів, {len(groups_to_send)} груп")
    
        # Циклічна розсилка: кожен аккаунт відправляє по всіх своїх басейнах, потім перехід до наступного
        while True:
            # Перевіряємо флаг зупинки на початку кожного циклу
            if state:
                data = await state.get_data()
                if data.get('stop_broadcast', False):
                    logger.info("🛑 Отримано команду зупинки розсилки")
                    await message_obj.answer("🛑 Циклічна розсилка зупинена користувачем.")
                    break
            
            cycle_count += 1
            logger.info(f"🔄 Початок циклу #{cycle_count}")
            await message_obj.answer(f"🔄 <b>Початок циклу #{cycle_count}</b>\n\n"
                                   f"👥 <b>Аккаунтів:</b> {len(accounts)}\n"
                                   f"📦 <b>Груп:</b> {len(groups_to_send)}",
                                   parse_mode='HTML')
            
            # Проходимо по всіх аккаунтах
            for account_phone, groups in groups_by_account.items():
                # Перевіряємо флаг зупинки перед обробкою кожного аккаунта
                if state:
                    data = await state.get_data()
                    if data.get('stop_broadcast', False):
                        logger.info("🛑 Отримано команду зупинки розсилки")
                        await message_obj.answer("🛑 Циклічну розсилку зупинено користувачем.")
                        return
                
                try:
                    logger.info(f"📱 Обробляємо аккаунт: {account_phone} ({len(groups)} груп)")
                    # Отримуємо дані аккаунта
                    account = None
                    for acc in accounts:
                        if acc['phone_number'] == account_phone:
                            account = acc
                            break
                    
                    if not account:
                        logger.error(f"❌ Аккаунт {account_phone} не знайдено в списку аккаунтів")
                        continue
                    
                    await message_obj.answer(f"📱 <b>Розсилка з аккаунта:</b> {account_phone}\n"
                                           f"📦 <b>Груп для відправки:</b> {len(groups)}",
                                           parse_mode='HTML')
                    
                    # Створюємо клієнт
                    session_name = f"sessions/temp_{account_phone.replace('+', '').replace('-', '')}"
                    logger.info(f"🔗 Створюємо клієнт для аккаунта {account_phone}: {session_name}")
                    client = TelegramClient(session_name, account['api_id'], account['api_hash'])
                    
                    logger.info(f"🔌 Підключаємося до Telegram для аккаунта {account_phone}")
                    await client.connect()
                    
                    if not await client.is_user_authorized():
                        logger.error(f"❌ Аккаунт {account_phone} не авторизований")
                        await client.disconnect()
                        continue
                    
                    logger.info(f"✅ Аккаунт {account_phone} успішно авторизований")
                    
                    # Визначаємо повідомлення для цього аккаунта
                    if account_messages and account_phone in account_messages:
                        logger.info(f"📝 Використовуємо налаштоване повідомлення для аккаунта {account_phone}")
                        current_message = account_messages[account_phone]
                    else:
                        # Для загальної розсилки - перевіряємо чи це медіа
                        if message_type and media_file_path:
                            # Це медіа-повідомлення
                            logger.info(f"📎 Використовуємо медіа-повідомлення: {message_type}, файл: {media_file_path}")
                            current_message = {
                                'type': message_type,
                                'text': media_caption or '',  # Використовуємо підпис з параметра
                                'file_path': media_file_path,
                                'file_id': media_file_id  # file_id з параметрів функції
                            }
                        else:
                            # Це текстове повідомлення
                            logger.info(f"📝 Використовуємо текстове повідомлення для аккаунта {account_phone}")
                            current_message = message_text
                        
                    # Відправляємо повідомлення в групи цього аккаунта
                    logger.info(f"📤 Початок відправки повідомлень для аккаунта {account_phone}: {len(groups)} груп")
                    for j, group in enumerate(groups):
                        # Перевіряємо флаг зупинки перед кожною групою
                        if state:
                            data = await state.get_data()
                            if data.get('stop_broadcast', False):
                                logger.info("🛑 Отримано команду зупинки розсилки")
                                await message_obj.answer("🛑 Циклічну розсилку зупинено користувачем.")
                                await client.disconnect()
                                return
                        
                        max_retries = 3
                        logger.info(f"📋 Обробляємо групу {j+1}/{len(groups)}: {group['name']} (ID: {group['group_id']})")
                        for attempt in range(max_retries):
                            try:
                                group_id = int(group['group_id'])
                                
                                logger.info(f"🔍 Оригінальний ID з бази: {group['group_id']}, Конвертований: {group_id}")
                                
                                # Повідомляємо про початок відправки
                                await message_obj.answer(f"📤 <b>Відправляємо повідомлення:</b>\n\n"
                                                       f"📱 <b>Аккаунт:</b> {account_phone}\n"
                                                       f"📝 <b>Група:</b> {group['name']}\n"
                                                       f"🆔 <b>ID басейни:</b> {group_id}\n"
                                                       f"📈 <b>Прогрес:</b> {j+1}/{len(groups)}",
                                                       parse_mode='HTML')
                                
                                logger.info(f"📤 Спроба {attempt + 1}/{max_retries} відправки в групу {group['name']} (ID: {group_id})")
                                success = await db.send_message_with_retry(
                                    client, 
                                    str(group_id), 
                                    group['name'], 
                                    current_message,
                                    message_obj
                                )
                                
                                if success:
                                    total_sent += 1
                                    logger.info(f"✅ Повідомлення успішно відправлено в групу {group['name']} ({group_id})")
                                    logger.info(f"📊 Статистика: відправлено={total_sent}, невдало={total_failed}")
                                else:
                                    total_failed += 1
                                    logger.warning(f"⚠️ Не вдалося відправити повідомлення в групу {group['name']} ({group_id})")
                                    logger.info(f"📊 Статистика: відправлено={total_sent}, невдало={total_failed}")
                                    
                                # Затримка між повідомленнями
                                if j < len(groups) - 1:  # Не чекаємо після останнього повідомлення
                                    # Перевіряємо флаг зупинки перед затримкою
                                    if state:
                                        data = await state.get_data()
                                        if data.get('stop_broadcast', False):
                                            logger.info("🛑 Отримано команду зупинки розсилки")
                                            await message_obj.answer("🛑 Циклічну розсилку зупинено користувачем.")
                                            await client.disconnect()
                                            return
                                    
                                    if use_random_message_interval:
                                        delay = random.randint(min_message_interval, max_message_interval)
                                        await message_obj.answer(f"⏳ <b>Затримка між повідомленнями:</b>\n\n"
                                                               f"🕐 <b>Чекаємо:</b> {delay} секунд\n"
                                                               f"📊 <b>Діапазон:</b> {min_message_interval}-{max_message_interval} сек\n"
                                                               f"📝 <b>Група:</b> {group['name']}\n"
                                                               f"📈 <b>Прогрес:</b> {j+1}/{len(groups)}",
                                                               parse_mode='HTML')
                                    else:
                                        delay = message_interval
                                        await message_obj.answer(f"⏳ <b>Затримка між повідомленнями:</b>\n\n"
                                                               f"🕐 <b>Чекаємо:</b> {delay} секунд\n"
                                                               f"📝 <b>Група:</b> {group['name']}\n"
                                                               f"📈 <b>Прогрес:</b> {j+1}/{len(groups)}",
                                                               parse_mode='HTML')
                                    
                                    await asyncio.sleep(delay)
                                
                                break  # Успішно відправлено, виходимо з циклу retry
                                    
                            except FloodWaitError as flood_error:
                                # FloodWait обробляється в database.py
                                total_failed += 1
                                logger.error(f"❌ FloodWait Error в циклічній розсилці: {flood_error}")
                                logger.error(f"⏳ FloodWait: {flood_error.seconds} секунд для групи {group['name']}")
                                logger.info(f"Чекаємо {flood_error.seconds} секунд")
                                logger.info(f"📊 Статистика після FloodWait: відправлено={total_sent}, невдало={total_failed}")
                                break
                                    
                            except Exception as e:
                                error_msg = str(e)
                                logger.warning(f"⚠️ Спроба {attempt + 1}/{max_retries} невдала для групи {group['name']}: {error_msg}")
                                
                                if "Could not find the input entity" in error_msg:
                                    logger.error(f"❌ Група {group['name']} (ID: {group_id}) не існує або недоступна для аккаунта {account_phone}")
                                    logger.error(f"💡 Можливі причини: група видалена, аккаунт заблокований, група стала приватною")
                                    await message_obj.answer(f"❌ <b>Група недоступна:</b>\n\n"
                                                           f"📝 <b>Група:</b> {group['name']}\n"
                                                           f"🆔 <b>ID:</b> {group_id}\n"
                                                           f"📱 <b>Аккаунт:</b> {account_phone}\n\n"
                                                           f"💡 <b>Можливі причини:</b>\n"
                                                           f"• Група була видалена\n"
                                                           f"• Аккаунт заблокований в групі\n"
                                                           f"• Група стала приватною",
                                                           parse_mode='HTML')
                                    total_failed += 1
                                    logger.info(f"📊 Статистика після помилки: відправлено={total_sent}, невдало={total_failed}")
                                    break
                                elif attempt < max_retries - 1:
                                    retry_delay = random.randint(5, 15)
                                    logger.info(f"⏳ Повторна спроба через {retry_delay} секунд")
                                    await asyncio.sleep(retry_delay)
                                    continue
                                else:
                                    total_failed += 1
                                    logger.error(f"❌ Помилка при відправці в групу {group['name']} після {max_retries} спроб: {e}")
                                    logger.info(f"📊 Статистика після всіх спроб: відправлено={total_sent}, невдало={total_failed}")
                                    break
                        
                    logger.info(f"🔌 Відключаємо клієнт для аккаунта {account_phone}")
                    await client.disconnect()
                    
                    # Затримка між аккаунтами (тільки якщо не останній аккаунт)
                    if account_phone != list(groups_by_account.keys())[-1]:
                        # Перевіряємо флаг зупинки перед затримкою між аккаунтами
                        if state:
                            data = await state.get_data()
                            if data.get('stop_broadcast', False):
                                logger.info("🛑 Отримано команду зупинки розсилки")
                                await message_obj.answer("🛑 Циклічну розсилку зупинено користувачем.")
                                return
                        
                        # Використовуємо збережені дані інтервалу між аккаунтами
                        if account_interval_data:
                            if account_interval_data.get('use_random'):
                                delay = random.randint(account_interval_data.get('min_random', 30), account_interval_data.get('max_random', 120))
                                logger.info(f"🎲 Рандомний інтервал між аккаунтами: {delay} сек (діапазон: {account_interval_data.get('min_random')}-{account_interval_data.get('max_random')})")
                            else:
                                delay = account_interval_data.get('interval', interval)
                                logger.info(f"⏳ Фіксований інтервал між аккаунтами: {delay} сек")
                        else:
                            # Fallback до FSM даних
                            data = await state.get_data()
                            delay = data.get('interval', interval)
                            logger.info(f"⏳ Fallback інтервал між аккаунтами: {delay} сек (data.interval={data.get('interval')}, param.interval={interval})")
                        await message_obj.answer(f"⏳ <b>Затримка між аккаунтами:</b>\n\n"
                                               f"🕐 <b>Чекаємо:</b> {delay} секунд\n"
                                               f"📱 <b>Наступний аккаунт:</b> {list(groups_by_account.keys())[list(groups_by_account.keys()).index(account_phone) + 1]}",
                                               parse_mode='HTML')
                        await asyncio.sleep(delay)
                    
                    logger.info(f"✅ Завершено обробку аккаунта {account_phone}")
                except Exception as e:
                    logger.error(f"❌ Помилка при обробці аккаунта {account_phone}: {e}")
                    logger.info(f"📊 Статистика після помилки аккаунта: відправлено={total_sent}, невдало={total_failed}")
                    continue
            
            # Затримка між циклами (рандомна від 10 до 120 секунд)
            # Перевіряємо флаг зупинки перед затримкою між циклами
            if state:
                data = await state.get_data()
                if data.get('stop_broadcast', False):
                    logger.info("🛑 Отримано команду зупинки розсилки")
                    await message_obj.answer("🛑 Циклічна розсилка зупинена користувачем.")
                    break
            
            # Використовуємо збережені дані інтервалу циклу
            if cycle_interval_data:
                logger.info(f"🔍 Дані інтервалу циклу: use_random_cycle_interval={cycle_interval_data.get('use_random_cycle_interval')}, cycle_interval={cycle_interval_data.get('cycle_interval')}, cycle_interval_min={cycle_interval_data.get('cycle_interval_min')}, cycle_interval_max={cycle_interval_data.get('cycle_interval_max')}")
                
                # Генеруємо інтервал для наступного циклу
                if cycle_interval_data.get('use_random_cycle_interval'):
                    # Використовуємо рандомний інтервал циклу
                    cycle_min = cycle_interval_data.get('cycle_interval_min', 30)
                    cycle_max = cycle_interval_data.get('cycle_interval_max', 120)
                    delay = random.randint(cycle_min, cycle_max)
                    logger.info(f"🎲 Згенеровано рандомний інтервал між циклами: {delay} секунд (діапазон: {cycle_min}-{cycle_max})")
                else:
                    # Використовуємо фіксований інтервал
                    delay = cycle_interval_data.get('cycle_interval', 60)
                    logger.info(f"⏳ Фіксований інтервал між циклами: {delay} секунд")
            else:
                # Fallback до FSM даних (якщо cycle_interval_data не передано)
                data = await state.get_data()
                logger.info(f"🔍 Fallback до FSM даних: use_random_cycle_interval={data.get('use_random_cycle_interval')}, cycle_interval={data.get('cycle_interval')}")
                delay = data.get('cycle_interval', data.get('interval', 60))
                logger.info(f"⏳ Fallback інтервал між циклами: {delay} секунд")
            
            logger.info(f"📊 Статистика циклу #{cycle_count}: відправлено={total_sent}, невдало={total_failed}")
            
            # Визначаємо чи використовується рандомний інтервал для повідомлення
            use_random_for_message = False
            if cycle_interval_data:
                use_random_for_message = cycle_interval_data.get('use_random_cycle_interval', False)
            else:
                data = await state.get_data()
                use_random_for_message = data.get('use_random_cycle_interval', False)
            
            if use_random_for_message:
                cycle_min = cycle_interval_data.get('cycle_interval_min', 30) if cycle_interval_data else 30
                cycle_max = cycle_interval_data.get('cycle_interval_max', 120) if cycle_interval_data else 120
                await message_obj.answer(f"⏳ <b>Затримка між циклами:</b>\n\n"
                                       f"🕐 <b>Чекаємо:</b> {delay} секунд\n"
                                       f"🎲 <b>Рандомний інтервал:</b> {cycle_min}-{cycle_max} сек\n"
                                       f"🔄 <b>Наступний цикл:</b> #{cycle_count + 1}",
                                       parse_mode='HTML')
            else:
                await message_obj.answer(f"⏳ <b>Затримка між циклами:</b>\n\n"
                                       f"🕐 <b>Чекаємо:</b> {delay} секунд\n"
                                       f"🔄 <b>Наступний цикл:</b> #{cycle_count + 1}",
                                       parse_mode='HTML')
            
            await asyncio.sleep(delay)
        
    except Exception as e:  # except для try блоку на лінії 1586
        logger.error(f"❌ Критична помилка в циклічній розсилці: {e}")
        logger.error(f"📊 Фінальна статистика: відправлено={total_sent if 'total_sent' in locals() else 0}, невдало={total_failed if 'total_failed' in locals() else 0}")
        await message_obj.answer(f"❌ Помилка в циклічній розсилці: {e}")
        return
    
    logger.info("🏁 Циклічна розсилка завершена")
    logger.info(f"📊 Фінальна статистика: відправлено={total_sent}, невдало={total_failed}")

@router.message(Command("stop_message"))
async def stop_message_command(message: Message, state: FSMContext):
    """Команда зупинки масової розсилки"""
    # Встановлюємо флаг зупинки в FSM state
    await state.update_data(stop_broadcast=True)
    await message.answer("🛑 <b>Команда зупинки розсилки отримана!</b>\n\n"
                        "⏳ Активні розсилки будуть зупинені:\n"
                        "• Після завершення поточного повідомлення\n"
                        "• Перед наступною затримкою\n"
                        "• Перед обробкою наступного аккаунта\n"
                        "• Перед наступним циклом (для циклічної розсилки)\n\n"
                        "📊 Статус зупинки буде показано в наступних повідомленнях.",
                        parse_mode='HTML')

