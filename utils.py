import asyncio
import random
import os
import logging
from aiogram.fsm.context import FSMContext
from states import RegistrationStates

# Налаштування логування
logger = logging.getLogger(__name__)

# Глобальні змінні для емодзі та стикерів
RANDOM_EMOJIS = ['😊', '👍', '🔥', '💯', '✨', '🎉', '🚀', '💪', '⭐', '❤️', '😍', '🤩', '💖', '🌟', '🎊', '🎈', '🎁', '🏆', '🥇', '💎']

def add_random_emoji_to_text(text: str) -> str:
    """Додає випадкові емоції до тексту"""
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
    # Випадкова пауза від 1 до 3 секунд
    pause_time = random.uniform(1.0, 3.0)
    await asyncio.sleep(pause_time)

def should_send_sticker() -> bool:
    """Визначає чи потрібно відправити стикер замість тексту"""
    # 10% шанс відправити стикер
    return random.random() < 0.1

def get_media_type_from_file(file_path: str) -> str:
    """Визначає тип медіа-файлу за розширенням"""
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
    # 70% шанс додати емоції до підпису
    return random.random() < 0.7

def should_send_sticker_with_media() -> bool:
    """Визначає чи відправляти стикер разом з медіа"""
    # 15% шанс відправити стикер разом з медіа
    return random.random() < 0.15

# ========== СПІЛЬНІ ФУНКЦІЇ ==========

async def show_accounts_list(message_or_callback, db):
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
