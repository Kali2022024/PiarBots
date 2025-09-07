import asyncio
import logging
import random
from aiogram import Router
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from states import JoinGroupsStates

# Налаштування логування
logger = logging.getLogger(__name__)

# Створюємо роутер для приєднання до груп
router = Router()

# Глобальні змінні (будуть імпортовані з основного файлу)
db = None

def init_join_groups_module(database):
    """Ініціалізація модуля приєднання до груп"""
    global db
    db = database

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
    
    await start_join_groups_process(callback, state)
    await callback.answer()

@router.message(JoinGroupsStates.waiting_for_random_interval_config)
async def process_custom_random_interval(message: Message, state: FSMContext):
    """Обробка власного діапазону рандомного інтервалу"""
    try:
        interval_text = message.text.strip()
        
        # Перевіряємо формат min,max
        if ',' not in interval_text:
            await message.answer("❌ Використовуйте формат min,max (наприклад: 15,90). Спробуйте ще раз:")
            return
        
        min_str, max_str = interval_text.split(',', 1)
        min_interval = int(min_str.strip())
        max_interval = int(max_str.strip())
        
        # Валідуємо діапазон
        if not (5 <= min_interval <= 3600 and 5 <= max_interval <= 3600):
            await message.answer("❌ Діапазон повинен бути від 5 до 3600 секунд. Спробуйте ще раз:")
            return
        
        if min_interval >= max_interval:
            await message.answer("❌ Мінімальне значення повинно бути менше максимального. Спробуйте ще раз:")
            return
        
        # Зберігаємо налаштування
        await state.update_data(
            interval="_random_interval",
            min_random_interval=min_interval,
            max_random_interval=max_interval
        )
        
        await start_join_groups_process(message, state)
        
    except ValueError:
        await message.answer("❌ Введіть числа в форматі min,max (наприклад: 15,90). Спробуйте ще раз:")

async def start_join_groups_process(message_or_callback, state: FSMContext):
    """Початок процесу приєднання до груп"""
    data = await state.get_data()
    account_phone = data['selected_account']
    group_ids = data['group_ids']
    interval = data.get('interval', 10)
    
    # Показуємо підтвердження
    confirmation_text = f"📤 <b>Підтвердження приєднання до груп:</b>\n\n"
    confirmation_text += f"📱 <b>Аккаунт:</b> {account_phone}\n"
    confirmation_text += f"👥 <b>Груп для приєднання:</b> {len(group_ids)}\n"
    
    if interval == "_random_interval":
        min_interval = data.get('min_random_interval', 10)
        max_interval = data.get('max_random_interval', 40)
        confirmation_text += f"🎲 <b>Інтервал:</b> Рандомний ({min_interval}-{max_interval} сек)\n"
    else:
        confirmation_text += f"⏱️ <b>Інтервал:</b> {interval} секунд\n"
    
    confirmation_text += f"\n📋 <b>Список груп:</b>\n"
    for i, group_id in enumerate(group_ids[:5], 1):  # Показуємо тільки перші 5
        confirmation_text += f"{i}. {group_id}\n"
    
    if len(group_ids) > 5:
        confirmation_text += f"... та ще {len(group_ids) - 5} груп\n"
    
    confirmation_text += "\n🚀 Підтвердити приєднання?"
    
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
    interval = data.get('interval', 10)
    
    # Підготовка даних для інтервалу
    interval_data = None
    if interval == "_random_interval":
        interval_data = {
            'min': data.get('min_random_interval', 10),
            'max': data.get('max_random_interval', 40)
        }
    
    await callback.message.answer("🚀 Приєднання до груп запущено! Процес може зайняти деякий час.")
    
    # Запускаємо процес приєднання в фоновому режимі
    asyncio.create_task(join_groups_process(account_phone, group_ids, interval, callback.message, interval_data))
    
    await state.clear()
    await callback.answer()

async def join_groups_process(account_phone: str, group_ids: list, interval_type, message_obj, interval_data=None):
    """Процес приєднання до груп"""
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
        
        joined_count = 0
        failed_count = 0
        already_joined = 0
        
        # Створюємо пакет для збереження груп
        package_name = f"Автоматичний пакет {account_phone}"
        package_id = db.create_group_package(package_name, account_phone)
        
        async def wait_between_groups(current_index, total_groups):
            """Функція для очікування між приєднаннями"""
            if current_index < total_groups - 1:  # Не чекаємо після останньої групи
                if interval_type == "_random_interval" and interval_data:
                    wait_time = random.randint(interval_data['min'], interval_data['max'])
                else:
                    wait_time = interval_type
                
                await message_obj.answer(f"⏳ Очікування {wait_time} секунд перед наступною групою...")
                await asyncio.sleep(wait_time)
        
        for i, group_input in enumerate(group_ids):
            try:
                await message_obj.answer(f"🔄 Обробка групи {i+1}/{len(group_ids)}: {group_input}")
                
                # Обробляємо різні формати груп
                if group_input.isdigit():
                    # ID групи
                    group_id = f"-100{group_input}"
                    try:
                        entity = await client.get_entity(int(group_id))
                        group_name = getattr(entity, 'title', f'Група {group_input}')
                        
                        # Перевіряємо чи вже приєднані
                        if db.group_exists_for_account(group_id, account_phone):
                            already_joined += 1
                            await message_obj.answer(f"ℹ️ Група {group_name} вже додана до аккаунта")
                        else:
                            # Приєднуємося до групи
                            await client(JoinChannelRequest(entity))
                            joined_count += 1
                            
                            # Додаємо групу до бази даних
                            db.add_group(group_name, group_id, None, account_phone, package_id)
                            await message_obj.answer(f"✅ Успішно приєднано до групи {group_name}")
                    
                    except Exception as entity_error:
                        failed_count += 1
                        await message_obj.answer(f"❌ Помилка обробки групи {group_input}: {str(entity_error)[:100]}")
                
                elif group_input.startswith('@'):
                    # Username
                    try:
                        entity = await client.get_entity(group_input)
                        group_name = getattr(entity, 'title', group_input)
                        group_id = str(entity.id)
                        
                        # Перевіряємо чи вже приєднані
                        if db.group_exists_for_account(group_id, account_phone):
                            already_joined += 1
                            await message_obj.answer(f"ℹ️ Група {group_name} вже додана до аккаунта")
                        else:
                            # Приєднуємося до групи
                            await client(JoinChannelRequest(entity))
                            joined_count += 1
                            
                            # Додаємо групу до бази даних
                            db.add_group(group_name, group_id, group_input, account_phone, package_id)
                            await message_obj.answer(f"✅ Успішно приєднано до групи {group_name}")
                    
                    except Exception as entity_error:
                        failed_count += 1
                        await message_obj.answer(f"❌ Помилка обробки групи {group_input}: {str(entity_error)[:100]}")
                
                elif group_input.startswith('https://t.me/'):
                    # Посилання
                    try:
                        if '/joinchat/' in group_input:
                            # Invite посилання
                            invite_hash = group_input.split('/joinchat/')[-1]
                            entity = await client(ImportChatInviteRequest(invite_hash))
                            group_name = getattr(entity, 'title', 'Група з invite')
                            group_id = str(entity.id)
                        else:
                            # Звичайне посилання
                            username = group_input.replace('https://t.me/', '').replace('@', '')
                            entity = await client.get_entity(f"@{username}")
                            group_name = getattr(entity, 'title', username)
                            group_id = str(entity.id)
                        
                        # Перевіряємо чи вже приєднані
                        if db.group_exists_for_account(group_id, account_phone):
                            already_joined += 1
                            await message_obj.answer(f"ℹ️ Група {group_name} вже додана до аккаунта")
                        else:
                            # Приєднуємося до групи
                            await client(JoinChannelRequest(entity))
                            joined_count += 1
                            
                            # Додаємо групу до бази даних
                            db.add_group(group_name, group_id, f"@{username}", account_phone, package_id)
                            await message_obj.answer(f"✅ Успішно приєднано до групи {group_name}")
                    
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
