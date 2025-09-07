import logging
from aiogram import Router
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from states import GroupStates, DeletePackageStates

# Налаштування логування
logger = logging.getLogger(__name__)

# Створюємо роутер для груп
router = Router()

# Глобальна змінна (буде імпортована з основного файлу)
db = None

def init_groups_module(database):
    """Ініціалізація модуля груп"""
    global db
    db = database

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
        await message.answer("❌ Всі групи зі списку вже додані до бази даних.")
        await state.clear()
        return
    
    # Додаємо групи для всіх аккаунтів
    total_added = 0
    total_accounts = len(accounts)
    
    for account in accounts:
        account_phone = account['phone_number']
        added_for_account = 0
        
        for group_id in unique_groups:
            # Створюємо назву групи
            group_name = f"Група {group_id.replace('-100', '')}"
            
            # Додаємо групу до бази даних
            success = db.add_group(group_name, group_id, None, account_phone, package_id)
            if success:
                added_for_account += 1
                total_added += 1
        
        logger.info(f"✅ Додано {added_for_account} груп для аккаунта {account_phone}")
    
    # Показуємо результат
    result_text = f"""
✅ <b>Пакет груп '{package_name}' створено успішно!</b>

📦 <b>Пакет:</b> {package_name}
👥 <b>Аккаунтів:</b> {total_accounts}
📋 <b>Унікальних груп:</b> {len(unique_groups)}
✅ <b>Всього додано:</b> {total_added} записів

📊 <b>Статистика:</b>
• Груп на аккаунт: {len(unique_groups)}
• Всього записів: {total_added}
    """
    
    await message.answer(result_text, parse_mode='HTML')
    await state.clear()

# Функції для видалення пакетів груп
@router.callback_query(lambda c: c.data == "delete_group_package")
async def delete_group_package_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка натискання кнопки видалення пакету груп"""
    await callback.message.answer("🗑️ Введіть назву пакету груп для видалення:")
    await state.set_state(DeletePackageStates.waiting_for_package_name)
    await callback.answer()

@router.message(DeletePackageStates.waiting_for_package_name)
async def process_delete_package_name(message: Message, state: FSMContext):
    """Обробка назви пакету для видалення"""
    package_name = message.text.strip()
    
    # Отримуємо всі пакети груп
    packages = db.get_all_group_packages()
    
    # Знаходимо пакети з такою назвою
    matching_packages = [p for p in packages if p['name'].lower() == package_name.lower()]
    
    if not matching_packages:
        await message.answer(f"❌ Пакет '{package_name}' не знайдено.")
        await state.clear()
        return
    
    # Показуємо знайдені пакети
    packages_text = f"🔍 <b>Знайдені пакети з назвою '{package_name}':</b>\n\n"
    
    for i, package in enumerate(matching_packages, 1):
        packages_text += f"{i}. 📦 <b>{package['name']}</b>\n"
        packages_text += f"   👤 <b>Аккаунт:</b> {package['account_phone']}\n"
        packages_text += f"   📅 <b>Створено:</b> {package['created_at']}\n\n"
    
    # Створюємо кнопки для підтвердження видалення
    keyboard_buttons = []
    for i, package in enumerate(matching_packages, 1):
        keyboard_buttons.append([InlineKeyboardButton(
            text=f"🗑️ Видалити пакет {i}",
            callback_data=f"confirm_delete_package_{package['id']}"
        )])
    
    keyboard_buttons.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_delete")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await message.answer(packages_text, parse_mode='HTML', reply_markup=keyboard)
    await state.clear()

@router.callback_query(lambda c: c.data.startswith("confirm_delete_package_"))
async def confirm_delete_package_callback(callback: CallbackQuery):
    """Підтвердження видалення пакету груп"""
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
        await callback.message.answer(f"✅ Пакет '{package['name']}' успішно видалено!")
    else:
        await callback.message.answer("❌ Помилка при видаленні пакету.")
    
    await callback.answer()
