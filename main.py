import asyncio
import logging
import os
import sqlite3
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from config import TELEGRAM_BOT_TOKEN

# Налаштування логування
logger = logging.getLogger(__name__)

# Імпортуємо наші модулі
from database import Database
from states import *
from utils import show_accounts_list
from registration import router as registration_router, init_registration_module
from groups import router as groups_router, init_groups_module
from mass_broadcast import router as mass_broadcast_router, init_mass_broadcast_module
from join_groups import router as join_groups_router, init_join_groups_module
from templates import TemplateManager
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

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

# Ініціалізація бота та диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

# Ініціалізація бази даних
db = Database()

# Ініціалізація менеджера шаблонів
template_manager = TemplateManager(bot)

# Список ID адміністраторів
ADMIN_IDS = [1904902463,
    # Додайте сюди ID адміністраторів
    # Приклад: 123456789, 987654321
    # 
    # Як отримати свій ID:
    # 1. Напишіть боту @userinfobot
    # 2. Скопіюйте ваш ID
    # 3. Додайте його в цей список
    # 4. Перезапустіть бота
]

def is_admin(user_id: int) -> bool:
    """Перевірити чи користувач є адміністратором"""
    return user_id in ADMIN_IDS

def admin_only(message: Message) -> bool:
    """Перевірити чи користувач є адміністратором"""
    return is_admin(message.from_user.id)

def admin_only_callback(callback: CallbackQuery) -> bool:
    """Перевірити чи користувач є адміністратором для callback"""
    return is_admin(callback.from_user.id)


# Ініціалізація модулів
init_registration_module(API_ID, API_HASH, db)
init_groups_module(db)
init_mass_broadcast_module(db, bot)
init_join_groups_module(db)


main_menu_button = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🤖Главное меню")],
        [KeyboardButton(text="🤖 Статус бота"), KeyboardButton(text="📤Функціональна розсилка")],
        [KeyboardButton(text="📋 Шаблони повідомлень"), KeyboardButton(text="📊 Статус розсилання")],
        [KeyboardButton(text="🛑 Зупинити розсилки"), KeyboardButton(text="🛑 Зупинити розсилку аккаунта")]
    ],
    resize_keyboard=True
)
# Імпортуємо RANDOM_STICKERS в database модуль
import database
RANDOM_STICKERS = database.RANDOM_STICKERS

@router.message(Command("start_bot"))
async def cmd_start(message: Message):
    """Обробник команди /start"""
    if not admin_only(message):
        return
    welcome_text = f"""
🍽️ Ласкаво просимо до Piar Bot

🆔 Ваш ID: {message.from_user.id}

Я бот для реклами в telegram)

💡 Доступні команди:
/start - Почати роботу
/register_number - Привязать номер телефона
/accounts - Список аккаунтів
/delete_account - видалити аккаунт
/stop_message - зупинити всі розсилки
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Статус бота", callback_data="bot_status")],
        [InlineKeyboardButton(text="📋 Список аккаунтів", callback_data="accounts")],
        [InlineKeyboardButton(text="📱 Зареєструвати номер", callback_data="register_number")],
        [InlineKeyboardButton(text="🗑️ Видалити аккаунт", callback_data="delete_account")],
        [InlineKeyboardButton(text="👥 Додати список груп", callback_data="Groups_for_account")],
        [InlineKeyboardButton(text="➕ Додатись в групи", callback_data="join_groups")],
        [InlineKeyboardButton(text="🗑️ Видалити пакет груп", callback_data="delete_group_package")],
        [InlineKeyboardButton(text="📊 Статус розсилання", callback_data="monitor_broadcasts")],
        [InlineKeyboardButton(text="📤 Функціональна роозсилка", callback_data="Mass_broadcast")],
        [InlineKeyboardButton(text="🛑 Зупинити розсилки", callback_data="stop_all_broadcasts")],
        [InlineKeyboardButton(text="🛑 Зупинити розсилку аккаунта", callback_data="stop_account_broadcast")],
        [InlineKeyboardButton(text="📋 Шаблони повідомлень", callback_data="templates_menu")]
    ])
    await message.answer('Оберіть дію', reply_markup=main_menu_button)
    await message.answer(welcome_text, reply_markup=keyboard)



@router.message(lambda m: m.text == "🤖Главное меню")
async def main_menu_button_message(message: Message):
    if not admin_only(message):
        return
    await cmd_start(message)

@router.message(lambda m: m.text == "📊 Статус розсилання")
async def monitor_broadcasts_button_message(message: Message):
    if not admin_only(message):
        return
    class FakeCallback:
        def __init__(self, msg):
            self.message = msg
            self.data = "monitor_broadcasts"
            self.from_user = msg.from_user
            self.id = msg.message_id
        async def answer(self, text=None, show_alert=False): 
            pass
    fake_callback = FakeCallback(message)
    await monitor_broadcasts_callback(fake_callback)


@router.message(lambda m: m.text == "🤖 Статус бота")
async def bot_status_button_message(message: Message):
    """Показати статус бота через кнопку"""
    if not admin_only(message):
        return
    class FakeCallback:
        def __init__(self, msg):
            self.message = msg
            self.data = "bot_status"
            self.from_user = msg.from_user
            self.id = msg.message_id
        async def answer(self, text=None, show_alert=False): 
            pass
    fake_callback = FakeCallback(message)
    await bot_status_callback(fake_callback)


from mass_broadcast import mass_broadcast_callback, get_problematic_accounts

def get_bot_status() -> dict:
    """Отримати статус бота"""
    status = {
        'database': False,
        'total_accounts': 0,
        'active_accounts': 0,
        'problematic_accounts': 0,
        'overall_status': 'unknown'
    }
    
    try:
        # Перевіряємо підключення до бази даних
        accounts = db.get_accounts()
        status['database'] = True
        status['total_accounts'] = len(accounts)
        
        # Перевіряємо активні аккаунти
        active_accounts = 0
        for account in accounts:
            phone = account['phone_number']
            if db.is_account_broadcasting(phone):
                active_accounts += 1
        
        status['active_accounts'] = active_accounts
        
        # Перевіряємо проблемні аккаунти
        problematic_accounts = get_problematic_accounts()
        status['problematic_accounts'] = len(problematic_accounts)
        
        # Визначаємо загальний статус
        if status['database'] and status['total_accounts'] > 0:
            if status['problematic_accounts'] == 0:
                status['overall_status'] = 'excellent'  # Відмінно
            elif status['problematic_accounts'] <= status['total_accounts'] // 2:
                status['overall_status'] = 'good'  # Добре
            else:
                status['overall_status'] = 'warning'  # Попередження
        else:
            status['overall_status'] = 'error'  # Помилка
            
    except Exception as e:
        logger.error(f"❌ Помилка при перевірці статусу бота: {e}")
        status['overall_status'] = 'error'
    
    return status

@router.callback_query(lambda c: c.data == "bot_status")
async def bot_status_callback(callback: CallbackQuery):
    """Показати статус бота"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    try:
        status = get_bot_status()
        
        # Визначаємо емодзі та колір для загального статусу
        status_emoji = {
            'excellent': '🟢',
            'good': '🟡', 
            'warning': '🟠',
            'error': '🔴',
            'unknown': '⚪'
        }
        
        status_text = {
            'excellent': 'Відмінно',
            'good': 'Добре',
            'warning': 'Попередження', 
            'error': 'Помилка',
            'unknown': 'Невідомо'
        }
        
        # Формуємо повідомлення
        message = f"""
🤖 <b>Статус бота</b>

{status_emoji.get(status['overall_status'], '⚪')} <b>Загальний статус:</b> {status_text.get(status['overall_status'], 'Невідомо')}

📊 <b>Детальна інформація:</b>
• База даних: {'✅ Підключена' if status['database'] else '❌ Не підключена'}
• Всього аккаунтів: {status['total_accounts']}
• Активних розсилок: {status['active_accounts']}
• Проблемних аккаунтів: {status['problematic_accounts']}

📈 <b>Статистика:</b>
• Робочих аккаунтів: {status['total_accounts'] - status['problematic_accounts']}
• Відсоток робочих: {((status['total_accounts'] - status['problematic_accounts']) / max(status['total_accounts'], 1) * 100):.1f}%
"""
        
        # Додаємо рекомендації залежно від статусу
        if status['overall_status'] == 'excellent':
            message += "\n✅ <b>Всі системи працюють ідеально!</b>"
        elif status['overall_status'] == 'good':
            message += "\n🟡 <b>Система працює добре, але є незначні проблеми</b>"
        elif status['overall_status'] == 'warning':
            message += "\n🟠 <b>Увага! Є проблеми з деякими аккаунтами</b>\n"
            message += "• Перевірте підключення до інтернету\n"
            message += "• Перезапустіть бота\n"
            message += "• Перевірте налаштування проблемних аккаунтів"
        elif status['overall_status'] == 'error':
            message += "\n🔴 <b>Критичні проблеми!</b>\n"
            message += "• Перевірте підключення до бази даних\n"
            message += "• Перезапустіть бота\n"
            message += "• Зверніться до адміністратора"
        
        # Створюємо клавіатуру з кнопками
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Оновити статус", callback_data="bot_status")],
            [InlineKeyboardButton(text="🔍 Детальний статус аккаунтів", callback_data="detailed_accounts_status")],
            [InlineKeyboardButton(text="👈 Назад", callback_data="back_to_main")]
        ])
        
        await callback.message.answer(message, parse_mode='HTML', reply_markup=keyboard)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"❌ Помилка при показі статусу бота: {e}")
        await callback.message.answer(
            "❌ <b>Помилка при отриманні статусу</b>\n\n"
            "Спробуйте пізніше або зверніться до адміністратора.",
            parse_mode='HTML'
        )
        await callback.answer()

@router.callback_query(lambda c: c.data == "detailed_accounts_status")
async def detailed_accounts_status_callback(callback: CallbackQuery):
    """Показати детальний статус аккаунтів"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    try:
        accounts = db.get_accounts()
        problematic_accounts = get_problematic_accounts()
        
        message = "🔍 <b>Детальний статус аккаунтів</b>\n\n"
        
        if not accounts:
            message += "❌ Аккаунти не знайдені в базі даних"
        else:
            for i, account in enumerate(accounts, 1):
                phone = account['phone_number']
                first_name = account.get('first_name', '')
                last_name = account.get('last_name', '')
                name = f"{first_name} {last_name}".strip() or phone
                
                is_broadcasting = db.is_account_broadcasting(phone)
                is_problematic = phone in problematic_accounts
                
                if is_problematic:
                    status_icon = "🔴"
                    status_text = "не працює"
                elif is_broadcasting:
                    status_icon = "🟡"
                    status_text = "розсилка"
                else:
                    status_icon = "🟢"
                    status_text = "готовий"
                
                message += f"{i}. {status_icon} <b>{name}</b>\n"
                message += f"   📱 {phone}\n"
                message += f"   📊 Статус: {status_text}\n\n"
        
        # Додаємо кнопки
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Оновити", callback_data="detailed_accounts_status")],
            [InlineKeyboardButton(text="👈 Назад до статусу", callback_data="bot_status")]
        ])
        
        await callback.message.answer(message, parse_mode='HTML', reply_markup=keyboard)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"❌ Помилка при показі детального статусу: {e}")
        await callback.message.answer(
            "❌ <b>Помилка при отриманні детального статусу</b>",
            parse_mode='HTML'
        )
        await callback.answer()

@router.message(lambda m: m.text == "📤Функціональна розсилка")
async def mass_broadcast_button_message(message: Message, state: FSMContext):
    if not admin_only(message):
        return
    # створюємо фейковий CallbackQuery з message
    class FakeCallback:
        def __init__(self, msg):
            self.message = msg
            self.data = "mass_broadcast"
            self.from_user = msg.from_user
            self.id = msg.message_id
        async def answer(self, text=None, show_alert=False): 
            pass
    
    fake_callback = FakeCallback(message)
    await mass_broadcast_callback(fake_callback, state)

@router.message(lambda m: m.text == "🛑 Зупинити розсилки")
async def stop_all_button_broadcasts_message(message: Message, state: FSMContext):
    if not admin_only(message):
        return
    class FakeCallback:
        def __init__(self, msg):
            self.message = msg
            self.data = "stop_all_broadcasts"
            self.from_user = msg.from_user
            self.id = msg.message_id
        async def answer(self, text=None, show_alert=False): 
            pass
    fake_callback = FakeCallback(message)
    # Викликаємо існуючу функцію з правильними параметрами
    await stop_all_broadcasts_callback(fake_callback, state)

@router.message(lambda m: m.text == "🛑 Зупинити розсилку аккаунта")
async def stop_account_broadcast_button_message(message: Message, state: FSMContext):
    if not admin_only(message):
        return
    class FakeCallback:
        def __init__(self, msg):
            self.message = msg
            self.data = "stop_account_broadcast"
            self.from_user = msg.from_user
            self.id = msg.message_id
        async def answer(self, text=None, show_alert=False): 
            pass
    fake_callback = FakeCallback(message)
    # Викликаємо існуючу функцію з правильними параметрами
    await stop_account_broadcast_callback(fake_callback, state)

@router.message(lambda m: m.text == "📋 Шаблони повідомлень")
async def templates_menu_button_message(message: Message, state: FSMContext):
    if not admin_only(message):
        return
    class FakeCallback:
        def __init__(self, msg):
            self.message = msg
            self.data = "templates_menu"
            self.from_user = msg.from_user
            self.id = msg.message_id
        async def answer(self, text=None, show_alert=False): 
            pass
    fake_callback = FakeCallback(message)
    # Викликаємо існуючу функцію
    await templates_menu_callback(fake_callback)

@router.callback_query(lambda c: c.data == "monitor_broadcasts")
async def monitor_broadcasts_callback(callback: CallbackQuery):
    """Моніторинг та статистика розсилок"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    try:
        # Отримуємо статистику з бази даних
        statistics = db.get_broadcast_statistics()
        history = db.get_broadcast_history(limit=100)
        
        # Формуємо короткий звіт
        total = statistics['total']
        success_rate = (total['successful_sends'] / max(total['total_sends'], 1)) * 100
        
        report_text = f"""
📊 <b>Статистика розсилок</b>

📈 <b>Загальна статистика:</b>
• Всього відправок: {total['total_sends']}
• Успішних: {total['successful_sends']} ✅
• Невдалих: {total['failed_sends']} ❌
• Успішність: {success_rate:.1f}%

📱 <b>Охоплення:</b>
• Унікальних чатів: {total['unique_chats']}
• Активних аккаунтів: {total['unique_accounts']}"""
        
        # Додаємо інформацію про проблемні аккаунти з FloodWait
        floodwait_accounts = statistics.get('floodwait_accounts', [])
        if floodwait_accounts:
            report_text += f"\n\n⚠️ <b>Проблемні аккаунти (FloodWait): {len(floodwait_accounts)}</b>\n"
            for account_data in floodwait_accounts[:5]:  # Показуємо топ-5 проблемних
                phone, floodwait_count, last_floodwait = account_data
                report_text += f"• {phone} ({floodwait_count} раз)\n"
            
            if len(floodwait_accounts) > 5:
                report_text += f"• ... і ще {len(floodwait_accounts) - 5} аккаунтів\n"
        
        report_text += "\n\n💬 <b>Топ аккаунтів:</b>\n"
        
        # Додаємо топ-3 аккаунти
        for i, account_data in enumerate(statistics['by_accounts'][:3], 1):
            phone, first_name, last_name, total_sends, successful, failed = account_data
            account_name = f"{first_name or ''} {last_name or ''}".strip() or phone
            acc_success_rate = (successful / max(total_sends, 1)) * 100
            report_text += f"{i}. {account_name}: {total_sends} відправок ({acc_success_rate:.1f}% успішних)\n"
        
        # Створюємо клавіатуру з кнопками
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Завантажити Excel", callback_data="export_to_excel")],
            [InlineKeyboardButton(text="⏩ Оновити", callback_data="monitor_broadcasts")],
            [InlineKeyboardButton(text="🗑 Очистити історію", callback_data="clear_broadcast_history")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ])
        
        try:
            # Спробуємо відредагувати повідомлення
            await callback.message.edit_text(
                report_text,
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except Exception as edit_error:
            # Якщо не можемо редагувати, відправляємо нове повідомлення
            logger.warning(f"⚠️ Не можна редагувати повідомлення: {edit_error}")
            await callback.message.answer(
                report_text,
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        
        await callback.answer()
        
    except Exception as e:
        logger.error(f"❌ Помилка при отриманні статистики: {e}")
        try:
            await callback.answer("❌ Помилка при отриманні статистики", show_alert=True)
        except:
            # Якщо не можемо відправити alert, відправляємо звичайне повідомлення
            await callback.message.answer("❌ Помилка при отриманні статистики")



@router.callback_query(lambda c: c.data == "export_to_excel")
async def export_to_excel_callback(callback: CallbackQuery):
    """Експорт статистики розсилок до Excel"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    
    try:
        # Показуємо що почали створювати файл
        await callback.answer("📊 Створюємо Excel файл...", show_alert=True)
        
        # Отримуємо статистику з бази даних
        statistics = db.get_broadcast_statistics()
        history = db.get_broadcast_history(limit=1000)  # Більше записів для Excel
        
        # Створюємо Excel файл
        from excelgenerator import ExcelGenerator
        excel_gen = ExcelGenerator()
        excel_file = excel_gen.create_broadcast_statistics_excel(statistics, history)
        
        if excel_file and os.path.exists(excel_file):
            # Відправляємо повідомлення з файлом
            from aiogram.types import FSInputFile
            
            input_file = FSInputFile(excel_file, filename=os.path.basename(excel_file))
            await callback.message.answer_document(
                document=input_file,
                caption="📊 <b>Статистика розсилок</b>\n\nФайл створено успішно!",
                parse_mode='HTML'
            )
            
            # Видаляємо тимчасовий файл
            try:
                os.remove(excel_file)
            except:
                pass
        else:
            await callback.message.answer("❌ Помилка при створенні Excel файлу")
            
    except Exception as e:
        logger.error(f"❌ Помилка при експорті до Excel: {e}")
        await callback.answer("❌ Помилка при створенні файлу", show_alert=True)
        
        await callback.answer()

@router.callback_query(lambda c: c.data == "clear_broadcast_history")
async def clear_broadcast_history_callback(callback: CallbackQuery):
    """Очищення історії розсилок"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    try:
        # Очищаємо історію
        success = db.clear_broadcast_history()
        
        if success:
            await callback.message.answer(
                "✅ <b>Історія розсилок очищена!</b>\n\n"
                "Всі записи статистики були видалені з бази даних.",
                parse_mode='HTML'
            )
        else:
            await callback.message.answer(
                "❌ <b>Помилка при очищенні історії</b>\n\n"
                "Спробуйте пізніше або зверніться до адміністратора.",
                parse_mode='HTML'
            )
        
        await callback.answer()
        
    except Exception as e:
        logger.error(f"❌ Помилка при очищенні історії: {e}")
        await callback.message.answer(
            "❌ <b>Помилка при очищенні історії</b>\n\n"
            "Спробуйте пізніше або зверніться до адміністратора.",
            parse_mode='HTML'
        )
        await callback.answer()

@router.callback_query(lambda c: c.data == "stop_all_broadcasts")
async def stop_all_broadcasts_callback(callback: CallbackQuery, state: FSMContext):
    """Обробник кнопки зупинки всіх розсилок"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    from mass_broadcast import handle_stop_message_command
    
    # Створюємо фейкове повідомлення з командою
    class FakeMessage:
        def __init__(self, callback_message):
            self.text = "/stop_message"
            self.answer = callback_message.answer
    
    fake_message = FakeMessage(callback.message)
    await handle_stop_message_command(fake_message, state)
    await callback.answer()

@router.callback_query(lambda c: c.data == "stop_account_broadcast")
async def stop_account_broadcast_callback(callback: CallbackQuery, state: FSMContext):
    """Обробник кнопки зупинки розсилки конкретного аккаунта"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    await callback.message.answer(
        "🛑 <b>Зупинка розсилки аккаунта</b>\n\n"
        "📱 Введіть номер телефону аккаунта, розсилку якого потрібно зупинити:\n\n"
        "📝 <b>Приклад:</b> <code>+380123456789</code>\n\n"
        "ℹ️ Використовуйте той самий формат, що й при реєстрації аккаунта",
        parse_mode='HTML'
    )
    
    # Встановлюємо стан очікування номера телефону
    await state.set_state("waiting_for_phone_to_stop")
    await callback.answer()

@router.message(lambda message: message.text and message.text.startswith('+') and len(message.text) > 10)
async def process_phone_to_stop(message: Message, state: FSMContext):
    """Обробка введення номера телефону для зупинки розсилки"""
    if not admin_only(message):
        return
    current_state = await state.get_state()
    
    if current_state == "waiting_for_phone_to_stop":
        phone_number = message.text.strip()
        
        # Перевіряємо чи аккаунт існує
        accounts = db.get_accounts()
        account_exists = any(acc['phone_number'] == phone_number for acc in accounts)
        
        if not account_exists:
            await message.answer(
                f"❌ <b>Аккаунт не знайдено!</b>\n\n"
                f"📱 Номер: {phone_number}\n"
                f"ℹ️ Перевірте правильність номера телефону\n\n"
                f"🔄 Спробуйте ще раз або натисніть /start для повернення в головне меню",
                parse_mode='HTML'
            )
            return
        
        # Зупиняємо розсилку аккаунта (адаптована логіка з handle_stop_message_command)
        try:
            logger.info(f"🔧 DEBUG: Зупиняємо конкретний аккаунт {phone_number} (НЕ всі розсилки)")
            
            # Встановлюємо флаг зупинки через глобальну змінну
            from mass_broadcast import stop_account_broadcast
            stop_account_broadcast(phone_number)
            
            # Встановлюємо флаг зупинки для цього аккаунта (додатково через FSM)
            await state.update_data(stop_broadcast_account=phone_number)
            
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE broadcast_status 
                    SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                    WHERE account_phone = ? AND status IN ('pending', 'running')
                """, (phone_number,))
                updated_count = cursor.rowcount
                conn.commit()
            
            # Відключаємо клієнт Telegram для цього аккаунта
            from mass_broadcast import disconnect_account_client, resume_account_broadcast
            disconnect_success = await disconnect_account_client(phone_number)
            
            # Очищаємо аккаунт зі списку зупинених після відключення
            resume_account_broadcast(phone_number)
            
            # НЕ викликаємо cleanup_hanging_tasks() - це може пошкодити інші активні аккаунти
                
            if updated_count > 0:
                disconnect_info = "🔌 Клієнт відключений" if disconnect_success else "⚠️ Клієнт не відключений"
                await message.answer(
                    f"🛑 <b>Розсилку аккаунта зупинено!</b>\n\n"
                    f"📱 <b>Аккаунт:</b> {phone_number}\n"
                    f"✅ Зупинено {updated_count} активних розсилок\n"
                    f"{disconnect_info}\n\n"
                    f"📊 Аккаунт тепер доступний для нових розсилок",
                    parse_mode='HTML'
                )
            else:
                disconnect_info = "🔌 Клієнт відключений" if disconnect_success else "⚠️ Клієнт не відключений"
                await message.answer(
                    f"ℹ️ <b>Активних розсилок не знайдено</b>\n\n"
                    f"📱 <b>Аккаунт:</b> {phone_number}\n"
                    f"{disconnect_info}\n"
                    f"✅ Аккаунт вже доступний для нових розсилок",
                    parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"❌ Помилка при зупинці розсилки аккаунта {phone_number}: {e}")
            await message.answer(
                f"❌ <b>Помилка при зупинці розсилки!</b>\n\n"
                f"📱 <b>Аккаунт:</b> {phone_number}\n"
                f"⚠️ Спробуйте ще раз або зверніться до адміністратора",
                parse_mode='HTML'
            )
        
        # Очищаємо стан
        await state.clear()

@router.message(Command("add_template"))
async def cmd_add_template(message: Message, state: FSMContext):
    """Команда для додавання шаблону"""
    if not admin_only(message):
        return
    await message.answer(
        "📋 <b>Створення нового шаблону</b>\n\n"
        "Відправте повідомлення (текст, фото, відео, аудіо тощо), яке ви хочете зберегти як шаблон.\n\n"
        "Після відправки введіть назву для шаблону.",
        parse_mode='HTML'
    )
    await state.set_state(MassBroadcastStates.waiting_for_template_message)

@router.callback_query(lambda c: c.data == "accounts")
async def accounts_callback(callback):
    """Обробка натискання кнопки списку аккаунтів"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    await show_accounts_list(callback, db)
    await callback.answer()

@router.callback_query(lambda c: c.data == "register_number")
async def register_number_callback(callback, state):
    """Обробка натискання кнопки реєстрації номера"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    from utils import start_registration_process
    await start_registration_process(callback, state)
    await callback.answer()

#====================== ОБРОБКА ШАБЛОНІВ ======================

@router.callback_query(lambda c: c.data == "templates_menu")
async def templates_menu_callback(callback):
    """Меню шаблонів"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    templates = template_manager.db.get_templates()
    if templates:
        keyboard = template_manager.get_template_keyboard(templates)
        await callback.message.answer(
            "📋 <b>Меню шаблонів повідомлень</b>\n\n"
            "Оберіть шаблон або створіть новий:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Додати шаблон", callback_data="add_template")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ])
        await callback.message.answer(
            "📋 <b>Меню шаблонів повідомлень</b>\n\n"
            "❌ Шаблони не знайдені\n\n"
            "Створіть перший шаблон, відправивши повідомлення боту:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    await callback.answer()

@router.callback_query(lambda c: c.data == "add_template")
async def add_template_callback(callback, state: FSMContext):
    """Додавання нового шаблону"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    await callback.message.answer(
        "📋 <b>Створення нового шаблону</b>\n\n"
        "Відправте повідомлення (текст, фото, відео, аудіо тощо), яке ви хочете зберегти як шаблон.\n\n"
        "Після відправки введіть назву для шаблону.",
        parse_mode='HTML'
    )
    await state.set_state(MassBroadcastStates.waiting_for_template_message)
    await callback.answer()

@router.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main_callback(callback):
    """Повернення до головного меню"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    await cmd_start(callback.message)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_template_message)
async def process_template_message(message: Message, state: FSMContext):
    """Обробка повідомлення для шаблону"""
    if not admin_only(message):
        return
    # Зберігаємо повідомлення як шаблон
    await state.update_data(template_message=message)
    await message.answer("📝 Введіть назву для шаблону:")
    await state.set_state(MassBroadcastStates.waiting_for_template_name)

@router.message(MassBroadcastStates.waiting_for_template_name)
async def process_template_name(message: Message, state: FSMContext):
    """Обробка назви шаблону"""
    if not admin_only(message):
        return
    data = await state.get_data()
    template_message = data.get('template_message')
    template_name = message.text.strip()
    
    if not template_name:
        await message.answer("❌ Назва шаблону не може бути порожньою. Спробуйте ще раз:")
        return
    
    # Зберігаємо шаблон
    success = await template_manager.save_template_from_message(template_message, template_name)
    
    if success:
        await message.answer(f"✅ Шаблон '{template_name}' успішно збережено!")
    else:
        await message.answer("❌ Помилка при збереженні шаблону. Спробуйте ще раз.")
    
    await state.clear()

# Додаткові обробники для шаблонів

@router.callback_query(lambda c: c.data == "mass_different_messages")
async def mass_different_messages_callback(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору різних повідомлень для аккаунтів"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    from mass_broadcast import process_mass_different_messages_callback
    await process_mass_different_messages_callback(callback, state)
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("edit_template_"))
async def edit_template_callback(callback: CallbackQuery):
    """Редагування шаблону"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    template_id = int(callback.data.replace("edit_template_", ""))
    template_info = template_manager.db.get_template(template_id)
    
    if template_info:
        keyboard = template_manager.get_edit_template_keyboard(template_id)
        await callback.message.answer(
            f"✏️ <b>Редагування шаблону:</b> {template_info['name']}\n\n"
            "Оберіть дію:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    else:
        await callback.message.answer("❌ Шаблон не знайдено")
    
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("delete_template_"))
async def delete_template_callback(callback: CallbackQuery):
    """Видалення шаблону"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    template_id = int(callback.data.replace("delete_template_", ""))
    template_info = template_manager.db.get_template(template_id)
    
    if template_info:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Так, видалити", callback_data=f"confirm_delete_template_{template_id}")],
            [InlineKeyboardButton(text="❌ Скасувати", callback_data="templates_menu")]
        ])
        await callback.message.answer(
            f"🗑️ <b>Видалення шаблону</b>\n\n"
            f"Ви впевнені, що хочете видалити шаблон '{template_info['name']}'?\n\n"
            "⚠️ Цю дію неможливо скасувати!",
            parse_mode='HTML',
            reply_markup=keyboard
        )
    else:
        await callback.message.answer("❌ Шаблон не знайдено")
    
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("confirm_delete_template_"))
async def confirm_delete_template_callback(callback: CallbackQuery):
    """Підтвердження видалення шаблону"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    template_id = int(callback.data.replace("confirm_delete_template_", ""))
    
    success = template_manager.db.delete_template(template_id)
    if success:
        await callback.message.answer("✅ Шаблон успішно видалено!")
    else:
        await callback.message.answer("❌ Помилка при видаленні шаблону")
    
    # Повертаємося до меню шаблонів
    await templates_menu_callback(callback)

@router.callback_query(lambda c: c.data == "close_templates")
async def close_templates_callback(callback: CallbackQuery):
    """Закриття меню шаблонів"""
    if not admin_only_callback(callback):
        await callback.answer("🚫 Доступ заборонено!", show_alert=True)
        return
    await callback.message.edit_text(
        "🎯 <b>Головне меню</b>\n\n"
        "📱 <b>Доступні команди:</b>\n"
        "• <code>/start</code> - головне меню\n"
        "• <code>/stop_message</code> - зупинити всі розсилки\n\n"
        "🔧 <b>Доступні функції:</b>\n"
        "• Реєстрація аккаунтів\n"
        "• Масові розсилки\n"
        "• Управління шаблонами\n"
        "• Статистика розсилок",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 Аккаунти", callback_data="accounts")],
            [InlineKeyboardButton(text="📢 Масові розсилки", callback_data="Mass_broadcast")],
            [InlineKeyboardButton(text="📝 Шаблони", callback_data="templates_menu")],
            [InlineKeyboardButton(text="🛑 Зупинити всі розсилки", callback_data="stop_all_broadcasts")],
            [InlineKeyboardButton(text="🛑 Зупинити розсилку аккаунта", callback_data="stop_account_broadcast")]
        ])
    )
    await callback.answer()

@router.message()
async def unauthorized_access_handler(message: Message, state: FSMContext):
    """Обробник для неавторизованих користувачів (останній в порядку)"""
    # Перевіряємо чи користувач в FSM стані - тоді НЕ блокуємо
    current_state = await state.get_state()
    if current_state:
        # Користувач в FSM стані - пропускаємо цей обробник
        return
    
    if not admin_only(message):
        await message.answer(
            "🚫 <b>Доступ заборонено!</b>\n\n"
            "❌ Ви не маєте прав для використання цього бота.\n\n",
            parse_mode='HTML'
        )
        return  # Блокуємо обробку для неавторизованих
    # Для адміністраторів нічого не робимо - дозволяємо іншим обробникам спрацювати

async def main():
    """Головна функція"""
    try:
        # Придушуємо помилки Telethon при завершенні
        from mass_broadcast import suppress_telethon_errors
        suppress_telethon_errors()
        
        # Підключаємо роутери (ВАЖЛИВО: порядок має значення!)
        # Спеціалізовані роутери йдуть ПЕРШИМИ (з конкретними станами FSM)
        dp.include_router(mass_broadcast_router)  # ПЕРШИЙ - щоб FSM стани спрацьовували
        dp.include_router(registration_router)
        dp.include_router(groups_router)
        dp.include_router(join_groups_router)
        dp.include_router(router)  # ОСТАННІЙ - загальні команди та admin обробник
        
        logger.info("🔄 Запуск бота...")
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        logger.info("🛑 Отримано сигнал завершення...")
        
        # Імпортуємо функції очищення
        try:
            from mass_broadcast import disconnect_all_active_clients, cleanup_hanging_tasks
            logger.info("🧹 Очищення ресурсів...")
            
            # Відключаємо всі клієнти
            await disconnect_all_active_clients()
            
            # Очищаємо завислі задачі
            await cleanup_hanging_tasks()
            
            logger.info("✅ Очищення завершено")
        except Exception as cleanup_error:
            logger.warning(f"⚠️ Помилка при очищенні: {cleanup_error}")
        
        logger.info("👋 Бот зупинений")
    except Exception as e:
        logger.error(f"❌ Помилка при запуску бота: {e}")
        
        # Навіть при помилці намагаємося очистити ресурси
        try:
            from mass_broadcast import disconnect_all_active_clients, cleanup_hanging_tasks
            await disconnect_all_active_clients()
            await cleanup_hanging_tasks()
        except:
            pass
        
        raise

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Програма зупинена користувачем")
    except Exception as e:
        print(f"❌ Критична помилка: {e}")
        logger.error(f"❌ Критична помилка: {e}")
    finally:
        print("👋 Завершення роботи програми")
