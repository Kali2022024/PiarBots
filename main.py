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

# Ініціалізація модулів
init_registration_module(API_ID, API_HASH, db)
init_groups_module(db)
init_mass_broadcast_module(db, bot)
init_join_groups_module(db)


# Імпортуємо RANDOM_STICKERS в database модуль
import database
RANDOM_STICKERS = database.RANDOM_STICKERS

@router.message(Command("start_bot"))
async def cmd_start(message: Message):
    """Обробник команди /start"""
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
        [InlineKeyboardButton(text="📋 Список аккаунтів", callback_data="accounts")],
        [InlineKeyboardButton(text="📱 Зареєструвати номер", callback_data="register_number")],
        [InlineKeyboardButton(text="🗑️ Видалити аккаунт", callback_data="delete_account")],
        [InlineKeyboardButton(text="👥 Додати список груп", callback_data="Groups_for_account")],
        [InlineKeyboardButton(text="➕ Додатись в групи", callback_data="join_groups")],
        [InlineKeyboardButton(text="🗑️ Видалити пакет груп", callback_data="delete_group_package")],
        [InlineKeyboardButton(text="📊 Статус розсилання", callback_data="broadcast_status")],
        [InlineKeyboardButton(text="📤 Функціональна роозсилка", callback_data="Mass_broadcast")],
        [InlineKeyboardButton(text="🛑 Зупинити розсилки", callback_data="stop_all_broadcasts")],
        [InlineKeyboardButton(text="🛑 Зупинити розсилку аккаунта", callback_data="stop_account_broadcast")],
        [InlineKeyboardButton(text="📋 Шаблони повідомлень", callback_data="templates_menu")]
    ])
    
    await message.answer(welcome_text, reply_markup=keyboard)

@router.callback_query(lambda c: c.data == "stop_all_broadcasts")
async def stop_all_broadcasts_callback(callback: CallbackQuery, state: FSMContext):
    """Обробник кнопки зупинки всіх розсилок"""
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
        
        # Зупиняємо розсилку аккаунта
        try:
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
            from mass_broadcast import disconnect_account_client
            disconnect_success = await disconnect_account_client(phone_number)
                
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
    await show_accounts_list(callback, db)
    await callback.answer()

@router.callback_query(lambda c: c.data == "register_number")
async def register_number_callback(callback, state):
    """Обробка натискання кнопки реєстрації номера"""
    from utils import start_registration_process
    await start_registration_process(callback, state)
    await callback.answer()

#====================== ОБРОБКА ШАБЛОНІВ ======================

@router.callback_query(lambda c: c.data == "templates_menu")
async def templates_menu_callback(callback):
    """Меню шаблонів"""
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
    await cmd_start(callback.message)
    await callback.answer()

@router.message(MassBroadcastStates.waiting_for_template_message)
async def process_template_message(message: Message, state: FSMContext):
    """Обробка повідомлення для шаблону"""
    # Зберігаємо повідомлення як шаблон
    await state.update_data(template_message=message)
    await message.answer("📝 Введіть назву для шаблону:")
    await state.set_state(MassBroadcastStates.waiting_for_template_name)

@router.message(MassBroadcastStates.waiting_for_template_name)
async def process_template_name(message: Message, state: FSMContext):
    """Обробка назви шаблону"""
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
    from mass_broadcast import process_mass_different_messages_callback
    await process_mass_different_messages_callback(callback, state)
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("edit_template_"))
async def edit_template_callback(callback: CallbackQuery):
    """Редагування шаблону"""
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

async def main():
    """Головна функція"""
    try:
        # Підключаємо роутери
        dp.include_router(router)
        dp.include_router(registration_router)
        dp.include_router(groups_router)
        dp.include_router(mass_broadcast_router)
        dp.include_router(join_groups_router)
        
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
