import os
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat
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

client = TelegramClient("session", API_ID, API_HASH)

async def main():
    dialogs = await client.get_dialogs()
    group_data = []

    for dialog in dialogs:
        entity = dialog.entity
        if isinstance(entity, (Channel, Chat)):  
            # Отримуємо ID групи
            group_id = str(entity.id)
            
            # Отримуємо username якщо є
            username = getattr(entity, 'username', None)
            
            # Отримуємо назву групи
            title = getattr(entity, 'title', f'Група {group_id}')
            
            # Формуємо дані групи
            group_info = {
                'id': group_id,
                'title': title,
                'username': username,
                'link': None
            }
            
            # Створюємо посилання
            if username:
                group_info['link'] = f"https://t.me/{username}"
            else:
                # Для груп без username використовуємо ID
                group_info['link'] = f"ID: {group_id}"
            
            group_data.append(group_info)

    # Виводимо результати
    print("📋 Список груп:")
    print("=" * 80)
    
    for i, group in enumerate(group_data, 1):
        print(f"{i}. {group['title']}")
        print(f"   ID: {group['id']}")
        if group['username']:
            print(f"   Username: @{group['username']}")
            print(f"   Посилання: {group['link']}")
        else:
            print(f"   Посилання: {group['link']}")
        print()
    
    # Виводимо тільки ID через кому
    group_ids = [group['id'] for group in group_data]
    ids_result = ",".join(group_ids)
    print("🔢 ID груп через кому:")
    print(ids_result)
    print()
    
    # Виводимо username через кому
    usernames = []
    for group in group_data:
        if group['username']:
            usernames.append(f"@{group['username']}")
        else:
            usernames.append(f"ID:{group['id']}")
    
    usernames_result = ",".join(usernames)
    print("👤 Username через кому:")
    print(usernames_result)
    print()
    
    # Виводимо посилання через кому
    links = []
    for group in group_data:
        if group['username']:
            links.append(group['link'])
        else:
            links.append(f"ID:{group['id']}")
    
    links_result = ",".join(links)
    print("🔗 Посилання через кому:")
    print(links_result)
    print()
    
    # Виводимо змішаний формат (ID, username та посилання)
    mixed = []
    for group in group_data:
        if group['username']:
            mixed.append(f"@{group['username']}")
        else:
            mixed.append(group['id'])
    
    mixed_result = ",".join(mixed)
    print("🎯 Змішаний формат (ID та username):")
    print(mixed_result)
    print()
    
    # Виводимо всі формати разом
    all_formats = []
    for group in group_data:
        if group['username']:
            all_formats.append(f"@{group['username']}")
        else:
            all_formats.append(group['id'])
    
    all_formats_result = ",".join(all_formats)
    print("🌟 Всі формати разом (ID та @username):")
    print(all_formats_result)
    
    return {
        'ids': ids_result,
        'links': links_result,
        'mixed': mixed_result,
        'groups': group_data
    }

with client:
    client.loop.run_until_complete(main())
