import os
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat
from dotenv import load_dotenv

# Завантажуємо змінні з .env файлу
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Перевірка конфігурації
print(f"🔍 API_ID: {API_ID}")
print(f"🔍 API_HASH: {'*' * len(API_HASH) if API_HASH else 'НЕ ВСТАНОВЛЕНО'}")

if not API_ID or not API_HASH:
    print("❌ Помилка: API_ID або API_HASH не встановлено!")
    exit(1)

# Запитуємо номер телефону
phone_number = input("📱 Введіть номер телефону (у форматі +380...): ").strip()

# Ім'я файлу сесії = номеру телефону
session_name = f"session_{phone_number}"

# Створюємо клієнт
client = TelegramClient(session_name, API_ID, API_HASH)

async def main():
    # Авторизація (використовує існуючу сесію, або попросить код тільки перший раз)
    await client.start(phone_number)

    dialogs = await client.get_dialogs()
    group_data = []

    for dialog in dialogs:
        entity = dialog.entity
        if isinstance(entity, (Channel, Chat)):
            group_id = str(entity.id)
            username = getattr(entity, 'username', None)
            title = getattr(entity, 'title', f'Група {group_id}')

            group_info = {
                'id': group_id,
                'title': title,
                'username': username,
                'link': None
            }

            if username:
                group_info['link'] = f"https://t.me/{username}"
            else:
                group_info['link'] = f"ID: {group_id}"

            group_data.append(group_info)

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

    # ID через кому
    ids_result = ",".join([group['id'] for group in group_data])
    print("🔢 ID груп через кому:")
    print(ids_result)
    print()

    # Usernames через кому
    usernames = [f"@{g['username']}" if g['username'] else f"ID:{g['id']}" for g in group_data]
    usernames_result = ",".join(usernames)
    print("👤 Username через кому:")
    print(usernames_result)
    print()

    # Посилання через кому
    links = [g['link'] for g in group_data]
    links_result = ",".join(links)
    print("🔗 Посилання через кому:")
    print(links_result)
    print()

    # Змішаний формат
    mixed = [f"@{g['username']}" if g['username'] else g['id'] for g in group_data]
    mixed_result = ",".join(mixed)
    print("🎯 Змішаний формат (ID та username):")
    print(mixed_result)
    print()

    return {
        'ids': ids_result,
        'links': links_result,
        'mixed': mixed_result,
        'groups': group_data
    }

with client:
    client.loop.run_until_complete(main())
