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
    group_ids = []

    for dialog in dialogs:
        entity = dialog.entity
        if isinstance(entity, (Channel, Chat)):  
            group_ids.append(str(entity.id))  # перетворюємо на str для join

    result = ",".join(group_ids)  # всі id через кому
    print(result)
    return result

with client:
    client.loop.run_until_complete(main())
