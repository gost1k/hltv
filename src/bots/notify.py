import os
from telegram import Bot
from src.bots.config import load_config
import asyncio

def get_bot():
    config = load_config('user')  # или 'user_dev', если нужно
    return Bot(token=config['token'])

def send_telegram_message(user_id, text):
    bot = get_bot()
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(bot.send_message(chat_id=user_id, text=text))
        else:
            loop.run_until_complete(bot.send_message(chat_id=user_id, text=text))
    except Exception as e:
        print(f"Ошибка отправки сообщения {user_id}: {e}") 