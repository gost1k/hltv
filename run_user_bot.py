#!/usr/bin/env python
"""
Run bot for users
"""
import sys
import os
import logging

# Создаем директорию для логов (если нужно)
if sys.platform == "win32":
    os.makedirs("logs", exist_ok=True)

# Основная настройка логгера: пишем в файл с utf-8
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
        # logging.StreamHandler(sys.stdout)  # Если нужно видеть логи в консоли, раскомментируйте
    ]
)

from src.bots.start_user_bot import main

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBot was stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred when starting the bot: {e}")
        sys.exit(1) 