#!/usr/bin/env python
"""
Запуск бота-разработчика для пользователей
"""
import sys
import os

# Создаем директорию для логов
if sys.platform == "win32":
    os.makedirs("logs", exist_ok=True)

from src.bots.start_user_dev_bot import main

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nБот был остановлен пользователем.")
    except Exception as e:
        print(f"\nПроизошла ошибка при запуске бота: {e}")
        sys.exit(1) 