#!/usr/bin/env python
"""
Запуск бота для пользователей
"""
import sys
import os

# Настраиваем кодировку для корректной работы с русским языком в консоли Windows
if sys.platform == "win32":
    # Устанавливаем UTF-8 для вывода в консоль
    # Исправляем проблему с UnicodeEncodeError на Windows
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer)
    
    # Создаем директорию для логов
    os.makedirs("logs", exist_ok=True)

from src.bots.start_user_bot import main

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nБот был остановлен пользователем.")
    except Exception as e:
        print(f"\nПроизошла ошибка при запуске бота: {e}")
        sys.exit(1) 