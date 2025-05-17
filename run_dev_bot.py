#!/usr/bin/env python
"""
Run bot for developers (dev_bot)
"""
import sys
import os
import logging

# Отключаем подробные логи httpx/urllib3
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.vendor.ptb_urllib3.urllib3.connectionpool").setLevel(logging.WARNING)

# Создаем директорию для логов
if sys.platform == "win32":
    os.makedirs("logs", exist_ok=True)

from src.bots.start_dev_bot import main

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDev bot was stopped by user.")
    except Exception as e:
        print(f"\nAn error occurred when starting the dev bot: {e}")
        sys.exit(1) 