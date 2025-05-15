#!/usr/bin/env python
"""
Скрипт для запуска телеграм-бота HLTV для пользователей
"""

import logging
import sys
import os
from src.bots.config import load_config
from src.bots.user_bot.telegram_bot import HLTVStatsBot

# Загружаем конфигурацию
config = load_config('user')

# Создаем директорию для логов, если она не существует
os.makedirs(os.path.dirname(config['log_file']), exist_ok=True)

# Настройка логирования с явным указанием кодировки UTF-8
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Для вывода в консоль используем специальный обработчик с кодировкой
        logging.StreamHandler(sys.stdout),
        # Для файла указываем явно кодировку UTF-8
        logging.FileHandler(config['log_file'], encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def main():
    """
    Основная функция для запуска бота
    """
    # Проверяем конфигурацию
    if config['token'] == "YOUR_BOT_TOKEN_HERE":
        logger.error("Не указан токен бота в конфигурационном файле src/bots/config/user_bot_config.json")
        logger.info("Пожалуйста, укажите токен бота, полученный от @BotFather в Telegram")
        sys.exit(1)
    
    # Запускаем бота
    try:
        logger.info(f"Запуск бота с базой данных: {config['hltv_db_path']}")
        bot = HLTVStatsBot(
            token=config['token'],
            db_path=config['hltv_db_path'],
            subscribers_db_path=config['subscribers_db_path']
        )
        bot.run()
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 