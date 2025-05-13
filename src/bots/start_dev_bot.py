#!/usr/bin/env python
"""
Скрипт для запуска телеграм-бота HLTV для разработчиков
"""

import logging
import sys
from src.bots.config import load_config
from src.bots.dev_bot.telegram_bot import DevBot

# Загружаем конфигурацию
config = load_config('dev')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(config['log_file'])
    ]
)
logger = logging.getLogger(__name__)

def main():
    """
    Основная функция для запуска бота для разработчиков
    """
    # Проверяем конфигурацию
    if config['token'] == "YOUR_DEV_BOT_TOKEN_HERE":
        logger.error("Не указан токен бота в конфигурационном файле src/bots/config/dev_bot_config.json")
        logger.info("Пожалуйста, укажите токен бота, полученный от @BotFather в Telegram")
        sys.exit(1)
    
    # Запускаем бота
    logger.info(f"Запуск бота для разработчиков с базой данных: {config['hltv_db_path']}")
    bot = DevBot(
        token=config['token'],
        db_path=config['hltv_db_path']
    )
    bot.run()

if __name__ == "__main__":
    main() 