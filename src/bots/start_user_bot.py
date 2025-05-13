#!/usr/bin/env python
"""
Скрипт для запуска телеграм-бота HLTV для пользователей
"""

import logging
import sys
from src.bots.config import load_config
from src.bots.user_bot.telegram_bot import HLTVStatsBot

# Загружаем конфигурацию
config = load_config('user')

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
    Основная функция для запуска бота
    """
    # Проверяем конфигурацию
    if config['token'] == "YOUR_BOT_TOKEN_HERE":
        logger.error("Не указан токен бота в конфигурационном файле src/bots/config/user_bot_config.json")
        logger.info("Пожалуйста, укажите токен бота, полученный от @BotFather в Telegram")
        sys.exit(1)
    
    # Запускаем бота
    logger.info(f"Запуск бота с базой данных: {config['hltv_db_path']}")
    bot = HLTVStatsBot(
        token=config['token'],
        db_path=config['hltv_db_path'],
        subscribers_db_path=config['subscribers_db_path']
    )
    bot.run()

if __name__ == "__main__":
    main() 