#!/usr/bin/env python
"""
Скрипт для запуска телеграм-бота HLTV для пользователей
"""

import logging
import sys
import io
import os

# Установка кодировки UTF-8 для консоли Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Безопасная настройка логирования
def setup_logging(config):
    # Создаем директорию для логов, если она не существует
    log_dir = os.path.dirname(config['log_file'])
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Настраиваем логирование только в файл
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        filename=config['log_file'],
        encoding='utf-8'
    )
    
    return logging.getLogger(__name__)

from src.bots.config import load_config
from src.bots.user_bot.telegram_bot import HLTVStatsBot

# Загружаем конфигурацию
config = load_config('user')

# Настройка логирования
logger = setup_logging(config)

def main():
    """
    Основная функция для запуска бота
    """
    try:
        # Проверяем конфигурацию
        if config['token'] == "YOUR_BOT_TOKEN_HERE":
            logger.error("Не указан токен бота в конфигурационном файле src/bots/config/user_bot_config.json")
            logger.info("Пожалуйста, укажите токен бота, полученный от @BotFather в Telegram")
            sys.exit(1)
        
        # Запускаем бота
        logger.info(f"Запуск бота с базой данных: {config['hltv_db_path']}")
        
        # Создаем экземпляр бота и запускаем его
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