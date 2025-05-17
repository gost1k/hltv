#!/usr/bin/env python
"""
Скрипт для запуска телеграм-бота HLTV для разработчиков
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
from src.bots.common.hltv_user_bot import HLTVUserBot

# Загружаем конфигурацию
config = load_config('user_dev')

# Настройка логирования
logger = setup_logging(config)

def main():
    """
    Main function to start the bot
    """
    try:
        # Check config
        if config['token'] == "YOUR_BOT_TOKEN_HERE":
            logger.error("No bot token specified in config file src/bots/config/user_dev_bot_config.json")
            logger.info("Please specify the bot token obtained from @BotFather in Telegram")
            sys.exit(1)
        
        # Start the bot
        logger.info(f"Starting bot with database: {config['hltv_db_path']}")
        
        # Create and run the bot instance
        bot = HLTVUserBot(
            token=config['token'],
            db_path=config['hltv_db_path'],
            log_file=config['log_file'],
            config_name='user_dev'
        )
        bot.run()
    except Exception as e:
        logger.error(f"Error starting bot: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 