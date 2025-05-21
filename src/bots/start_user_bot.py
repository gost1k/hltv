#!/usr/bin/env python
"""
Скрипт для запуска телеграм-бота HLTV для пользователей
"""

import logging
import sys
import io
import os
import json

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
config = load_config('user')

# Настройка логирования
logger = setup_logging(config)

USER_ACTIONS_PATH = os.path.join('storage', 'json', 'bot', 'user_actions.json')

def update_user_summary(user_id):
    # Загружаем или создаём структуру user_actions.json
    if os.path.exists(USER_ACTIONS_PATH):
        with open(USER_ACTIONS_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {}
    if 'summary' not in data:
        data['summary'] = {'user_ids': [], 'total_users': 0}
    if str(user_id) not in data['summary']['user_ids']:
        data['summary']['user_ids'].append(str(user_id))
        data['summary']['total_users'] = len(data['summary']['user_ids'])
        with open(USER_ACTIONS_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

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
        bot = HLTVUserBot(
            token=config['token'],
            db_path=config['hltv_db_path'],
            log_file=config['log_file'],
            config_name='user'
        )
        # --- Логирование user_id или username в user_actions.json ---
        user_key = None
        user_id = getattr(bot, 'user_id', None)
        if user_id is not None:
            user_key = str(user_id)
        else:
            username = getattr(bot, 'username', None)
            if username:
                user_key = username
            elif 'username' in config:
                user_key = config['username']
        if user_key:
            update_user_summary(user_key)
        bot.run()
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 