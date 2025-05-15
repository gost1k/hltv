#!/usr/bin/env python
"""
Модуль конфигурации для телеграм-ботов
"""

import os
import json
import logging

logger = logging.getLogger(__name__)

# Путь к директории с конфигурационными файлами
CONFIG_DIR = os.path.join(os.path.dirname(__file__))

# Шаблон структуры конфигурации бота
DEFAULT_CONFIG = {
    "token": "YOUR_BOT_TOKEN_HERE",
    "hltv_db_path": "hltv.db",
    "log_file": "bot.log",
    "admin_chat_ids": []
}

def ensure_config_dir():
    """
    Проверяет существование директории для конфигурационных файлов
    """
    if not os.path.exists(CONFIG_DIR):
        try:
            os.makedirs(CONFIG_DIR, exist_ok=True)
            logger.info(f"Создана директория для конфигурационных файлов: {CONFIG_DIR}")
        except Exception as e:
            logger.error(f"Ошибка при создании директории конфигурации: {str(e)}")

def create_default_config(bot_type):
    """
    Создает конфигурационный файл с настройками по умолчанию
    
    Args:
        bot_type (str): Тип бота ('user' или 'dev')
        
    Returns:
        str: Путь к созданному файлу
    """
    ensure_config_dir()
    
    config_path = os.path.join(CONFIG_DIR, f"{bot_type}_bot_config.json")
    
    if not os.path.exists(config_path):
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
            logger.info(f"Создан конфигурационный файл по умолчанию: {config_path}")
        except Exception as e:
            logger.error(f"Ошибка при создании конфигурационного файла: {str(e)}")
    
    return config_path

def load_config(bot_type):
    """
    Загружает конфигурацию для указанного типа бота
    
    Args:
        bot_type (str): Тип бота ('user' или 'dev')
        
    Returns:
        dict: Словарь с конфигурацией бота
    """
    config_path = os.path.join(CONFIG_DIR, f"{bot_type}_bot_config.json")
    
    # Создаем конфигурационный файл по умолчанию, если он не существует
    if not os.path.exists(config_path):
        config_path = create_default_config(bot_type)
        logger.warning(f"Конфигурационный файл {bot_type}_bot_config.json не найден. Создан файл с настройками по умолчанию.")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Проверяем, что все обязательные параметры присутствуют
        for key in DEFAULT_CONFIG:
            if key not in config:
                config[key] = DEFAULT_CONFIG[key]
                logger.warning(f"В конфигурации отсутствует параметр {key}. Использовано значение по умолчанию.")
        
        return config
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке конфигурационного файла: {str(e)}")
        logger.warning("Возвращена конфигурация по умолчанию")
        return DEFAULT_CONFIG.copy()

def save_config(bot_type, config):
    """
    Сохраняет конфигурацию в файл
    
    Args:
        bot_type (str): Тип бота ('user' или 'dev')
        config (dict): Словарь с конфигурацией бота
        
    Returns:
        bool: True если успешно, False если ошибка
    """
    ensure_config_dir()
    
    config_path = os.path.join(CONFIG_DIR, f"{bot_type}_bot_config.json")
    
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
        logger.info(f"Конфигурация бота {bot_type} успешно сохранена")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при сохранении конфигурации: {str(e)}")
        return False

# Создаем директорию и файлы конфигурации при импорте модуля
ensure_config_dir() 