#!/usr/bin/env python
"""
Скрипт для загрузки прошедших матчей из JSON-файлов в базу данных.
Загружает все связанные данные, включая детали матчей и статистику игроков.
"""
import os
import sys
import logging
import argparse
import json
from src.utils.telegram_log_handler import TelegramLogHandler

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.loader.matches_loader import MatchesLoader
from src.loader.match_details_loader import MatchDetailsLoader
from src.config.constants import DATABASE_FILE

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/load_past_matches.log")
    ]
)
logger = logging.getLogger(__name__)

with open("src/bots/config/dev_bot_config.json", encoding="utf-8") as f:
    dev_bot_config = json.load(f)
dev_bot_token = dev_bot_config["token"]
telegram_handler = TelegramLogHandler(dev_bot_token, chat_id="7146832422")
telegram_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
telegram_handler.setFormatter(formatter)
logger.addHandler(telegram_handler)

def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Загрузка прошедших матчей из JSON в базу данных')
    parser.add_argument('--db-path', type=str, default=DATABASE_FILE, help='Путь к файлу базы данных')
    parser.add_argument('--force', action='store_true', help='Принудительная загрузка, даже если файлы уже обработаны')
    parser.add_argument('--skip-match-details', action='store_true', help='Пропустить загрузку деталей матчей')
    parser.add_argument('--skip-player-stats', action='store_true', help='Пропустить загрузку статистики игроков')
    return parser.parse_args()

def main():
    """
    Основная функция скрипта
    """
    args = parse_arguments()
    
    try:
        logger.info("Загрузка ПРОШЕДШИХ матчей", extra={"telegram_firstline": True})
        logger.info("Начало загрузки деталей матчей и статистики игроков из JSON в базу данных")
        # Создаем директорию для логов, если ее нет
        os.makedirs("logs", exist_ok=True)
        
        # Загружаем детали матчей и статистику игроков, если не пропущено
        if not args.skip_match_details or not args.skip_player_stats:
            details_loader = MatchDetailsLoader(db_path=args.db_path)
            
            # Используем специальный метод для загрузки, передавая флаги
            details_stats = details_loader.load_match_details_and_stats(
                skip_match_details=args.skip_match_details,
                skip_player_stats=args.skip_player_stats
            )
            
            # Выводим статистику только для деталей и игроков
            if not args.skip_match_details:
                logger.info("======== Загрузка деталей матчей ========")
                logger.info(f"Обработано файлов: {details_stats.get('match_details_processed', 0)}")
                logger.info(f"Успешно загружено: {details_stats.get('match_details_success', 0)}")
                logger.info(f"Ошибок: {details_stats.get('match_details_error', 0)}")
            
            if not args.skip_player_stats:
                logger.info("======== Загрузка статистики игроков ========")
                logger.info(f"Обработано файлов: {details_stats.get('player_stats_processed', 0)}")
                logger.info(f"Успешно загружено: {details_stats.get('player_stats_success', 0)}")
                logger.info(f"Ошибок: {details_stats.get('player_stats_error', 0)}")

                # Новый блок: статистика по картам
                logger.info("======== Загрузка информации о картах ========")
                logger.info(f"Обработано файлов: {details_stats.get('maps_processed', 0)}")
                logger.info(f"Успешно загружено: {details_stats.get('maps_success', 0)}")
                logger.info(f"Ошибок: {details_stats.get('maps_error', 0)}")
        
        logger.info("Загрузка прошедших матчей завершена")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {str(e)}")
        sys.exit(1)
    finally:
        for handler in logger.handlers:
            if hasattr(handler, 'send_buffer'):
                handler.send_buffer()

if __name__ == "__main__":
    main() 