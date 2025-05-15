#!/usr/bin/env python
"""
Скрипт для загрузки прошедших матчей из JSON-файлов в базу данных.
Загружает все связанные данные, включая детали матчей и статистику игроков.
"""
import os
import sys
import logging
import argparse

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
        logger.info("Начало загрузки прошедших матчей из JSON в базу данных")
        
        # Создаем директорию для логов, если ее нет
        os.makedirs("logs", exist_ok=True)
        
        # Загружаем прошедшие матчи (url_result)
        matches_loader = MatchesLoader(db_path=args.db_path)
        matches_stats = matches_loader.load_past_matches_only()
        
        # Статистика для итогового отчета
        total_stats = {
            'matches_processed': matches_stats.get('processed', 0),
            'matches_success': matches_stats.get('success', 0),
            'matches_error': matches_stats.get('error', 0),
            'details_processed': 0,
            'details_success': 0,
            'details_error': 0,
            'player_stats_processed': 0,
            'player_stats_success': 0,
            'player_stats_error': 0
        }
        
        # Загружаем детали матчей и статистику игроков, если не пропущено
        if not args.skip_match_details or not args.skip_player_stats:
            details_loader = MatchDetailsLoader(db_path=args.db_path)
            
            # Используем специальный метод для загрузки, передавая флаги
            details_stats = details_loader.load_match_details_and_stats(
                skip_match_details=args.skip_match_details,
                skip_player_stats=args.skip_player_stats
            )
            
            # Обновляем общую статистику
            if not args.skip_match_details:
                total_stats['details_processed'] = details_stats.get('match_details_processed', 0)
                total_stats['details_success'] = details_stats.get('match_details_success', 0)
                total_stats['details_error'] = details_stats.get('match_details_error', 0)
                
            if not args.skip_player_stats:
                total_stats['player_stats_processed'] = details_stats.get('player_stats_processed', 0)
                total_stats['player_stats_success'] = details_stats.get('player_stats_success', 0)
                total_stats['player_stats_error'] = details_stats.get('player_stats_error', 0)
        
        # Выводим статистику
        logger.info("======== Загрузка прошедших матчей ========")
        logger.info(f"Обработано файлов: {total_stats['matches_processed']}")
        logger.info(f"Успешно загружено: {total_stats['matches_success']}")
        logger.info(f"Ошибок: {total_stats['matches_error']}")
        
        if not args.skip_match_details:
            logger.info("======== Загрузка деталей матчей ========")
            logger.info(f"Обработано файлов: {total_stats['details_processed']}")
            logger.info(f"Успешно загружено: {total_stats['details_success']}")
            logger.info(f"Ошибок: {total_stats['details_error']}")
        
        if not args.skip_player_stats:
            logger.info("======== Загрузка статистики игроков ========")
            logger.info(f"Обработано файлов: {total_stats['player_stats_processed']}")
            logger.info(f"Успешно загружено: {total_stats['player_stats_success']}")
            logger.info(f"Ошибок: {total_stats['player_stats_error']}")
        
        logger.info("Загрузка прошедших матчей завершена")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 