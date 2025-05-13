"""
Скрипт для сбора результатов прошедших матчей
"""
import logging
import argparse
from src.collector.match_details import MatchDetailsCollector

def main():
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description="Сбор данных о прошедших матчах")
    parser.add_argument("--limit", type=int, default=0, help="Ограничение количества обрабатываемых файлов")
    parser.add_argument("--html-dir", type=str, default="storage/html/result", help="Путь к директории с HTML-файлами прошедших матчей")
    parser.add_argument("--db", type=str, default="hltv.db", help="Путь к файлу базы данных")
    args = parser.parse_args()
    
    logger.info("Запуск сбора данных о прошедших матчах")
    logger.info(f"Директория с HTML: {args.html_dir}")
    logger.info(f"База данных: {args.db}")
    
    # Создаем коллектор и запускаем сбор данных
    collector = MatchDetailsCollector(html_dir=args.html_dir, db_path=args.db)
    stats = collector.collect(limit=args.limit)
    
    # Выводим статистику
    logger.info("Сбор данных завершен")
    logger.info(f"Обработано {stats['processed_files']} файлов из {stats['total_files']}")
    logger.info(f"Успешно извлечено данных матчей: {stats['successful_match_details']}")
    logger.info(f"Успешно извлечено данных игроков: {stats['successful_player_stats']}")
    logger.info(f"Ошибок: {stats['errors']}")

if __name__ == "__main__":
    main() 