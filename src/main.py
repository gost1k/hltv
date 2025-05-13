"""
Основной модуль HLTV Parser
"""
import argparse
import logging
from src.database import init_db
from src.parser.matches import MatchesParser
from src.parser.results import ResultsParser
from src.parser.match_details import MatchDetailsParser
from src.collector.matches import MatchesCollector
from src.collector.match_details import MatchDetailsCollector
from src.config import LOG_LEVEL, LOG_FORMAT, LOG_FILE

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='HLTV Parser CLI')
    parser.add_argument('--parse-matches', action='store_true', help='Parse matches page')
    parser.add_argument('--parse-results', action='store_true', help='Parse results page')
    parser.add_argument('--parse-details', action='store_true', help='Parse match details pages from DB')
    parser.add_argument('--details-limit', type=int, help='Limit of match details to parse (default: parse all)')
    parser.add_argument('--all-details', action='store_true', help='Parse all match details from DB without limit')
    parser.add_argument('--test', action='store_true', help='Test mode: parse only 3 matches')
    parser.add_argument('--parse-past', action='store_true', help='Parse past matches (from past_matches table)')
    parser.add_argument('--parse-upcoming', action='store_true', help='Parse upcoming matches (from upcoming_matches table)')
    parser.add_argument('--collect', action='store_true', help='Collect data from all HTML files (both lists and details)')
    parser.add_argument('--collect-lists', action='store_true', help='Collect data only from list HTML files (results.html)')
    parser.add_argument('--collect-details', action='store_true', help='Collect data only from match details HTML files in /storage/html/result/')
    parser.add_argument('--all', action='store_true', help='Run all operations')
    return parser.parse_args()

def main():
    """Основная функция программы"""
    args = parse_arguments()
    logger.info("HLTV Project Started")
    
    try:
        init_db()
        logger.info("Database initialized")

        if args.all or args.parse_matches:
            logger.info("Starting matches page parsing...")
            with MatchesParser() as matches_parser:
                matches_file = matches_parser.parse()
                logger.info(f"Matches page saved to: {matches_file}")
        
        if args.all or args.parse_results:
            logger.info("Starting results page parsing...")
            with ResultsParser() as results_parser:
                results_file = results_parser.parse()
                logger.info(f"Results page saved to: {results_file}")
        
        if args.all or args.collect or args.collect_lists:
            if args.collect or args.collect_lists:
                logger.info("Starting data collection from list HTML files...")
                # Собираем данные из списков матчей
                collector = MatchesCollector()
                collector.collect()
            
        if args.all or args.collect or args.collect_details:
            if args.collect or args.collect_details:
                # Собираем данные из деталей матчей
                logger.info("Starting match details collection from HTML files...")
                match_details_collector = MatchDetailsCollector()
                stats = match_details_collector.collect()
                logger.info(f"Match details collection completed. Stats: {stats['successful_match_details']} matches processed, {stats['successful_player_stats']} player stats saved")
            
        if args.all or args.collect or args.collect_lists or args.collect_details:
            logger.info("Data collection completed")
        
        if args.all or args.parse_details:
            # Если указан тестовый режим, используем лимит 3
            if args.test:
                details_limit = 3
            # Иначе, если указан конкретный лимит, используем его
            elif args.details_limit is not None:
                details_limit = args.details_limit
            # Если указан флаг --all-details или не указан --details-limit, парсим все матчи
            else:
                details_limit = None
            
            # Определяем, какие типы матчей нужно парсить
            parse_past = args.parse_past or not args.parse_upcoming  # По умолчанию парсим прошедшие
            parse_upcoming = args.parse_upcoming
            
            # Выводим информацию о том, что будем парсить
            match_types = []
            if parse_past:
                match_types.append("прошедших")
            if parse_upcoming:
                match_types.append("предстоящих")
            
            limit_info = "без лимита" if details_limit is None else f"лимит: {details_limit}"
            logger.info(f"Starting match details parsing ({', '.join(match_types)} матчей, {limit_info})...")
            
            details_parser = MatchDetailsParser(
                limit=details_limit,
                parse_past=parse_past,
                parse_upcoming=parse_upcoming
            )
            successful = details_parser.parse()
            logger.info(f"Match details parsing completed. Successful: {successful}")
            
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()