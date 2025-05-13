"""
Основной модуль HLTV Parser
"""
import argparse
import logging
from src.database import init_db
from src.parser.matches import MatchesParser
from src.parser.results import ResultsParser
from src.collector.matches import MatchesCollector
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
    parser.add_argument('--collect', action='store_true', help='Collect data from HTML files')
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
        
        if args.all or args.collect:
            logger.info("Starting data collection from HTML files...")
            collector = MatchesCollector()
            collector.collect()
            logger.info("Data collection completed")
            
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__" or __name__ == "src.main":
    main()