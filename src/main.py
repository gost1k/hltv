"""
Main entry point for HLTV Parser
"""
import argparse
import logging
import sys
from src.parsers.manager import ParserManager
from src.collectors.manager import CollectorManager
from src.db.database import DatabaseService
from src.config.constants import LOG_LEVEL, LOG_FORMAT, LOG_FILE
from src.loader.match_details_loader import MatchDetailsLoader
from src.loader.matches_loader import MatchesLoader

# Setup logging
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
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='HLTV Parser CLI')
    
    # Parser commands
    parser.add_argument('--parse-matches', action='store_true', help='Parse matches page')
    parser.add_argument('--parse-results', action='store_true', help='Parse results page')
    parser.add_argument('--parse-results-details', action='store_true', help='Parse match details pages from DB')
    parser.add_argument('--parse-details', action='store_true', help='Alias for --parse-results-details (deprecated)')
    
    # Collector commands
    parser.add_argument('--collect-matches-list', action='store_true', help='Collect data from matches.html file')
    parser.add_argument('--collect-results-list', action='store_true', help='Collect data from results.html file')
    parser.add_argument('--collect-results-matches', action='store_true', help='Collect data from match details HTML files')
    
    # Backward compatibility
    parser.add_argument('--collect-lists', action='store_true', help='Collect data from both matches.html and results.html files (deprecated)')
    parser.add_argument('--collect-details', action='store_true', help='Alias for --collect-results-details (deprecated)')
    parser.add_argument('--collect-results-details', action='store_true', help='Alias for --collect-results-matches (deprecated)')
    
    # Optional parameters
    parser.add_argument('--details-limit', type=int, help='Limit of match details to parse')
    parser.add_argument('--test', action='store_true', help='Test mode: parse only 3 matches')
    parser.add_argument('--past', action='store_true', help='Parse past matches')
    parser.add_argument('--upcoming', action='store_true', help='Parse upcoming matches')
    
    # New commands
    parser.add_argument('--load-matches-from-json', action='store_true',
                      help='Загрузить списки матчей из JSON файлов в БД')
    parser.add_argument('--load-match-details-from-json', action='store_true',
                      help='Загрузить детали матчей из JSON файлов в БД')
    parser.add_argument('--load-upcoming-only', action='store_true',
                      help='Загрузить только предстоящие матчи из JSON')
    parser.add_argument('--load-past-only', action='store_true',
                      help='Загрузить только прошедшие матчи из JSON')
    parser.add_argument('--skip-match-details', action='store_true',
                      help='Пропустить загрузку деталей матчей (только при --load-past-only)')
    parser.add_argument('--skip-player-stats', action='store_true',
                      help='Пропустить загрузку статистики игроков (только при --load-past-only)')
    
    return parser.parse_args()

def init_database():
    """Initialize the database"""
    db_service = DatabaseService()
    if db_service.init_db():
        logger.info("Database initialized successfully")
        return True
    else:
        logger.error("Failed to initialize database")
        return False

def main():
    """Main entry point"""
    args = parse_arguments()
    logger.info("HLTV Parser Started")
    
    try:
        # Initialize database
        if not init_database():
            sys.exit(1)
            
        # Create manager instances
        parser_manager = ParserManager()
        collector_manager = CollectorManager()
        
        # Handle parser commands
        if args.parse_matches:
            logger.info("Starting matches page parsing...")
            matches_file = parser_manager.parse_matches()
            logger.info(f"Matches page saved to: {matches_file}")
            
        if args.parse_results:
            logger.info("Starting results page parsing...")
            results_file = parser_manager.parse_results()
            logger.info(f"Results page saved to: {results_file}")
            
        if args.parse_details or args.parse_results_details:
            if args.parse_details:
                logger.warning("The --parse-details command is deprecated. Please use --parse-results-details instead.")
                
            # Use test mode limit if specified
            if args.test:
                details_limit = 3
            elif args.details_limit:
                details_limit = args.details_limit
            else:
                details_limit = None
                
            # Determine which types of matches to parse
            parse_past = args.past or not args.upcoming  # Default to past if not specified
            parse_upcoming = args.upcoming
            
            # Log info about what's being parsed
            match_types = []
            if parse_past:
                match_types.append("past")
            if parse_upcoming:
                match_types.append("upcoming")
                
            limit_info = f"limit: {details_limit}" if details_limit else "no limit"
            logger.info(f"Parsing details for {', '.join(match_types)} matches ({limit_info})...")
            
            # Run the parser
            success_count = parser_manager.parse_match_details(
                limit=details_limit,
                parse_past=parse_past,
                parse_upcoming=parse_upcoming
            )
            logger.info(f"Match details parsing completed. Successfully parsed: {success_count}")
            
        # Handle collector commands
        if args.collect_matches_list:
            logger.info("Collecting data from matches.html...")
            matches_stats = collector_manager.collect_matches()
            logger.info(f"Matches list collection completed: {matches_stats}")
            if matches_stats.get('deleted', 0) > 0:
                logger.info(f"Removed {matches_stats['deleted']} obsolete matches from database.")
            
        if args.collect_results_list:
            logger.info("Collecting data from results.html...")
            results_stats = collector_manager.collect_results()
            logger.info(f"Results list collection completed: {results_stats}")
            
        if args.collect_lists:
            logger.warning("The --collect-lists command is deprecated. Please use --collect-matches-list and --collect-results-list separately.")
            logger.info("Collecting data from matches and results HTML...")
            collection_stats = collector_manager.collect_results_list()
            logger.info(f"Results list collection completed: {collection_stats}")
            if collection_stats.get('matches', {}).get('deleted', 0) > 0:
                logger.info(f"Removed {collection_stats['matches']['deleted']} obsolete matches from database.")
            
        if args.collect_results_matches or args.collect_results_details or args.collect_details:
            if args.collect_details:
                logger.warning("The --collect-details command is deprecated. Please use --collect-results-matches instead.")
            if args.collect_results_details:
                logger.warning("The --collect-results-details command is deprecated. Please use --collect-results-matches instead.")
                
            logger.info("Collecting data from match details HTML...")
            details_stats = collector_manager.collect_results_details()
            logger.info(f"Match details collection completed: {details_stats}")
            
        # Handle new commands
        if args.load_matches_from_json:
            loader = MatchesLoader()
            stats = loader.load_all()
            return
        
        if args.load_match_details_from_json:
            loader = MatchDetailsLoader()
            stats = loader.load_all()
            return
        
        # Загрузка только предстоящих матчей
        if args.load_upcoming_only:
            loader = MatchesLoader()
            stats = loader.load_upcoming_matches_only()
            logger.info("======== Загрузка предстоящих матчей ========")
            logger.info(f"Обработано файлов: {stats.get('processed', 0)}")
            logger.info(f"Успешно загружено: {stats.get('success', 0)}")
            logger.info(f"Ошибок: {stats.get('error', 0)}")
            return

        # Загрузка только прошедших матчей
        if args.load_past_only:
            matches_loader = MatchesLoader()
            matches_stats = matches_loader.load_past_matches_only()
            
            logger.info("======== Загрузка прошедших матчей ========")
            logger.info(f"Обработано файлов: {matches_stats.get('processed', 0)}")
            logger.info(f"Успешно загружено: {matches_stats.get('success', 0)}")
            logger.info(f"Ошибок: {matches_stats.get('error', 0)}")
            
            # Загружаем детали матчей и статистику игроков, если не пропущено
            if not args.skip_match_details or not args.skip_player_stats:
                details_loader = MatchDetailsLoader()
                details_stats = details_loader.load_match_details_and_stats(
                    skip_match_details=args.skip_match_details, 
                    skip_player_stats=args.skip_player_stats
                )
                
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
            
            return
        
        logger.info("HLTV Parser completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()