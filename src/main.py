import argparse
from database import init_db
from parser.matches import MatchesParser
from parser.results import ResultsParser
from collector.matches import MatchesCollector

def parse_arguments():
    parser = argparse.ArgumentParser(description='HLTV Parser CLI')
    parser.add_argument('--parse-matches', action='store_true', help='Parse matches page')
    parser.add_argument('--parse-results', action='store_true', help='Parse results page')
    parser.add_argument('--collect', action='store_true', help='Collect data from HTML files')
    parser.add_argument('--all', action='store_true', help='Run all operations')
    return parser.parse_args()

def main():
    args = parse_arguments()
    print("HLTV Project Started")
    init_db()
    print("Database initialized")

    if args.all or args.parse_matches:
        print("\nПарсим страницу матчей...")
        with MatchesParser() as matches_parser:
            matches_file = matches_parser.parse()
            print(f"Matches page saved to: {matches_file}")
    
    if args.all or args.parse_results:
        print("\nПарсим страницу результатов...")
        with ResultsParser() as results_parser:
            results_file = results_parser.parse()
            print(f"Results page saved to: {results_file}")
    
    if args.all or args.collect:
        print("\nНачинаем сбор данных из HTML файлов...")
        collector = MatchesCollector()
        collector.collect()
    collector = MatchesCollector()
    collector.collect()
    print("Сбор данных завершен")

if __name__ == "__main__":
    main()