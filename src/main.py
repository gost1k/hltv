from database import init_db
from parser.matches import MatchesParser
from parser.results import ResultsParser

def main():
    print("HLTV Project Started")
    init_db()
    print("Database initialized")

    # Example of using matches parser
    with MatchesParser() as matches_parser:
        matches_file = matches_parser.parse()
        print(f"Matches page saved to: {matches_file}")
    
    # Example of using results parser
    with ResultsParser() as results_parser:
        results_file = results_parser.parse()
        print(f"Results page saved to: {results_file}")

if __name__ == "__main__":
    main()