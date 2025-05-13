from src.collector.match_details import MatchDetailsCollector

# Create collector instance
collector = MatchDetailsCollector()

# Run collection
stats = collector.collect()

# Print stats
print(f"Collection completed!")
print(f"Total files: {stats['total_files']}")
print(f"Processed files: {stats['processed_files']}")
print(f"Successfully saved match details: {stats['successful_match_details']}")
print(f"Successfully saved player stats: {stats['successful_player_stats']}")
print(f"Deleted files: {stats['deleted_files']}")
print(f"Errors: {stats['errors']}")
print(f"Incomplete stats (only one team): {stats['incomplete_stats']}") 