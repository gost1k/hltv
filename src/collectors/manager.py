"""
Collector Manager - handles all data collection operations
"""
import logging
from src.collector.matches import MatchesCollector
from src.collector.match_details import MatchDetailsCollector
from src.db.database import DatabaseService

logger = logging.getLogger(__name__)

class CollectorManager:
    """
    Manages all collectors and their operations
    """
    
    def __init__(self):
        self.db = DatabaseService()
        
    def collect_results(self):
        """
        Collect data from results HTML files and store in database
        
        Returns:
            dict: Statistics about the collection process
        """
        matches_collector = MatchesCollector()
        stats = matches_collector.collect_results()
        return stats
        
    def collect_matches(self):
        """
        Collect data from matches HTML files and store in database
        
        Returns:
            dict: Statistics about the collection process
        """
        matches_collector = MatchesCollector()
        stats = matches_collector.collect_matches()
        return stats
    
    def collect_results_list(self):
        """
        Collect data from matches and results HTML files and store in database
        
        Returns:
            dict: Statistics about the collection process
        """
        matches_stats = self.collect_matches()
        results_stats = self.collect_results()
        
        return {
            "matches": matches_stats,
            "results": results_stats
        }
        
    def collect_match_details(self, force=False, remove_processed=False):
        """
        Collect data from match details HTML files and store in database
        
        Args:
            force (bool): Deprecated parameter, not used
            remove_processed (bool): Deprecated parameter, not used
        
        Returns:
            dict: Statistics about the collection process
        """
        detail_collector = MatchDetailsCollector()
        stats = detail_collector.collect()
        
        # Convert to the expected format for backward compatibility
        return {
            "processed": stats['processed_files'],
            "success": stats['successful_match_details'],
            "failed": stats['errors'],
            "already_exists": stats['already_exists'],
            "updated": stats.get('updated', 0),
            "removed_files": stats.get('removed_files', 0)
        }
    
    def collect_results_details(self, force=False, remove_processed=False):
        """
        Collect data from match details HTML files and store in database.
        New name for collect_match_details for better naming consistency.
        
        Args:
            force (bool): Deprecated parameter, not used
            remove_processed (bool): Deprecated parameter, not used
            
        Returns:
            dict: Statistics about the collection process
        """
        return self.collect_match_details() 