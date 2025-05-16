import os
import json
import logging
import sqlite3
from datetime import datetime

# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
JSON_OUTPUT_DIR = "storage/json"
UPCOMING_MATCHES_JSON_FILE = os.path.join(JSON_OUTPUT_DIR, "upcoming_matches.json")
PAST_MATCHES_JSON_FILE = os.path.join(JSON_OUTPUT_DIR, "past_matches.json")
DATABASE_FILE = "hltv.db"

class MatchesLoader:
    """
    Class for loading match lists from JSON to database
    """
    def __init__(self, db_path=DATABASE_FILE):
        self.db_path = db_path
        
    def load_all(self):
        """
        Loads all data from JSON files to database
        
        Returns:
            dict: Loading statistics
        """
        # Create tables if they don't exist
        self._create_tables()
        
        stats = {
            'upcoming_matches_processed': 0,
            'upcoming_matches_success': 0,
            'upcoming_matches_error': 0,
            'past_matches_processed': 0,
            'past_matches_success': 0,
            'past_matches_error': 0
        }
        
        # Load upcoming matches
        if os.path.exists(UPCOMING_MATCHES_JSON_FILE):
            try:
                stats['upcoming_matches_processed'] += 1
                self._load_upcoming_matches()
                stats['upcoming_matches_success'] += 1
                # Optionally delete file after processing
                # os.remove(UPCOMING_MATCHES_JSON_FILE)
                # logger.info(f"File {os.path.basename(UPCOMING_MATCHES_JSON_FILE)} deleted after processing")
            except Exception as e:
                logger.error(f"Error loading upcoming matches: {str(e)}")
                stats['upcoming_matches_error'] += 1
        else:
            logger.info(f"Upcoming matches file not found: {UPCOMING_MATCHES_JSON_FILE}")
        
        # Load past matches
        if os.path.exists(PAST_MATCHES_JSON_FILE):
            try:
                stats['past_matches_processed'] += 1
                self._load_past_matches()
                stats['past_matches_success'] += 1
                # Optionally delete file after processing
                # os.remove(PAST_MATCHES_JSON_FILE)
                # logger.info(f"File {os.path.basename(PAST_MATCHES_JSON_FILE)} deleted after processing")
            except Exception as e:
                logger.error(f"Error loading past matches: {str(e)}")
                stats['past_matches_error'] += 1
        else:
            logger.info(f"Past matches file not found: {PAST_MATCHES_JSON_FILE}")
        
        return stats
    
    def _create_tables(self):
        """Creates necessary tables in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create table for upcoming matches if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS upcoming_urls (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    date INTEGER NOT NULL,
                    toParse INTEGER NOT NULL DEFAULT 1
                )
            ''')
            
            # Create table for match results if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS result_urls (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    toParse INTEGER NOT NULL DEFAULT 1
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Tables successfully created/verified")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
    
    def _load_upcoming_matches(self):
        """
        Loads upcoming matches from JSON file to database
        """
        try:
            # Read file
            with open(UPCOMING_MATCHES_JSON_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'matches' not in data or not data['matches']:
                logger.warning(f"No data about upcoming matches in file {UPCOMING_MATCHES_JSON_FILE}")
                return
                
            matches = data['matches']
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get list of all match IDs in current data
            current_match_ids = [match['id'] for match in matches]
            
            # Get list of all IDs in database
            cursor.execute('SELECT id FROM upcoming_urls')
            db_match_ids = [row[0] for row in cursor.fetchall()]
            
            # Find match IDs that are in DB but not in current data
            obsolete_ids = [match_id for match_id in db_match_ids if match_id not in current_match_ids]
            
            # Delete obsolete matches
            deleted_count = 0
            if obsolete_ids:
                placeholders = ','.join(['?'] * len(obsolete_ids))
                delete_query = f'DELETE FROM upcoming_urls WHERE id IN ({placeholders})'
                cursor.execute(delete_query, obsolete_ids)
                deleted_count = cursor.rowcount
                logger.info(f"Deleted {deleted_count} obsolete matches from upcoming_urls table")
            
            # Process each match
            new_count = 0
            updated_count = 0
            
            for match in matches:
                # Check if match exists in database
                cursor.execute('SELECT toParse FROM upcoming_urls WHERE id = ?', (match['id'],))
                result = cursor.fetchone()
                
                if result is not None:
                    # Update existing match (preserve current toParse value)
                    to_parse = result[0]
                    cursor.execute('''
                        UPDATE upcoming_urls SET date = ?, toParse = ?
                        WHERE id = ?
                    ''', (match['date'], to_parse, match['id']))
                    updated_count += 1
                else:
                    # Add new match
                    to_parse = match.get('toParse', 1)  # Use value from JSON or 1 by default
                    cursor.execute('''
                        INSERT INTO upcoming_urls (id, url, date, toParse)
                        VALUES (?, ?, ?, ?)
                    ''', (match['id'], match['url'], match['date'], to_parse))
                    new_count += 1
                
                # Delete duplicate entries from results table (if match moved)
                cursor.execute('DELETE FROM result_urls WHERE id = ?', (match['id'],))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Upcoming matches loading completed: new - {new_count}, updated - {updated_count}, deleted - {deleted_count}")
            
        except Exception as e:
            logger.error(f"Error loading upcoming matches: {str(e)}")
            raise
    
    def _load_past_matches(self):
        """
        Loads past matches from JSON file to database
        """
        try:
            # Read file
            with open(PAST_MATCHES_JSON_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'matches' not in data or not data['matches']:
                logger.warning(f"No data about past matches in file {PAST_MATCHES_JSON_FILE}")
                return
                
            matches = data['matches']
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Process each match
            new_count = 0
            updated_count = 0
            
            for match in matches:
                # Check if match exists in database
                cursor.execute('SELECT toParse FROM result_urls WHERE id = ?', (match['id'],))
                result = cursor.fetchone()
                
                if result is not None:
                    # Match already exists, update toParse if needed
                    to_parse = result[0]
                    if to_parse != match.get('toParse', 1):
                        cursor.execute('''
                            UPDATE result_urls SET toParse = ?
                            WHERE id = ?
                        ''', (match['toParse'], match['id']))
                    updated_count += 1
                else:
                    # Add new match
                    to_parse = match.get('toParse', 1)  # Use value from JSON or 1 by default
                    cursor.execute('''
                        INSERT INTO result_urls (id, url, toParse)
                        VALUES (?, ?, ?)
                    ''', (match['id'], match['url'], to_parse))
                    new_count += 1
                    
                    # Delete match from upcoming if it moved to results
                    cursor.execute('DELETE FROM upcoming_urls WHERE id = ?', (match['id'],))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Past matches loading completed: new - {new_count}, updated - {updated_count}")
            
        except Exception as e:
            logger.error(f"Error loading past matches: {str(e)}")
            raise

    def load_upcoming_matches_only(self):
        """
        Loads only upcoming matches from JSON file to database
        
        Returns:
            dict: Loading statistics
        """
        # Create tables if they don't exist
        self._create_tables()
        
        stats = {
            'processed': 0,
            'success': 0,
            'error': 0
        }
        
        # Load upcoming matches
        if os.path.exists(UPCOMING_MATCHES_JSON_FILE):
            try:
                stats['processed'] += 1
                self._load_upcoming_matches()
                stats['success'] += 1
            except Exception as e:
                logger.error(f"Error loading upcoming matches: {str(e)}")
                stats['error'] += 1
        else:
            logger.warning(f"Upcoming matches file not found: {UPCOMING_MATCHES_JSON_FILE}")
            stats['error'] += 1
        
        return stats
    
    def load_past_matches_only(self):
        """
        Loads only past matches from JSON file to database
        
        Returns:
            dict: Loading statistics
        """
        # Create tables if they don't exist
        self._create_tables()
        
        stats = {
            'processed': 0,
            'success': 0,
            'error': 0
        }
        
        # Load past matches
        if os.path.exists(PAST_MATCHES_JSON_FILE):
            try:
                stats['processed'] += 1
                self._load_past_matches()
                stats['success'] += 1
            except Exception as e:
                logger.error(f"Error loading past matches: {str(e)}")
                stats['error'] += 1
        else:
            logger.warning(f"Past matches file not found: {PAST_MATCHES_JSON_FILE}")
            stats['error'] += 1
        
        return stats

if __name__ == "__main__":
    loader = MatchesLoader()
    stats = loader.load_all()
    
    # Output statistics
    logger.info("======== Loading upcoming matches ========")
    logger.info(f"Processed files: {stats['upcoming_matches_processed']}")
    logger.info(f"Successfully loaded: {stats['upcoming_matches_success']}")
    logger.info(f"Errors: {stats['upcoming_matches_error']}")
    
    logger.info("======== Loading past matches ========")
    logger.info(f"Processed files: {stats['past_matches_processed']}")
    logger.info(f"Successfully loaded: {stats['past_matches_success']}")
    logger.info(f"Errors: {stats['past_matches_error']}") 