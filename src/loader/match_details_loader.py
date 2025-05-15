import os
import json
import logging
import sqlite3
import glob
from datetime import datetime

# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
JSON_OUTPUT_DIR = "storage/json"
MATCH_DETAILS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "match_details")
PLAYER_STATS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "player_stats")
DATABASE_FILE = "hltv.db"

class MatchDetailsLoader:
    """
    Class for loading match details from JSON to database
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
            'match_details_processed': 0,
            'match_details_success': 0,
            'match_details_error': 0,
            'player_stats_processed': 0,
            'player_stats_success': 0,
            'player_stats_error': 0
        }
        
        # Load match details
        match_details_files = glob.glob(os.path.join(MATCH_DETAILS_JSON_DIR, "*.json"))
        logger.info(f"Found {len(match_details_files)} files with match details")
        
        for file_path in match_details_files:
            try:
                stats['match_details_processed'] += 1
                self._load_match_details(file_path)
                stats['match_details_success'] += 1
                # Delete processed file
                os.remove(file_path)
                logger.info(f"File {os.path.basename(file_path)} deleted after processing")
            except Exception as e:
                logger.error(f"Error loading match details from {file_path}: {str(e)}")
                stats['match_details_error'] += 1
        
        # Load player statistics
        player_stats_files = glob.glob(os.path.join(PLAYER_STATS_JSON_DIR, "*.json"))
        logger.info(f"Found {len(player_stats_files)} files with player statistics")
        
        for file_path in player_stats_files:
            try:
                stats['player_stats_processed'] += 1
                self._load_player_stats(file_path)
                stats['player_stats_success'] += 1
                # Delete processed file
                os.remove(file_path)
                logger.info(f"File {os.path.basename(file_path)} deleted after processing")
            except Exception as e:
                logger.error(f"Error loading player statistics from {file_path}: {str(e)}")
                stats['player_stats_error'] += 1
        
        return stats
    
    def load_match_details_and_stats(self, skip_match_details=False, skip_player_stats=False):
        """
        Loads data from JSON files to database with the ability to skip certain types of data
        
        Args:
            skip_match_details (bool): Skip loading match details
            skip_player_stats (bool): Skip loading player statistics
            
        Returns:
            dict: Loading statistics
        """
        # Create tables if they don't exist
        self._create_tables()
        
        stats = {
            'match_details_processed': 0,
            'match_details_success': 0,
            'match_details_error': 0,
            'player_stats_processed': 0,
            'player_stats_success': 0,
            'player_stats_error': 0
        }
        
        # Load match details
        if not skip_match_details:
            match_details_files = glob.glob(os.path.join(MATCH_DETAILS_JSON_DIR, "*.json"))
            logger.info(f"Found {len(match_details_files)} files with match details")
            
            for file_path in match_details_files:
                try:
                    stats['match_details_processed'] += 1
                    self._load_match_details(file_path)
                    stats['match_details_success'] += 1
                    # Delete processed file if not needed
                    os.remove(file_path)
                    logger.info(f"File {os.path.basename(file_path)} deleted after processing")
                except Exception as e:
                    logger.error(f"Error loading match details from {file_path}: {str(e)}")
                    stats['match_details_error'] += 1
        
        # Load player statistics
        if not skip_player_stats:
            player_stats_files = glob.glob(os.path.join(PLAYER_STATS_JSON_DIR, "*.json"))
            logger.info(f"Found {len(player_stats_files)} files with player statistics")
            
            for file_path in player_stats_files:
                try:
                    stats['player_stats_processed'] += 1
                    self._load_player_stats(file_path)
                    stats['player_stats_success'] += 1
                    # Delete processed file if not needed
                    os.remove(file_path)
                    logger.info(f"File {os.path.basename(file_path)} deleted after processing")
                except Exception as e:
                    logger.error(f"Error loading player statistics from {file_path}: {str(e)}")
                    stats['player_stats_error'] += 1
        
        return stats
    
    def _create_tables(self):
        """Creates necessary tables in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create match_details table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS match_details (
                    match_id INTEGER PRIMARY KEY,
                    datetime INTEGER,
                    team1_id INTEGER,
                    team1_name TEXT,
                    team1_score INTEGER,
                    team1_rank INTEGER,
                    team2_id INTEGER,
                    team2_name TEXT,
                    team2_score INTEGER,
                    team2_rank INTEGER,
                    event_id INTEGER,
                    event_name TEXT,
                    demo_id INTEGER,
                    head_to_head_team1_wins INTEGER,
                    head_to_head_team2_wins INTEGER,
                    status TEXT,
                    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create player_stats table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id INTEGER NOT NULL,
                    team_id INTEGER,
                    player_id INTEGER,
                    player_nickname TEXT,
                    fullName TEXT,
                    nickName TEXT,
                    kills INTEGER,
                    deaths INTEGER,
                    kd_ratio REAL,
                    plus_minus INTEGER,
                    adr REAL,
                    kast REAL,
                    rating REAL,
                    FOREIGN KEY (match_id) REFERENCES match_details (match_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Tables successfully created/verified")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
    
    def _load_match_details(self, file_path):
        """
        Loads match details from a JSON file to the database
        
        Args:
            file_path (str): Path to JSON file
        """
        try:
            # Read file
            with open(file_path, 'r', encoding='utf-8') as f:
                match_data = json.load(f)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if record already exists for this match
            cursor.execute('SELECT 1 FROM match_details WHERE match_id = ?', (match_data['match_id'],))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Update existing record
                cursor.execute('''
                    UPDATE match_details SET 
                    datetime = ?,
                    team1_id = ?,
                    team1_name = ?,
                    team1_score = ?,
                    team1_rank = ?,
                    team2_id = ?,
                    team2_name = ?,
                    team2_score = ?,
                    team2_rank = ?,
                    event_id = ?,
                    event_name = ?,
                    demo_id = ?,
                    head_to_head_team1_wins = ?,
                    head_to_head_team2_wins = ?,
                    status = ?,
                    parsed_at = CURRENT_TIMESTAMP
                    WHERE match_id = ?
                ''', (
                    match_data['datetime'],
                    match_data['team1_id'],
                    match_data['team1_name'],
                    match_data['team1_score'],
                    match_data['team1_rank'],
                    match_data['team2_id'],
                    match_data['team2_name'],
                    match_data['team2_score'],
                    match_data['team2_rank'],
                    match_data['event_id'],
                    match_data['event_name'],
                    match_data['demo_id'],
                    match_data['head_to_head_team1_wins'],
                    match_data['head_to_head_team2_wins'],
                    match_data['status'],
                    match_data['match_id']
                ))
                logger.info(f"Updated match details for match ID {match_data['match_id']}")
            else:
                # Insert new record
                cursor.execute('''
                    INSERT INTO match_details (
                        match_id, datetime, team1_id, team1_name, team1_score, team1_rank,
                        team2_id, team2_name, team2_score, team2_rank,
                        event_id, event_name, demo_id,
                        head_to_head_team1_wins, head_to_head_team2_wins, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_data['match_id'],
                    match_data['datetime'],
                    match_data['team1_id'],
                    match_data['team1_name'],
                    match_data['team1_score'],
                    match_data['team1_rank'],
                    match_data['team2_id'],
                    match_data['team2_name'],
                    match_data['team2_score'],
                    match_data['team2_rank'],
                    match_data['event_id'],
                    match_data['event_name'],
                    match_data['demo_id'],
                    match_data['head_to_head_team1_wins'],
                    match_data['head_to_head_team2_wins'],
                    match_data['status']
                ))
                logger.info(f"Inserted new match details for match ID {match_data['match_id']}")
                
                # Update the toParse flag in the results table to indicate successful processing
                cursor.execute('''
                    UPDATE url_result SET toParse = 0 
                    WHERE id = ?
                ''', (match_data['match_id'],))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error processing match details from {file_path}: {str(e)}")
            raise
    
    def _load_player_stats(self, file_path):
        """
        Loads player statistics from a JSON file to the database
        
        Args:
            file_path (str): Path to JSON file
        """
        try:
            # Read file
            with open(file_path, 'r', encoding='utf-8') as f:
                stats_data = json.load(f)
            
            match_id = stats_data['match_id']
            team_players = stats_data['teams']
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Delete existing stats for this match if any
            cursor.execute('DELETE FROM player_stats WHERE match_id = ?', (match_id,))
            
            # Process each team
            for team_data in team_players:
                team_id = team_data['team_id']
                
                # Process each player
                for player_data in team_data['players']:
                    cursor.execute('''
                        INSERT INTO player_stats (
                            match_id, team_id, player_id, player_nickname,
                            fullName, nickName, kills, deaths, kd_ratio,
                            plus_minus, adr, kast, rating
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        match_id,
                        team_id,
                        player_data['player_id'],
                        player_data['player_nickname'],
                        player_data.get('fullName', ''),
                        player_data.get('nickName', ''),
                        player_data.get('kills', 0),
                        player_data.get('deaths', 0),
                        player_data.get('kd_ratio', 0.0),
                        player_data.get('plus_minus', 0),
                        player_data.get('adr', 0.0),
                        player_data.get('kast', 0.0),
                        player_data.get('rating', 0.0)
                    ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Loaded player statistics for match ID {match_id}, teams: {len(team_players)}")
            
        except Exception as e:
            logger.error(f"Error processing player statistics from {file_path}: {str(e)}")
            raise

if __name__ == "__main__":
    loader = MatchDetailsLoader()
    stats = loader.load_all()
    
    # Output statistics
    logger.info("======== Loading match details ========")
    logger.info(f"Processed files: {stats['match_details_processed']}")
    logger.info(f"Successfully loaded: {stats['match_details_success']}")
    logger.info(f"Errors: {stats['match_details_error']}")
    
    logger.info("======== Loading player statistics ========")
    logger.info(f"Processed files: {stats['player_stats_processed']}")
    logger.info(f"Successfully loaded: {stats['player_stats_success']}")
    logger.info(f"Errors: {stats['player_stats_error']}") 