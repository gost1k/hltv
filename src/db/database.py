"""
Database Service for HLTV Parser
"""
import os
import sqlite3
import logging
from typing import List, Dict, Any, Optional, Tuple

from src.config.constants import DATABASE_FILE

logger = logging.getLogger(__name__)

class DatabaseService:
    """
    Handles all database operations for the HLTV Parser
    """
    
    def __init__(self, db_file: str = DATABASE_FILE):
        self.db_file = db_file
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to the database"""
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return False
            
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            
    def init_db(self):
        """Initialize the database with tables if they don't exist"""
        if not self.connect():
            return False
        
        try:
            # Teams table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    logo_url TEXT,
                    country TEXT,
                    world_ranking INTEGER,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Players table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS players (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    nickname TEXT NOT NULL,
                    team_id INTEGER,
                    country TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (team_id) REFERENCES teams (id)
                )
            ''')
            
            # Events table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    start_date DATE,
                    end_date DATE,
                    location TEXT,
                    prize_pool TEXT,
                    teams_count INTEGER,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Matches table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    team1_id INTEGER,
                    team2_id INTEGER,
                    date TIMESTAMP NOT NULL,
                    event_id INTEGER,
                    event_name TEXT,
                    match_format TEXT,
                    status TEXT DEFAULT 'upcoming',
                    winner_id INTEGER,
                    score_team1 INTEGER,
                    score_team2 INTEGER,
                    parsed BOOLEAN DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (team1_id) REFERENCES teams (id),
                    FOREIGN KEY (team2_id) REFERENCES teams (id),
                    FOREIGN KEY (event_id) REFERENCES events (id)
                )
            ''')
            
            # Maps table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS maps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    pick TEXT,
                    score_team1 INTEGER,
                    score_team2 INTEGER,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (match_id) REFERENCES matches (id)
                )
            ''')
            
            # Match players table (for match lineups)
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS match_players (
                    match_id INTEGER NOT NULL,
                    player_id INTEGER NOT NULL,
                    team_id INTEGER NOT NULL,
                    stats TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (match_id, player_id),
                    FOREIGN KEY (match_id) REFERENCES matches (id),
                    FOREIGN KEY (player_id) REFERENCES players (id),
                    FOREIGN KEY (team_id) REFERENCES teams (id)
                )
            ''')
            
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            return False
        finally:
            self.close()
            
    def get_match_ids_for_parsing(self, is_past: bool = True, limit: Optional[int] = None) -> List[int]:
        """
        Get match IDs that need to be parsed
        
        Args:
            is_past (bool): True for past matches, False for upcoming
            limit (int, optional): Maximum number of matches to return
            
        Returns:
            List[int]: List of match IDs to parse
        """
        self.connect()
        try:
            status = "completed" if is_past else "upcoming"
            query = """
                SELECT id FROM matches 
                WHERE status = ? AND parsed = 0
                ORDER BY date DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
                
            self.cursor.execute(query, (status,))
            match_ids = [row['id'] for row in self.cursor.fetchall()]
            return match_ids
        except sqlite3.Error as e:
            logger.error(f"Error getting match IDs for parsing: {e}")
            return []
        finally:
            self.close()
            
    def update_match_parsed_status(self, match_id: int, parsed: bool = True) -> bool:
        """
        Update the parsed status of a match
        
        Args:
            match_id (int): Match ID
            parsed (bool): Whether the match has been parsed
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.connect()
        try:
            self.cursor.execute(
                "UPDATE matches SET parsed = ?, last_updated = CURRENT_TIMESTAMP WHERE id = ?",
                (1 if parsed else 0, match_id)
            )
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"Error updating match parsed status: {e}")
            return False
        finally:
            self.close()
            
    # Additional database methods would go here 