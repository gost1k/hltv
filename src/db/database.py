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
            
            # Upcoming matches table (was 'matches' before)
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS match_upcoming (
                    match_id INTEGER PRIMARY KEY,
                    datetime INTEGER,
                    team1_id INTEGER,
                    team1_name TEXT,
                    team1_rank INTEGER,
                    team2_id INTEGER,
                    team2_name TEXT,
                    team2_rank INTEGER,
                    event_id INTEGER,
                    event_name TEXT,
                    head_to_head_team1_wins INTEGER,
                    head_to_head_team2_wins INTEGER,
                    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'upcoming',
                    parsed INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (team1_id) REFERENCES teams (id),
                    FOREIGN KEY (team2_id) REFERENCES teams (id),
                    FOREIGN KEY (event_id) REFERENCES events (id)
                )
            ''')
            
            # Upcoming match players table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS match_upcoming_players (
                    match_id INTEGER NOT NULL,
                    team_id INTEGER NOT NULL,
                    player_id INTEGER,
                    player_nickname TEXT NOT NULL,
                    fullName TEXT,
                    nickName TEXT,
                    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (match_id, player_nickname),
                    FOREIGN KEY (match_id) REFERENCES match_upcoming (match_id),
                    FOREIGN KEY (team_id) REFERENCES teams (id),
                    FOREIGN KEY (player_id) REFERENCES players (id)
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
                    FOREIGN KEY (match_id) REFERENCES match_details (id)
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
                    FOREIGN KEY (match_id) REFERENCES match_details (id),
                    FOREIGN KEY (player_id) REFERENCES players (id),
                    FOREIGN KEY (team_id) REFERENCES teams (id)
                )
            ''')
            
            self.conn.commit()
            
            # Проверяем, нужно ли добавить колонки миграции
            self._migrate_db_if_needed()
            
            return True
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            return False
        finally:
            self.close()
            
    def _migrate_db_if_needed(self):
        """
        Проверяет необходимость миграции БД и добавляет отсутствующие колонки
        """
        try:
            # Проверяем существование таблицы
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='match_upcoming'")
            if not self.cursor.fetchone():
                logger.warning("Table match_upcoming does not exist yet, skipping migration")
                return
                
            # Получаем список колонок в таблице match_upcoming
            self.cursor.execute("PRAGMA table_info(match_upcoming)")
            columns_info = self.cursor.fetchall()
            columns = [column[1] for column in columns_info]
            
            logger.info(f"Existing columns in match_upcoming: {columns}")
            
            # Проверяем отсутствующие колонки и добавляем их
            if "parsed" not in columns:
                logger.info("Adding 'parsed' column to match_upcoming table")
                self.cursor.execute("ALTER TABLE match_upcoming ADD COLUMN parsed INTEGER DEFAULT 0")
                logger.info("'parsed' column added successfully")
            else:
                logger.info("'parsed' column already exists")
            
            if "last_updated" not in columns:
                logger.info("Adding 'last_updated' column to match_upcoming table")
                # SQLite не поддерживает DEFAULT CURRENT_TIMESTAMP при ALTER TABLE
                self.cursor.execute("ALTER TABLE match_upcoming ADD COLUMN last_updated TEXT")
                # Устанавливаем начальное значение для last_updated
                self.cursor.execute("UPDATE match_upcoming SET last_updated = datetime('now') WHERE last_updated IS NULL")
                logger.info("'last_updated' column added successfully")
            else:
                logger.info("'last_updated' column already exists")
            
            self.conn.commit()
            
            # Проверяем после миграции
            self.cursor.execute("PRAGMA table_info(match_upcoming)")
            columns_after = [column[1] for column in self.cursor.fetchall()]
            logger.info(f"Columns after migration: {columns_after}")
            
            logger.info("Database migration completed successfully")
        except sqlite3.Error as e:
            logger.error(f"Database migration error: {e}")
            # Добавляем вывод запроса для отладки
            self.cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='match_upcoming'")
            table_def = self.cursor.fetchone()
            if table_def:
                logger.error(f"Current table definition: {table_def[0]}")
            
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
                SELECT match_id FROM match_upcoming 
                WHERE status = ? AND parsed = 0
                ORDER BY datetime DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
                
            self.cursor.execute(query, (status,))
            match_ids = [row['match_id'] for row in self.cursor.fetchall()]
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
            # Сначала проверим, существует ли колонка parsed
            self.cursor.execute("PRAGMA table_info(match_upcoming)")
            columns = [column[1] for column in self.cursor.fetchall()]
            
            if "parsed" not in columns:
                # Попытаемся добавить колонку, если ее нет
                logger.warning("Column 'parsed' does not exist in match_upcoming, attempting to add it")
                try:
                    self.cursor.execute("ALTER TABLE match_upcoming ADD COLUMN parsed INTEGER DEFAULT 0")
                    self.conn.commit()
                    logger.info("Added 'parsed' column to match_upcoming table")
                except sqlite3.Error as e:
                    logger.error(f"Failed to add 'parsed' column: {e}")
                    return False
            
            # Теперь выполняем обновление
            self.cursor.execute(
                "UPDATE match_upcoming SET parsed = ?, last_updated = datetime('now') WHERE match_id = ?",
                (1 if parsed else 0, match_id)
            )
            self.conn.commit()
            
            # Проверяем, была ли обновлена хотя бы одна строка
            if self.cursor.rowcount > 0:
                logger.info(f"Successfully updated match {match_id} parsed status to {parsed}")
                return True
            else:
                logger.warning(f"No rows affected when updating match {match_id} parsed status")
                return True  # Возвращаем True, так как ошибки не было, просто не было затронутых строк
                
        except sqlite3.Error as e:
            logger.error(f"Error updating match parsed status: {e}")
            return False
        finally:
            self.close()
            
    # Additional database methods would go here 