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
            # Удаляю создание лишних таблиц
            # Оставьте только нужные CREATE TABLE ... если они реально нужны для работы других функций
            self.conn.commit()
            # Проверяем, нужно ли добавить колонки миграции
            # self._migrate_db_if_needed()  # Можно закомментировать, если не нужно
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS result_match_maps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id INTEGER NOT NULL,
                    map_name TEXT NOT NULL,
                    team1_rounds INTEGER,
                    team2_rounds INTEGER,
                    rounds TEXT
                )
            ''')
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
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='upcoming_match'")
            if not self.cursor.fetchone():
                logger.warning("Table upcoming_match does not exist yet, skipping migration")
                return
                
            # Получаем список колонок в таблице upcoming_match
            self.cursor.execute("PRAGMA table_info(upcoming_match)")
            columns_info = self.cursor.fetchall()
            columns = [column[1] for column in columns_info]
            
            logger.info(f"Existing columns in upcoming_match: {columns}")
            
            # Проверяем отсутствующие колонки и добавляем их
            if "parsed" not in columns:
                logger.info("Adding 'parsed' column to upcoming_match table")
                self.cursor.execute("ALTER TABLE upcoming_match ADD COLUMN parsed INTEGER DEFAULT 0")
                logger.info("'parsed' column added successfully")
            else:
                logger.info("'parsed' column already exists")
            
            if "last_updated" not in columns:
                logger.info("Adding 'last_updated' column to upcoming_match table")
                # SQLite не поддерживает DEFAULT CURRENT_TIMESTAMP при ALTER TABLE
                self.cursor.execute("ALTER TABLE upcoming_match ADD COLUMN last_updated TEXT")
                # Устанавливаем начальное значение для last_updated
                self.cursor.execute("UPDATE upcoming_match SET last_updated = datetime('now') WHERE last_updated IS NULL")
                logger.info("'last_updated' column added successfully")
            else:
                logger.info("'last_updated' column already exists")
            
            self.conn.commit()
            
            # Проверяем после миграции
            self.cursor.execute("PRAGMA table_info(upcoming_match)")
            columns_after = [column[1] for column in self.cursor.fetchall()]
            logger.info(f"Columns after migration: {columns_after}")
            
            logger.info("Database migration completed successfully")
        except sqlite3.Error as e:
            logger.error(f"Database migration error: {e}")
            # Добавляем вывод запроса для отладки
            self.cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='upcoming_match'")
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
                SELECT match_id FROM upcoming_match 
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
            self.cursor.execute("PRAGMA table_info(upcoming_match)")
            columns = [column[1] for column in self.cursor.fetchall()]
            
            if "parsed" not in columns:
                # Попытаемся добавить колонку, если ее нет
                logger.warning("Column 'parsed' does not exist in upcoming_match, attempting to add it")
                try:
                    self.cursor.execute("ALTER TABLE upcoming_match ADD COLUMN parsed INTEGER DEFAULT 0")
                    self.conn.commit()
                    logger.info("Added 'parsed' column to upcoming_match table")
                except sqlite3.Error as e:
                    logger.error(f"Failed to add 'parsed' column: {e}")
                    return False
            
            # Теперь выполняем обновление
            self.cursor.execute(
                "UPDATE upcoming_match SET parsed = ?, last_updated = datetime('now') WHERE match_id = ?",
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