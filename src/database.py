"""
Модуль для работы с базой данных
"""
import sqlite3
import logging
from src.config import DATABASE_NAME, LOG_LEVEL, LOG_FORMAT, LOG_FILE

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_connection():
    """Получение соединения с базой данных"""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def init_db():
    """Инициализация базы данных"""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Создаем таблицу матчей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team1 TEXT NOT NULL,
                    team2 TEXT NOT NULL,
                    match_time DATETIME,
                    event TEXT,
                    status TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Создаем таблицу результатов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team1 TEXT NOT NULL,
                    team2 TEXT NOT NULL,
                    score1 INTEGER,
                    score2 INTEGER,
                    event TEXT,
                    match_date DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            logger.info("Database initialized successfully")
    except sqlite3.Error as e:
        logger.error(f"Error initializing database: {e}")
        raise
