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
            
            # Проверяем существование таблиц
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Создаем таблицы только если они не существуют
            if "upcoming_urls" not in tables or "result_urls" not in tables:
                logger.info("Необходимые таблицы не найдены, создаем новые")
                
                # Создаем таблицу для предстоящих матчей
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS url_upcoming (
                        id INTEGER PRIMARY KEY,
                        url TEXT NOT NULL,
                        date INTEGER NOT NULL,
                        toParse INTEGER NOT NULL DEFAULT 1
                    )
                ''')
                
                # Создаем таблицу для результатов матчей
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS url_result (
                        id INTEGER PRIMARY KEY,
                        url TEXT NOT NULL,
                        toParse INTEGER NOT NULL DEFAULT 1
                    )
                ''')
            
            conn.commit()
            logger.info("Database initialized successfully")
    except sqlite3.Error as e:
        logger.error(f"Error initializing database: {e}")
        raise
