"""
Скрипт для создания таблиц данных матчей и статистики игроков
"""
import sqlite3
import logging
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_match_tables(db_path="hltv.db"):
    """
    Создает таблицы для хранения данных матчей и статистики игроков
    
    Args:
        db_path (str): Путь к файлу базы данных
        
    Returns:
        bool: True если успешно, False если ошибка
    """
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу для деталей матча
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
                demo_url TEXT,
                head_to_head_team1_wins INTEGER,
                head_to_head_team2_wins INTEGER,
                parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logger.info("Таблица match_details успешно создана или уже существует")
        
        # Создаем таблицу для статистики игроков
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER,
                team_id INTEGER,
                player_id INTEGER,
                player_nickname TEXT,
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
        logger.info("Таблица player_stats успешно создана или уже существует")
        
        # Создаем индексы для ускорения поиска
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_player_stats_match_id ON player_stats (match_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_player_stats_player_id ON player_stats (player_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_match_details_team1_id ON match_details (team1_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_match_details_team2_id ON match_details (team2_id)
        ''')
        
        # Подтверждаем транзакцию
        conn.commit()
        
        # Проверяем, что таблицы созданы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name='match_details' OR name='player_stats')")
        tables = cursor.fetchall()
        
        if len(tables) == 2:
            logger.info("Таблицы успешно созданы")
        else:
            logger.warning(f"Что-то пошло не так, найдено таблиц: {len(tables)}")
        
        # Закрываем соединение
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Запуск создания таблиц для хранения данных матчей")
    
    if create_match_tables():
        logger.info("Таблицы успешно созданы")
    else:
        logger.error("Не удалось создать таблицы") 