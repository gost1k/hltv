#!/usr/bin/env python
"""
Скрипт для создания таблиц match_upcoming и match_upcoming_players в базе данных
"""

import sqlite3
import os
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Путь к базе данных
DB_PATH = "hltv.db"

def create_upcoming_tables():
    """
    Создание таблиц match_upcoming и match_upcoming_players в базе данных
    """
    if not os.path.exists(DB_PATH):
        logger.error(f"База данных не найдена по пути: {DB_PATH}")
        return False
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Создаем таблицу для предстоящих матчей
        cursor.execute('''
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
                stars INTEGER,
                description TEXT
            )
        ''')
        
        # Создаем таблицу для игроков предстоящих матчей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS match_upcoming_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER,
                player_id INTEGER,
                nickname TEXT,
                team_id INTEGER,
                FOREIGN KEY (match_id) REFERENCES match_upcoming (match_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        
        logger.info("Таблицы match_upcoming и match_upcoming_players успешно созданы")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц: {str(e)}")
        return False

def main():
    """
    Основная функция
    """
    result = create_upcoming_tables()
    if result:
        print("Таблицы успешно созданы")
    else:
        print("Произошла ошибка при создании таблиц. Проверьте лог для подробностей.")

if __name__ == "__main__":
    main() 