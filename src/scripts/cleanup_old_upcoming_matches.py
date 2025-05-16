#!/usr/bin/env python
"""
Скрипт для очистки устаревших матчей из таблиц upcoming

1. Удаляет из таблицы match_upcoming матчи, у которых datetime (в unix-формате)
   меньше текущего времени, то есть матчи, которые уже прошли.
2. Удаляет из таблицы match_upcoming_players все записи, которые не связаны
   с актуальными матчами в таблице match_upcoming.
"""

import sqlite3
import logging
import time
from datetime import datetime
import sys
import os

# Настройка логирования
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "cleanup_upcoming_matches.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("cleanup_upcoming_matches")

def cleanup_old_upcoming_matches(db_path="hltv.db"):
    """
    Очищает таблицы match_upcoming и match_upcoming_players
    
    1. Удаляет прошедшие матчи из таблицы match_upcoming
    2. Удаляет из match_upcoming_players все записи, которые не связаны с актуальными матчами
    
    Args:
        db_path (str): Путь к файлу базы данных SQLite
    """
    logger.info("Запуск очистки устаревших матчей из таблиц upcoming")
    
    try:
        # Текущее время в формате UNIX timestamp
        current_time = int(time.time())
        logger.info(f"Текущее время: {current_time} ({datetime.fromtimestamp(current_time)})")
        
        # Подключение к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Шаг 1: Удаление прошедших матчей из match_upcoming
        # Получаем ID матчей, которые уже прошли
        cursor.execute(
            "SELECT match_id, datetime, team1_name, team2_name FROM match_upcoming WHERE datetime < ?", 
            (current_time,)
        )
        expired_matches = cursor.fetchall()
        
        if expired_matches:
            # Логируем информацию о матчах, которые будут удалены
            logger.info(f"Найдено {len(expired_matches)} устаревших матчей для удаления:")
            for match in expired_matches:
                match_id, match_time, team1, team2 = match
                match_date = datetime.fromtimestamp(match_time).strftime('%Y-%m-%d %H:%M')
                logger.info(f"  - ID: {match_id}, Дата: {match_date}, Матч: {team1} vs {team2}")
            
            # Удаляем записи из таблицы матчей
            match_ids = [match[0] for match in expired_matches]
            placeholders = ', '.join('?' for _ in match_ids)
            cursor.execute(
                f"DELETE FROM match_upcoming WHERE match_id IN ({placeholders})", 
                match_ids
            )
            logger.info(f"Удалено {cursor.rowcount} устаревших матчей из таблицы match_upcoming")
        else:
            logger.info("Нет устаревших матчей для удаления")
        
        # Шаг 2: Удаление записей из match_upcoming_players, которые не связаны с match_upcoming
        # Получаем список всех актуальных match_id из match_upcoming
        cursor.execute("SELECT match_id FROM match_upcoming")
        valid_match_ids = [row[0] for row in cursor.fetchall()]
        
        if not valid_match_ids:
            logger.info("Нет актуальных матчей в таблице match_upcoming, очищаем все записи из match_upcoming_players")
            cursor.execute("DELETE FROM match_upcoming_players")
            logger.info(f"Удалено {cursor.rowcount} записей из таблицы match_upcoming_players")
        else:
            # Получаем записи из match_upcoming_players, которые не связаны с актуальными матчами
            placeholders = ', '.join('?' for _ in valid_match_ids)
            cursor.execute(
                f"SELECT COUNT(*) FROM match_upcoming_players WHERE match_id NOT IN ({placeholders})",
                valid_match_ids
            )
            orphaned_players_count = cursor.fetchone()[0]
            
            if orphaned_players_count > 0:
                logger.info(f"Найдено {orphaned_players_count} записей в match_upcoming_players без связи с актуальными матчами")
                
                # Удаляем записи игроков, которые не связаны с актуальными матчами
                cursor.execute(
                    f"DELETE FROM match_upcoming_players WHERE match_id NOT IN ({placeholders})",
                    valid_match_ids
                )
                logger.info(f"Удалено {cursor.rowcount} записей из таблицы match_upcoming_players")
            else:
                logger.info("Все записи в match_upcoming_players связаны с актуальными матчами, очистка не требуется")
        
        # Сохраняем изменения
        conn.commit()
        logger.info("Изменения в базе данных сохранены")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка при очистке устаревших матчей: {e}")
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    cleanup_old_upcoming_matches() 