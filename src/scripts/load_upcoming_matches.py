#!/usr/bin/env python
"""
Скрипт для загрузки предстоящих матчей из JSON-файлов в базу данных.
Загружает все связанные данные, включая игроков команд.
"""
import os
import sys
import logging
import argparse
import sqlite3
from datetime import datetime
import time

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.loader.matches_loader import MatchesLoader
from src.config.constants import DATABASE_FILE

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/load_upcoming_matches.log")
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(description='Загрузка предстоящих матчей из JSON в базу данных')
    parser.add_argument('--db-path', type=str, default=DATABASE_FILE, help='Путь к файлу базы данных')
    parser.add_argument('--force', action='store_true', help='Принудительная загрузка, даже если файлы уже обработаны')
    return parser.parse_args()

def create_upcoming_match_players_table(db_path):
    """
    Создает таблицу upcoming_match_players, если она не существует
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу для игроков предстоящих матчей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS upcoming_match_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                team_id INTEGER,
                player_id INTEGER,
                player_nickname TEXT,
                team_position INTEGER,
                FOREIGN KEY (match_id) REFERENCES upcoming_urls (id)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Таблица upcoming_match_players успешно создана/проверена")
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы upcoming_match_players: {str(e)}")
        raise

def load_upcoming_players(db_path):
    """
    Загружает список игроков предстоящих матчей из JSON в базу данных
    """
    try:
        # Путь к JSON-файлу с игроками предстоящих матчей
        players_json_dir = "storage/json/upcoming_players"
        if not os.path.exists(players_json_dir):
            logger.info(f"Директория {players_json_dir} не существует, пропускаем загрузку игроков")
            return {"processed": 0, "success": 0, "error": 0}
        
        import json
        import glob
        
        stats = {"processed": 0, "success": 0, "error": 0}
        
        # Получаем все JSON-файлы в директории
        json_files = glob.glob(os.path.join(players_json_dir, "*.json"))
        logger.info(f"Найдено {len(json_files)} файлов с игроками предстоящих матчей")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for file_path in json_files:
            try:
                stats["processed"] += 1
                
                # Читаем JSON-файл
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if 'match_id' not in data or 'players' not in data:
                    logger.warning(f"В файле {file_path} отсутствуют необходимые данные")
                    stats["error"] += 1
                    continue
                
                match_id = data['match_id']
                
                # Проверяем, существует ли матч в базе данных
                cursor.execute('SELECT 1 FROM upcoming_urls WHERE id = ?', (match_id,))
                if not cursor.fetchone():
                    logger.warning(f"Матч с ID {match_id} не найден в базе данных, пропускаем")
                    stats["error"] += 1
                    continue
                
                # Удаляем существующие записи для этого матча
                cursor.execute('DELETE FROM upcoming_match_players WHERE match_id = ?', (match_id,))
                
                # Добавляем игроков
                for player in data['players']:
                    cursor.execute('''
                        INSERT INTO upcoming_match_players (
                            match_id, team_id, player_id, player_nickname, team_position
                        ) VALUES (?, ?, ?, ?, ?)
                    ''', (
                        match_id,
                        player.get('team_id'),
                        player.get('player_id'),
                        player.get('player_nickname'),
                        player.get('team_position', 0)
                    ))
                
                conn.commit()
                stats["success"] += 1
                logger.info(f"Загружены игроки для матча {match_id}")
                
                # Удаляем обработанный файл
                os.remove(file_path)
                logger.info(f"Файл {os.path.basename(file_path)} удален после обработки")
                
            except Exception as e:
                logger.error(f"Ошибка при загрузке игроков матча из {file_path}: {str(e)}")
                stats["error"] += 1
        
        conn.close()
        return stats
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке игроков предстоящих матчей: {str(e)}")
        return {"processed": 0, "success": 0, "error": len(json_files) if 'json_files' in locals() else 0}

def load_upcoming_matches_from_files(db_path):
    """
    Загружает предстоящие матчи из отдельных JSON-файлов в storage/json/upcoming_match/
    """
    import glob
    import json

    matches_json_dir = "storage/json/upcoming_match"
    if not os.path.exists(matches_json_dir):
        logger.info(f"Директория {matches_json_dir} не существует, пропускаем загрузку матчей")
        return {"processed": 0, "success": 0, "error": 0}

    stats = {"processed": 0, "success": 0, "error": 0}
    json_files = glob.glob(os.path.join(matches_json_dir, "*.json"))
    logger.info(f"Найдено {len(json_files)} файлов с предстоящими матчами")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for file_path in json_files:
        try:
            stats["processed"] += 1
            with open(file_path, 'r', encoding='utf-8') as f:
                match = json.load(f)
            cursor.execute('''
                INSERT OR REPLACE INTO upcoming_match (
                    match_id, datetime, team1_id, team1_name, team1_rank,
                    team2_id, team2_name, team2_rank, event_id, event_name,
                    head_to_head_team1_wins, head_to_head_team2_wins, status, parsed, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
            ''', (
                match['match_id'],
                match.get('datetime'),
                match.get('team1_id'),
                match.get('team1_name'),
                match.get('team1_rank'),
                match.get('team2_id'),
                match.get('team2_name'),
                match.get('team2_rank'),
                match.get('event_id'),
                match.get('event_name'),
                match.get('head_to_head_team1_wins'),
                match.get('head_to_head_team2_wins'),
                match.get('status', 'upcoming')
            ))
            conn.commit()
            stats["success"] += 1
            logger.info(f"Загружен матч {match['match_id']}")
            # Удаляем файл после успешной загрузки
            os.remove(file_path)
            logger.info(f"Файл {os.path.basename(file_path)} удален после загрузки")
        except Exception as e:
            logger.error(f"Ошибка при загрузке матча из {file_path}: {str(e)}")
            stats["error"] += 1

    conn.close()
    return stats

def cleanup_expired_upcoming_matches(db_path):
    """
    Удаляет устаревшие матчи (у которых datetime < текущее время) из upcoming_match
    и связанные с ними записи из upcoming_match_players
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    now = int(time.time())
    # Получаем id устаревших матчей
    cursor.execute("SELECT match_id FROM upcoming_match WHERE datetime < ?", (now,))
    expired_ids = [row[0] for row in cursor.fetchall()]
    if expired_ids:
        placeholders = ','.join(['?'] * len(expired_ids))
        # Удаляем из upcoming_match_players
        cursor.execute(f"DELETE FROM upcoming_match_players WHERE match_id IN ({placeholders})", expired_ids)
        # Удаляем из upcoming_match
        cursor.execute(f"DELETE FROM upcoming_match WHERE match_id IN ({placeholders})", expired_ids)
        conn.commit()
        logger.info(f"Удалено {len(expired_ids)} устаревших матчей и связанных игроков")
    else:
        logger.info("Нет устаревших матчей для удаления")
    conn.close()

def main():
    """
    Основная функция скрипта
    """
    args = parse_arguments()
    
    try:
        logger.info("Начало загрузки предстоящих матчей из JSON в базу данных")
        # Удаляем устаревшие матчи и игроков
        cleanup_expired_upcoming_matches(args.db_path)
        # Создаем таблицу для игроков предстоящих матчей
        create_upcoming_match_players_table(args.db_path)
        # Загружаем предстоящие матчи из отдельных файлов
        matches_stats = load_upcoming_matches_from_files(args.db_path)
        # Загружаем игроков предстоящих матчей
        players_stats = load_upcoming_players(args.db_path)
        # Выводим статистику
        logger.info("======== Загрузка предстоящих матчей ========")
        logger.info(f"Обработано файлов: {matches_stats.get('processed', 0)}")
        logger.info(f"Успешно загружено: {matches_stats.get('success', 0)}")
        logger.info(f"Ошибок: {matches_stats.get('error', 0)}")
        logger.info("======== Загрузка игроков предстоящих матчей ========")
        logger.info(f"Обработано файлов: {players_stats.get('processed', 0)}")
        logger.info(f"Успешно загружено: {players_stats.get('success', 0)}")
        logger.info(f"Ошибок: {players_stats.get('error', 0)}")
        logger.info("Загрузка предстоящих матчей завершена")
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 