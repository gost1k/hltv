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
from src.utils.telegram_log_handler import TelegramLogHandler

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

# Получить токен dev_bot из конфига
import json
with open("src/bots/config/dev_bot_config.json", encoding="utf-8") as f:
    dev_bot_config = json.load(f)
dev_bot_token = dev_bot_config["token"]

# Указать user_id нужного пользователя (7146832422)
telegram_handler = TelegramLogHandler(dev_bot_token, chat_id="7146832422")
telegram_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
telegram_handler.setFormatter(formatter)
logger.addHandler(telegram_handler)

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
        logger.info("Таблица upcoming_match_players успешно создана/проверена", extra={"no_telegram": True})
        
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
        logger.info(f"Найдено {len(json_files)} файлов с игроками предстоящих матчей", extra={"no_telegram": True})
        
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
    logger.info(f"Найдено {len(json_files)} файлов с предстоящими матчами", extra={"no_telegram": True})

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
    Удаляет устаревшие матчи (у которых datetime < текущее время - 2 часа) из upcoming_match
    и связанные с ними записи из upcoming_match_players и upcoming_match_streamers
    Возвращает кортеж (кол-во матчей, кол-во игроков, кол-во стримов)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    now = int(time.time())
    two_hours_ago = now - 2 * 60 * 60
    # Получаем id устаревших матчей (старше 2 часов)
    cursor.execute("SELECT match_id FROM upcoming_match WHERE datetime < ?", (two_hours_ago,))
    expired_ids = [row[0] for row in cursor.fetchall()]
    deleted_matches = len(expired_ids)
    deleted_players = 0
    deleted_streams = 0
    if expired_ids:
        placeholders = ','.join(['?'] * len(expired_ids))
        # Считаем сколько игроков и стримов будет удалено
        cursor.execute(f"SELECT COUNT(*) FROM upcoming_match_players WHERE match_id IN ({placeholders})", expired_ids)
        deleted_players = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM upcoming_match_streamers WHERE match_id IN ({placeholders})", expired_ids)
        deleted_streams = cursor.fetchone()[0]
        # Удаляем из upcoming_match_players
        cursor.execute(f"DELETE FROM upcoming_match_players WHERE match_id IN ({placeholders})", expired_ids)
        # Удаляем из upcoming_match_streamers
        cursor.execute(f"DELETE FROM upcoming_match_streamers WHERE match_id IN ({placeholders})", expired_ids)
        # Удаляем из upcoming_match
        cursor.execute(f"DELETE FROM upcoming_match WHERE match_id IN ({placeholders})", expired_ids)
        conn.commit()
        logger.info(f"Удалено {deleted_matches} устаревших матчей, {deleted_players} игроков, {deleted_streams} стримов")
    else:
        logger.info("Нет устаревших матчей для удаления", extra={"no_telegram": True})
    conn.close()
    return deleted_matches, deleted_players, deleted_streams

def create_upcoming_match_streamers_table(db_path):
    """
    Создает таблицу upcoming_match_streamers, если она не существует
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS upcoming_match_streamers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                name TEXT,
                lang TEXT,
                url TEXT,
                FOREIGN KEY (match_id) REFERENCES upcoming_urls (id)
            )
        ''')
        conn.commit()
        conn.close()
        logger.info("Таблица upcoming_match_streamers успешно создана/проверена")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы upcoming_match_streamers: {str(e)}")
        raise

def load_upcoming_streamers(db_path):
    """
    Загружает список стримеров предстоящих матчей из JSON в базу данных
    """
    try:
        streamers_json_dir = "storage/json/upcoming_streams"
        if not os.path.exists(streamers_json_dir):
            logger.info(f"Директория {streamers_json_dir} не существует, пропускаем загрузку стримеров")
            return {"processed": 0, "success": 0, "error": 0}
        import json
        import glob
        stats = {"processed": 0, "success": 0, "error": 0}
        json_files = glob.glob(os.path.join(streamers_json_dir, "*.json"))
        logger.info(f"Найдено {len(json_files)} файлов со стримерами предстоящих матчей", extra={"no_telegram": True})
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for file_path in json_files:
            try:
                stats["processed"] += 1
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if 'match_id' not in data or 'streams' not in data:
                    logger.warning(f"В файле {file_path} отсутствуют необходимые данные")
                    stats["error"] += 1
                    continue
                match_id = data['match_id']
                cursor.execute('SELECT 1 FROM upcoming_urls WHERE id = ?', (match_id,))
                if not cursor.fetchone():
                    logger.warning(f"Матч с ID {match_id} не найден в базе данных, пропускаем")
                    stats["error"] += 1
                    continue
                cursor.execute('DELETE FROM upcoming_match_streamers WHERE match_id = ?', (match_id,))
                for streamer in data['streams']:
                    cursor.execute('''
                        INSERT INTO upcoming_match_streamers (
                            match_id, name, lang, url
                        ) VALUES (?, ?, ?, ?)
                    ''', (
                        match_id,
                        streamer.get('name'),
                        streamer.get('lang'),
                        streamer.get('url')
                    ))
                conn.commit()
                stats["success"] += 1
                logger.info(f"Загружены стримеры для матча {match_id}")
                os.remove(file_path)
                logger.info(f"Файл {os.path.basename(file_path)} удален после обработки")
            except Exception as e:
                logger.error(f"Ошибка при загрузке стримеров матча из {file_path}: {str(e)}")
                stats["error"] += 1
        conn.close()
        return stats
    except Exception as e:
        logger.error(f"Ошибка при загрузке стримеров предстоящих матчей: {str(e)}")
        return {"processed": 0, "success": 0, "error": len(json_files) if 'json_files' in locals() else 0}

def update_upcoming_urls_to_parse(db_path):
    import math
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    now = int(time.time())
    three_days_sec = 3 * 24 * 3600

    # Получаем все id, date из upcoming_urls
    cursor.execute("SELECT id, date FROM upcoming_urls")
    rows = cursor.fetchall()
    for match_id, match_time in rows:
        # Если date в миллисекундах (больше 10 цифр), переводим в секунды и обновляем
        if match_time and match_time > 9999999999:
            match_time = int(match_time // 1000)
            cursor.execute("UPDATE upcoming_urls SET date=? WHERE id=?", (match_time, match_id))
        # Вычисляем ближайший 6-часовой слот до даты матча, который больше текущего времени, но не позже даты матча
        # Слоты: ..., match_time-12h, match_time-6h, match_time
        if not match_time or match_time <= now:
            slot = match_time  # если матч уже прошёл или невалиден, просто ставим текущее значение
        else:
            slot = match_time
            while slot - 6*3600 > now:
                slot -= 6*3600
            if slot <= now:
                slot = match_time
        # Обновляем next_update для всех записей
        cursor.execute("UPDATE upcoming_urls SET next_update=? WHERE id=?", (slot, match_id))
        # reParse=1 только если матч в будущем и до него 3 дня или меньше
        if match_time and match_time > now and match_time - now <= three_days_sec:
            cursor.execute("UPDATE upcoming_urls SET reParse=1 WHERE id=?", (match_id,))
        else:
            cursor.execute("UPDATE upcoming_urls SET reParse=0 WHERE id=?", (match_id,))
    conn.commit()
    conn.close()

def send_telegram_report(stats, logger):
    # stats: dict с ключами
    #   deleted_matches, deleted_players, deleted_streams
    #   matches_processed, matches_success, matches_error
    #   players_processed, players_success, players_error
    #   streamers_processed, streamers_success, streamers_error
    all_values = [
        stats.get('deleted_matches', 0), stats.get('deleted_players', 0), stats.get('deleted_streams', 0),
        stats.get('matches_processed', 0), stats.get('matches_success', 0), stats.get('matches_error', 0),
        stats.get('players_processed', 0), stats.get('players_success', 0), stats.get('players_error', 0),
        stats.get('streamers_processed', 0), stats.get('streamers_success', 0), stats.get('streamers_error', 0)
    ]
    if all(v == 0 for v in all_values):
        logger.info("Загрузка БУДУЩИХ матчей (Обновлений нету)", extra={"telegram_firstline": True})
    else:
        msg = ["Загрузка БУДУЩИХ матчей"]
        msg.append(f"Удалено - {stats.get('deleted_matches', 0)}/{stats.get('deleted_players', 0)}/{stats.get('deleted_streams', 0)}")
        msg.append(f"Матчей - {stats.get('matches_processed', 0)}/{stats.get('matches_success', 0)}/{stats.get('matches_error', 0)}")
        msg.append(f"Игроков - {stats.get('players_processed', 0)}/{stats.get('players_success', 0)}/{stats.get('players_error', 0)}")
        msg.append(f"Стримеров - {stats.get('streamers_processed', 0)}/{stats.get('streamers_success', 0)}/{stats.get('streamers_error', 0)}")
        logger.info("\n".join(msg), extra={"telegram_firstline": True})

def main():
    """
    Основная функция скрипта
    """
    args = parse_arguments()
    
    try:
        logger.info("Загрузка БУДУЩИХ матчей", extra={"telegram_firstline": True})
        logger.info("Начало загрузки предстоящих матчей из JSON в базу данных", extra={"no_telegram": True})
        # Удаляем устаревшие матчи и игроков
        deleted_matches, deleted_players, deleted_streams = cleanup_expired_upcoming_matches(args.db_path)
        # Создаем таблицу для игроков предстоящих матчей
        create_upcoming_match_players_table(args.db_path)
        # Загружаем предстоящие матчи из отдельных файлов
        matches_stats = load_upcoming_matches_from_files(args.db_path)
        # После загрузки матчей обновляем toParse в upcoming_urls
        update_upcoming_urls_to_parse(args.db_path)
        # Загружаем игроков предстоящих матчей
        players_stats = load_upcoming_players(args.db_path)
        # Загружаем стримеры предстоящих матчей
        streamers_stats = load_upcoming_streamers(args.db_path)
        # Выводим статистику
        send_telegram_report({"deleted_matches": deleted_matches, "deleted_players": deleted_players, "deleted_streams": deleted_streams,
                             "matches_processed": matches_stats.get('processed', 0), "matches_success": matches_stats.get('success', 0), "matches_error": matches_stats.get('error', 0),
                             "players_processed": players_stats.get('processed', 0), "players_success": players_stats.get('success', 0), "players_error": players_stats.get('error', 0),
                             "streamers_processed": streamers_stats.get('processed', 0), "streamers_success": streamers_stats.get('success', 0), "streamers_error": streamers_stats.get('error', 0)}, logger)
        logger.info("Загрузка предстоящих матчей завершена")
    except Exception as e:
        logger.error(f"Ошибка при выполнении скрипта: {str(e)}")
        sys.exit(1)
    finally:
        for handler in logger.handlers:
            if hasattr(handler, 'send_buffer'):
                handler.send_buffer()

if __name__ == "__main__":
    main() 