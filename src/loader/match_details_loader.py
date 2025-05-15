import os
import json
import logging
import sqlite3
import glob
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Константы
JSON_OUTPUT_DIR = "storage/json"
MATCH_DETAILS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "match_details")
PLAYER_STATS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "player_stats")
DATABASE_FILE = "hltv.db"

class MatchDetailsLoader:
    """
    Класс для загрузки деталей матчей из JSON в базу данных
    """
    def __init__(self, db_path=DATABASE_FILE):
        self.db_path = db_path
        
    def load_all(self):
        """
        Загружает все данные из JSON файлов в базу данных
        
        Returns:
            dict: Статистика загрузки
        """
        # Создаем таблицы, если их нет
        self._create_tables()
        
        stats = {
            'match_details_processed': 0,
            'match_details_success': 0,
            'match_details_error': 0,
            'player_stats_processed': 0,
            'player_stats_success': 0,
            'player_stats_error': 0
        }
        
        # Загружаем детали матчей
        match_details_files = glob.glob(os.path.join(MATCH_DETAILS_JSON_DIR, "*.json"))
        logger.info(f"Найдено {len(match_details_files)} файлов с деталями матчей")
        
        for file_path in match_details_files:
            try:
                stats['match_details_processed'] += 1
                self._load_match_details(file_path)
                stats['match_details_success'] += 1
                # Удаляем обработанный файл
                os.remove(file_path)
                logger.info(f"Файл {os.path.basename(file_path)} удален после обработки")
            except Exception as e:
                logger.error(f"Ошибка при загрузке деталей матча из {file_path}: {str(e)}")
                stats['match_details_error'] += 1
        
        # Загружаем статистику игроков
        player_stats_files = glob.glob(os.path.join(PLAYER_STATS_JSON_DIR, "*.json"))
        logger.info(f"Найдено {len(player_stats_files)} файлов со статистикой игроков")
        
        for file_path in player_stats_files:
            try:
                stats['player_stats_processed'] += 1
                self._load_player_stats(file_path)
                stats['player_stats_success'] += 1
                # Удаляем обработанный файл
                os.remove(file_path)
                logger.info(f"Файл {os.path.basename(file_path)} удален после обработки")
            except Exception as e:
                logger.error(f"Ошибка при загрузке статистики игроков из {file_path}: {str(e)}")
                stats['player_stats_error'] += 1
        
        return stats
    
    def load_match_details_and_stats(self, skip_match_details=False, skip_player_stats=False):
        """
        Загружает данные из JSON файлов в базу данных с возможностью пропуска определенных типов данных
        
        Args:
            skip_match_details (bool): Пропустить загрузку деталей матчей
            skip_player_stats (bool): Пропустить загрузку статистики игроков
            
        Returns:
            dict: Статистика загрузки
        """
        # Создаем таблицы, если их нет
        self._create_tables()
        
        stats = {
            'match_details_processed': 0,
            'match_details_success': 0,
            'match_details_error': 0,
            'player_stats_processed': 0,
            'player_stats_success': 0,
            'player_stats_error': 0
        }
        
        # Загружаем детали матчей
        if not skip_match_details:
            match_details_files = glob.glob(os.path.join(MATCH_DETAILS_JSON_DIR, "*.json"))
            logger.info(f"Найдено {len(match_details_files)} файлов с деталями матчей")
            
            for file_path in match_details_files:
                try:
                    stats['match_details_processed'] += 1
                    self._load_match_details(file_path)
                    stats['match_details_success'] += 1
                    # Удаляем обработанный файл, если не нужно сохранять
                    os.remove(file_path)
                    logger.info(f"Файл {os.path.basename(file_path)} удален после обработки")
                except Exception as e:
                    logger.error(f"Ошибка при загрузке деталей матча из {file_path}: {str(e)}")
                    stats['match_details_error'] += 1
        
        # Загружаем статистику игроков
        if not skip_player_stats:
            player_stats_files = glob.glob(os.path.join(PLAYER_STATS_JSON_DIR, "*.json"))
            logger.info(f"Найдено {len(player_stats_files)} файлов со статистикой игроков")
            
            for file_path in player_stats_files:
                try:
                    stats['player_stats_processed'] += 1
                    self._load_player_stats(file_path)
                    stats['player_stats_success'] += 1
                    # Удаляем обработанный файл, если не нужно сохранять
                    os.remove(file_path)
                    logger.info(f"Файл {os.path.basename(file_path)} удален после обработки")
                except Exception as e:
                    logger.error(f"Ошибка при загрузке статистики игроков из {file_path}: {str(e)}")
                    stats['player_stats_error'] += 1
        
        return stats
    
    def _create_tables(self):
        """Создает необходимые таблицы в базе данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Создаем таблицу match_details, если она не существует
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
                    demo_id INTEGER,
                    head_to_head_team1_wins INTEGER,
                    head_to_head_team2_wins INTEGER,
                    status TEXT,
                    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Создаем таблицу player_stats, если она не существует
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    match_id INTEGER NOT NULL,
                    team_id INTEGER,
                    player_id INTEGER,
                    player_nickname TEXT,
                    fullName TEXT,
                    nickName TEXT,
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
            
            conn.commit()
            conn.close()
            logger.info("Таблицы успешно созданы/проверены")
            
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц: {str(e)}")
            raise
    
    def _load_match_details(self, file_path):
        """
        Загружает детали матча из JSON файла в базу данных
        
        Args:
            file_path (str): Путь к JSON файлу
        """
        try:
            # Чтение файла
            with open(file_path, 'r', encoding='utf-8') as f:
                match_data = json.load(f)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Проверяем, существует ли уже запись для этого матча
            cursor.execute('SELECT 1 FROM match_details WHERE match_id = ?', (match_data['match_id'],))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Обновляем существующую запись
                cursor.execute('''
                    UPDATE match_details SET 
                    datetime = ?,
                    team1_id = ?,
                    team1_name = ?,
                    team1_score = ?,
                    team1_rank = ?,
                    team2_id = ?,
                    team2_name = ?,
                    team2_score = ?,
                    team2_rank = ?,
                    event_id = ?,
                    event_name = ?,
                    demo_id = ?,
                    head_to_head_team1_wins = ?,
                    head_to_head_team2_wins = ?,
                    status = ?,
                    parsed_at = CURRENT_TIMESTAMP
                    WHERE match_id = ?
                ''', (
                    match_data['datetime'],
                    match_data['team1_id'],
                    match_data['team1_name'],
                    match_data['team1_score'],
                    match_data['team1_rank'],
                    match_data['team2_id'],
                    match_data['team2_name'],
                    match_data['team2_score'],
                    match_data['team2_rank'],
                    match_data['event_id'],
                    match_data['event_name'],
                    match_data['demo_id'],
                    match_data['head_to_head_team1_wins'],
                    match_data['head_to_head_team2_wins'],
                    match_data['status'],
                    match_data['match_id']
                ))
                logger.info(f"Обновлены детали матча {match_data['match_id']}")
            else:
                # Добавляем новую запись
                cursor.execute('''
                    INSERT INTO match_details (
                    match_id, datetime, team1_id, team1_name, team1_score, team1_rank,
                    team2_id, team2_name, team2_score, team2_rank, event_id, event_name,
                    demo_id, head_to_head_team1_wins, head_to_head_team2_wins, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_data['match_id'],
                    match_data['datetime'],
                    match_data['team1_id'],
                    match_data['team1_name'],
                    match_data['team1_score'],
                    match_data['team1_rank'],
                    match_data['team2_id'],
                    match_data['team2_name'],
                    match_data['team2_score'],
                    match_data['team2_rank'],
                    match_data['event_id'],
                    match_data['event_name'],
                    match_data['demo_id'],
                    match_data['head_to_head_team1_wins'],
                    match_data['head_to_head_team2_wins'],
                    match_data['status']
                ))
                logger.info(f"Добавлены детали матча {match_data['match_id']}")
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке деталей матча из {file_path}: {str(e)}")
            raise
    
    def _load_player_stats(self, file_path):
        """
        Загружает статистику игроков из JSON файла в базу данных
        
        Args:
            file_path (str): Путь к JSON файлу
        """
        try:
            # Чтение файла
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'players' not in data or not data['players']:
                logger.warning(f"В файле {file_path} отсутствуют данные игроков")
                return
                
            players_data = data['players']
            match_id = players_data[0]['match_id']
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Удаляем существующие записи для этого матча
            cursor.execute('DELETE FROM player_stats WHERE match_id = ?', (match_id,))
            
            # Добавляем новые записи
            for player_data in players_data:
                cursor.execute('''
                    INSERT INTO player_stats (
                    match_id, team_id, player_id, player_nickname,
                    fullName, nickName,
                    kills, deaths, kd_ratio, plus_minus,
                    adr, kast, rating
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    player_data['match_id'],
                    player_data['team_id'],
                    player_data['player_id'],
                    player_data['player_nickname'],
                    player_data['fullName'],
                    player_data['nickName'],
                    player_data['kills'],
                    player_data['deaths'],
                    player_data['kd_ratio'],
                    player_data['plus_minus'],
                    player_data['adr'],
                    player_data['kast'],
                    player_data['rating']
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Сохранена статистика {len(players_data)} игроков для матча {match_id}")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке статистики игроков из {file_path}: {str(e)}")
            raise

if __name__ == "__main__":
    loader = MatchDetailsLoader()
    stats = loader.load_all()
    
    # Вывод статистики
    logger.info("======== Загрузка деталей матчей ========")
    logger.info(f"Обработано файлов: {stats['match_details_processed']}")
    logger.info(f"Успешно загружено: {stats['match_details_success']}")
    logger.info(f"Ошибок: {stats['match_details_error']}")
    
    logger.info("======== Загрузка статистики игроков ========")
    logger.info(f"Обработано файлов: {stats['player_stats_processed']}")
    logger.info(f"Успешно загружено: {stats['player_stats_success']}")
    logger.info(f"Ошибок: {stats['player_stats_error']}") 