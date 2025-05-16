#!/usr/bin/env python
"""
Модуль для загрузки деталей матчей и статистики игроков из JSON в базу данных
"""
import os
import json
import logging
import sqlite3
import glob
from datetime import datetime

# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
JSON_OUTPUT_DIR = "storage/json"
MATCH_DETAILS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "result_match")
PLAYER_STATS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "player_stats")
RESULT_MAPS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "result_maps")
DATABASE_FILE = "hltv.db"

class MatchDetailsLoader:
    """
    Class for loading match details from JSON to database
    """
    def __init__(self, db_path=DATABASE_FILE):
        self.db_path = db_path
        
    def load_all(self):
        """
        Loads all data from JSON files to database
        
        Returns:
            dict: Loading statistics
        """
        stats = {
            'match_details_processed': 0,
            'match_details_success': 0,
            'match_details_error': 0,
            'player_stats_processed': 0,
            'player_stats_success': 0,
            'player_stats_error': 0,
            'maps_processed': 0,
            'maps_success': 0,
            'maps_error': 0
        }
        
        # Load match details
        match_details_files = glob.glob(os.path.join(MATCH_DETAILS_JSON_DIR, "*.json"))
        logger.info(f"Found {len(match_details_files)} files with match details")
        
        for file_path in match_details_files:
            try:
                stats['match_details_processed'] += 1
                self._load_match_details(file_path)
                stats['match_details_success'] += 1
                os.remove(file_path)
                logger.info(f"File {os.path.basename(file_path)} deleted after processing")
            except Exception as e:
                logger.error(f"Error loading match details from {file_path}: {str(e)}")
                stats['match_details_error'] += 1
        
        # Load player statistics
        player_stats_files = glob.glob(os.path.join(PLAYER_STATS_JSON_DIR, "*.json"))
        logger.info(f"Found {len(player_stats_files)} files with player statistics")
        
        for file_path in player_stats_files:
            try:
                stats['player_stats_processed'] += 1
                self._load_player_stats(file_path)
                stats['player_stats_success'] += 1
                os.remove(file_path)
                logger.info(f"File {os.path.basename(file_path)} deleted after processing")
            except Exception as e:
                logger.error(f"Error loading player statistics from {file_path}: {str(e)}")
                stats['player_stats_error'] += 1
        
        # Load match maps
        maps_files = glob.glob(os.path.join(RESULT_MAPS_JSON_DIR, "*.json"))
        logger.info(f"Found {len(maps_files)} files with match maps")
        for file_path in maps_files:
            try:
                stats['maps_processed'] += 1
                self._load_match_maps(file_path)
                stats['maps_success'] += 1
                os.remove(file_path)
                logger.info(f"File {os.path.basename(file_path)} deleted after processing (maps)")
            except Exception as e:
                stats['maps_error'] += 1
                logger.error(f"Error loading match maps from {file_path}: {str(e)}")
        
        return stats
    
    def load_match_details_and_stats(self, skip_match_details=False, skip_player_stats=False):
        """
        Loads data from JSON files to database with the ability to skip certain types of data
        
        Args:
            skip_match_details (bool): Skip loading match details
            skip_player_stats (bool): Skip loading player statistics
            
        Returns:
            dict: Loading statistics
        """
        stats = {
            'match_details_processed': 0,
            'match_details_success': 0,
            'match_details_error': 0,
            'player_stats_processed': 0,
            'player_stats_success': 0,
            'player_stats_error': 0,
            'maps_processed': 0,
            'maps_success': 0,
            'maps_error': 0
        }
        
        # Load match details
        if not skip_match_details:
            match_details_files = glob.glob(os.path.join(MATCH_DETAILS_JSON_DIR, "*.json"))
            logger.info(f"Found {len(match_details_files)} files with match details")
            
            for file_path in match_details_files:
                try:
                    stats['match_details_processed'] += 1
                    self._load_match_details(file_path)
                    stats['match_details_success'] += 1
                    os.remove(file_path)
                    logger.info(f"File {os.path.basename(file_path)} deleted after processing")
                except Exception as e:
                    logger.error(f"Error loading match details from {file_path}: {str(e)}")
                    stats['match_details_error'] += 1
        
        # Load player statistics
        if not skip_player_stats:
            player_stats_files = glob.glob(os.path.join(PLAYER_STATS_JSON_DIR, "*.json"))
            logger.info(f"Found {len(player_stats_files)} files with player statistics")
            
            for file_path in player_stats_files:
                try:
                    stats['player_stats_processed'] += 1
                    self._load_player_stats(file_path)
                    stats['player_stats_success'] += 1
                    os.remove(file_path)
                    logger.info(f"File {os.path.basename(file_path)} deleted after processing")
                except Exception as e:
                    logger.error(f"Error loading player statistics from {file_path}: {str(e)}")
                    stats['player_stats_error'] += 1
        
        # Load match maps
        maps_files = glob.glob(os.path.join(RESULT_MAPS_JSON_DIR, "*.json"))
        logger.info(f"Found {len(maps_files)} files with match maps")
        for file_path in maps_files:
            try:
                stats['maps_processed'] += 1
                self._load_match_maps(file_path)
                stats['maps_success'] += 1
                os.remove(file_path)
                logger.info(f"File {os.path.basename(file_path)} deleted after processing (maps)")
            except Exception as e:
                stats['maps_error'] += 1
                logger.error(f"Error loading match maps from {file_path}: {str(e)}")
        
        return stats
    
    def _create_tables(self):
        """Creates necessary tables in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create result_match table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS result_match (
                    match_id INTEGER PRIMARY KEY,
                    url TEXT,
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
                    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create player_stats table if it doesn't exist
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
                    FOREIGN KEY (match_id) REFERENCES result_match (match_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Tables successfully created/verified")
            
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
    
    def _load_match_details(self, file_path):
        """
        Loads match details from a JSON file to the database
        
        Args:
            file_path (str): Path to JSON file
        """
        try:
            # Read file
            with open(file_path, 'r', encoding='utf-8') as f:
                match_data = json.load(f)
            
            # Extract match ID from file name
            file_name = os.path.basename(file_path)
            match_id = int(os.path.splitext(file_name)[0])
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if match details already exist
            cursor.execute('SELECT match_id FROM result_match WHERE match_id = ?', (match_id,))
            exists = cursor.fetchone()
            
            if exists:
                # Update existing record
                cursor.execute('''
                    UPDATE result_match SET
                        url = ?,
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
                        parsed_at = ?
                    WHERE match_id = ?
                ''', (
                    match_data.get('url', ''),
                    match_data.get('datetime', 0),
                    match_data.get('team1_id', 0),
                    match_data.get('team1_name', ''),
                    match_data.get('team1_score', 0),
                    match_data.get('team1_rank', 0),
                    match_data.get('team2_id', 0),
                    match_data.get('team2_name', ''),
                    match_data.get('team2_score', 0),
                    match_data.get('team2_rank', 0),
                    match_data.get('event_id', 0),
                    match_data.get('event_name', ''),
                    match_data.get('demo_id', 0),
                    match_data.get('head_to_head_team1_wins', 0),
                    match_data.get('head_to_head_team2_wins', 0),
                    match_data.get('parsed_at', datetime.now().isoformat()),
                    match_id
                ))
                logger.info(f"Updated match details for ID {match_id}")
            else:
                # Insert new record
                cursor.execute('''
                    INSERT INTO result_match (
                        match_id, url, datetime, 
                        team1_id, team1_name, team1_score, team1_rank,
                        team2_id, team2_name, team2_score, team2_rank,
                        event_id, event_name, demo_id,
                        head_to_head_team1_wins, head_to_head_team2_wins,
                        parsed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_id,
                    match_data.get('url', ''),
                    match_data.get('datetime', 0),
                    match_data.get('team1_id', 0),
                    match_data.get('team1_name', ''),
                    match_data.get('team1_score', 0),
                    match_data.get('team1_rank', 0),
                    match_data.get('team2_id', 0),
                    match_data.get('team2_name', ''),
                    match_data.get('team2_score', 0),
                    match_data.get('team2_rank', 0),
                    match_data.get('event_id', 0),
                    match_data.get('event_name', ''),
                    match_data.get('demo_id', 0),
                    match_data.get('head_to_head_team1_wins', 0),
                    match_data.get('head_to_head_team2_wins', 0),
                    match_data.get('parsed_at', datetime.now().isoformat())
                ))
                logger.info(f"Inserted match details for ID {match_id}")
            
            conn.commit()
            conn.close()

            # --- Новый блок: загрузка сыгранных карт ---
            if 'maps' in match_data and match_data['maps']:
                try:
                    conn2 = sqlite3.connect(self.db_path)
                    cursor2 = conn2.cursor()
                    cursor2.execute('DELETE FROM result_match_maps WHERE match_id = ?', (match_id,))
                    for m in match_data['maps']:
                        cursor2.execute(
                            '''
                            INSERT INTO result_match_maps (match_id, map_name, team1_rounds, team2_rounds, rounds)
                            VALUES (?, ?, ?, ?, ?)
                            ''',
                            (
                                match_id,
                                m.get('map_name', ''),
                                m.get('team1_rounds', 0),
                                m.get('team2_rounds', 0),
                                m.get('rounds', '')
                            )
                        )
                    conn2.commit()
                    conn2.close()
                    logger.info(f"Loaded {len(match_data['maps'])} maps for match ID {match_id}")
                except Exception as e:
                    logger.error(f"Error loading maps for match {match_id}: {str(e)}")
            # --- Конец нового блока ---
            
        except Exception as e:
            logger.error(f"Error processing match details from {file_path}: {str(e)}")
            raise
    
    def _load_player_stats(self, file_path):
        """
        Loads player statistics from a JSON file to the database
        
        Args:
            file_path (str): Path to JSON file
        """
        try:
            # Read file
            with open(file_path, 'r', encoding='utf-8') as f:
                stats_data = json.load(f)
            
            # Изменение: Проверяем формат файла и извлекаем match_id по-разному
            # в зависимости от структуры
            if 'match_id' in stats_data:
                # Старый формат
                match_id = stats_data['match_id']
                team_players = stats_data['teams']
                
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Delete existing stats for this match if any
                cursor.execute('DELETE FROM player_stats WHERE match_id = ?', (match_id,))
                
                # Process each team
                for team_data in team_players:
                    team_id = team_data['team_id']
                    
                    # Process each player
                    for player_data in team_data['players']:
                        cursor.execute('''
                            INSERT INTO player_stats (
                                match_id, team_id, player_id, player_nickname,
                                fullName, nickName, kills, deaths, kd_ratio,
                                plus_minus, adr, kast, rating
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            match_id,
                            team_id,
                            player_data.get('player_id', 0),
                            player_data.get('player_nickname', ''),
                            player_data.get('fullName', ''),
                            player_data.get('nickName', ''),
                            player_data.get('kills', 0),
                            player_data.get('deaths', 0),
                            player_data.get('kd_ratio', 0.0),
                            player_data.get('plus_minus', 0),
                            player_data.get('adr', 0.0),
                            player_data.get('kast', 0.0),
                            player_data.get('rating', 0.0)
                        ))
                
                conn.commit()
                conn.close()
                
                logger.info(f"Loaded player statistics for match ID {match_id}, teams: {len(team_players)}")
            
            elif 'players' in stats_data:
                # Новый формат - список игроков без группировки по командам
                # Получим match_id из имени файла или из первого игрока
                file_name = os.path.basename(file_path)
                try:
                    match_id = int(os.path.splitext(file_name)[0])
                except ValueError:
                    # Если не удалось получить из имени файла, берем из первого игрока
                    if stats_data['players']:
                        match_id = stats_data['players'][0]['match_id']
                    else:
                        raise ValueError("Cannot determine match_id from file or data")
                
                players = stats_data['players']
                
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Delete existing stats for this match if any
                cursor.execute('DELETE FROM player_stats WHERE match_id = ?', (match_id,))
                
                # Process each player directly
                for player_data in players:
                    # Проверяем, что match_id у игрока соответствует тому, что мы используем
                    player_match_id = player_data.get('match_id', match_id)
                    if player_match_id != match_id:
                        logger.warning(f"Player match_id {player_match_id} differs from file match_id {match_id}")
                        
                    cursor.execute('''
                        INSERT INTO player_stats (
                            match_id, team_id, player_id, player_nickname,
                            fullName, nickName, kills, deaths, kd_ratio,
                            plus_minus, adr, kast, rating
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        match_id,
                        player_data.get('team_id', 0),
                        player_data.get('player_id', 0),
                        player_data.get('player_nickname', ''),
                        player_data.get('fullName', ''),
                        player_data.get('nickName', ''),
                        player_data.get('kills', 0),
                        player_data.get('deaths', 0),
                        player_data.get('kd_ratio', 0.0),
                        player_data.get('plus_minus', 0),
                        player_data.get('adr', 0.0),
                        player_data.get('kast', 0.0),
                        player_data.get('rating', 0.0)
                    ))
                
                conn.commit()
                conn.close()
                
                logger.info(f"Loaded player statistics for match ID {match_id}, players: {len(players)}")
            
            else:
                raise ValueError("Unknown player stats format - neither 'match_id' nor 'players' found")
            
        except Exception as e:
            logger.error(f"Error processing player statistics from {file_path}: {str(e)}")
            raise

    def _load_match_maps(self, file_path):
        """
        Loads played maps for a match from a JSON file to the database
        Args:
            file_path (str): Path to JSON file
        """
        try:
            match_id = int(os.path.splitext(os.path.basename(file_path))[0])
            with open(file_path, "r", encoding="utf-8") as f:
                maps = json.load(f)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM result_match_maps WHERE match_id = ?", (match_id,))
            for m in maps:
                cursor.execute(
                    '''
                    INSERT INTO result_match_maps (match_id, map_name, team1_rounds, team2_rounds, rounds)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (
                        match_id,
                        m.get('map_name', ''),
                        m.get('team1_rounds', 0),
                        m.get('team2_rounds', 0),
                        m.get('rounds', '')
                    )
                )
            conn.commit()
            conn.close()
            logger.info(f"Loaded {len(maps)} maps for match ID {match_id}")
        except Exception as e:
            logger.error(f"Error loading maps for match {match_id}: {str(e)}")

if __name__ == "__main__":
    loader = MatchDetailsLoader()
    stats = loader.load_all()
    
    # Output statistics
    logger.info("======== Loading match details ========")
    logger.info(f"Processed files: {stats['match_details_processed']}")
    logger.info(f"Successfully loaded: {stats['match_details_success']}")
    logger.info(f"Errors: {stats['match_details_error']}")
    
    logger.info("======== Loading player statistics ========")
    logger.info(f"Processed files: {stats['player_stats_processed']}")
    logger.info(f"Successfully loaded: {stats['player_stats_success']}")
    logger.info(f"Errors: {stats['player_stats_error']}")
    
    logger.info("======== Загрузка информации о картах ========")
    logger.info(f"Обработано файлов: {stats['maps_processed']}")
    logger.info(f"Успешно загружено: {stats['maps_success']}")
    logger.info(f"Ошибок: {stats['maps_error']}") 