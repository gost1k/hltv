"""
Главный предиктор матчей CS2 на основе PyCaret
Автоматически подбирает лучшие модели и параметры
Поддерживает дообучение и визуализацию результатов
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
from pathlib import Path
from loguru import logger
import warnings
import pickle
from sklearn.calibration import CalibratedClassifierCV
warnings.filterwarnings('ignore')

# PyCaret imports
from pycaret.classification import *

# Настройка логирования
log_path = Path("logs/predictor")
log_path.mkdir(parents=True, exist_ok=True)
logger.add(log_path / "predictor_{time}.log", rotation="1 week")

# Пути
DB_PATH = 'hltv.db'
MODEL_PATH = 'models/pycaret'
DIAGNOSTICS_PATH = 'diagnostics/predictor'

class CS2MatchPredictor:
    """Автоматический предиктор матчей CS2 с PyCaret"""
    
    def __init__(self, db_path=DB_PATH, model_path=MODEL_PATH):
        self.db_path = db_path
        self.model_path = model_path
        self.best_model = None
        self.feature_importance = None
        self.setup_done = False
        
        # Создаем директории если не существуют
        Path(model_path).mkdir(parents=True, exist_ok=True)
        Path(DIAGNOSTICS_PATH).mkdir(parents=True, exist_ok=True)
        
        # Инициализируем список признаков
        self.feature_columns = [
            'rating_diff', 'kd_diff', 'firepower_diff',
            'winrate_diff', 'matches_played_diff',
            'team1_avg_rating', 'team2_avg_rating',
            'team1_avg_kd', 'team2_avg_kd',
            'team1_avg_adr', 'team2_avg_adr',
            'team1_avg_kast', 'team2_avg_kast',
            'team1_max_rating', 'team2_max_rating',
            'team1_min_rating', 'team2_min_rating',
            'team2_min_rating',
            'team1_rating_std', 'team2_rating_std',
            'team1_kd_std', 'team2_kd_std',
            'team1_adr_std', 'team2_adr_std',
            'team1_kast_std', 'team2_kast_std',
            'team1_carry_potential', 'team2_carry_potential',
            'team1_sniping_median', 'team2_sniping_median',
            'team1_majors_played_median', 'team1_majors_played_max', 'team1_majors_played_min',
            'team2_majors_played_min',
            'team1_age_median', 'team1_age_min', 'team2_age_min',
            'team1_firepower_max', 'team1_utility_min', 'team2_utility_min',
            'team1_avg_rating_2_1', 'team2_avg_rating_2_1',
            'team1_avg_firepower', 'team2_avg_firepower',
            'team1_avg_opening', 'team2_avg_opening',
            'team1_avg_clutching', 'team2_avg_clutching',
            'team1_avg_sniping', 'team2_avg_sniping',
            'team1_recent_winrate', 'team2_recent_winrate',
            'team1_avg_score_for', 'team1_avg_score_against',
            'team2_avg_score_for', 'team2_avg_score_against',
            'team1_matches_played', 'team2_matches_played',
            'team1_recent_sub', 'team2_recent_sub',
            'team1_close_map_rate', 'team2_close_map_rate',
            'team1_ot_map_rate', 'team2_ot_map_rate',
            'team1_comeback_rate', 'team2_comeback_rate',
            'rank_diff', 'rank_ratio', 'h2h_winrate_team1',
            'hour', 'weekday', 'log_rank_team1', 'log_rank_team2',
            'is_lan', 'recent_patch', 'stage_group', 'stage_playoff', 'stage_final',
            'team1_avg_score_against', 'team2_avg_score_against',
            'team2_matches_played', 'team2_carry_potential',
            'team2_avg_clutching', 'log_rank_team1',
        ]
        
        logger.info("Инициализация CS2MatchPredictor с PyCaret")
        
    def _get_connection(self):
        """Получение соединения с БД"""
        return sqlite3.connect(self.db_path)
    
    def _create_base_features(self, df):
        """Создание базовых признаков из сырых данных + новых (LAN, патч, стадия)"""
        logger.info("Создание базовых признаков...")
        
        # Проверяем наличие необходимых колонок
        required_columns = ['team1_rank', 'team2_rank', 'datetime', 
                          'head_to_head_team1_wins', 'head_to_head_team2_wins']
        
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"Колонка {col} отсутствует в данных")
                if col in ['head_to_head_team1_wins', 'head_to_head_team2_wins']:
                    df[col] = 0
                else:
                    raise ValueError(f"Отсутствует обязательная колонка: {col}")
        
        # Разница в рейтингах
        df['rank_diff'] = df['team1_rank'] - df['team2_rank']
        df['rank_ratio'] = df['team1_rank'] / (df['team2_rank'] + 1)  # +1 для избежания деления на 0
        
        # История встреч
        df['h2h_total'] = df['head_to_head_team1_wins'] + df['head_to_head_team2_wins']
        df['h2h_winrate_team1'] = np.where(
            df['h2h_total'] > 0,
            df['head_to_head_team1_wins'] / df['h2h_total'],
            0.5
        )
        
        # Время суток матча (может влиять на форму команд из разных часовых поясов)
        df['hour'] = pd.to_datetime(df['datetime'], unit='s').dt.hour
        df['weekday'] = pd.to_datetime(df['datetime'], unit='s').dt.weekday
        
        # Логарифмические преобразования рейтингов
        df['team1_rank'] = df['team1_rank'].fillna(-1)
        df['team2_rank'] = df['team2_rank'].fillna(-1)
        df['log_rank_team1'] = np.log1p(df['team1_rank'])
        df['log_rank_team2'] = np.log1p(df['team2_rank'])
        df['log_rank_team1'].replace([np.inf, -np.inf], 0, inplace=True)
        df['log_rank_team2'].replace([np.inf, -np.inf], 0, inplace=True)
        df['log_rank_team1'] = df['log_rank_team1'].fillna(0)
        df['log_rank_team2'] = df['log_rank_team2'].fillna(0)
        
        # LAN/online
        df['is_lan'] = df['event_name'].str.contains('LAN|LAN Finals|LAN Event|LAN-', case=False, na=False).astype(int)
        # Признак патча (пример: вручную по дате, можно расширить)
        PATCH_DATES = [1700000000, 1710000000]  # unix timestamps крупных патчей, заполнить актуальными
        df['recent_patch'] = df['datetime'].apply(
            lambda x: int(any(abs(int(x) - int(p)) < 7*24*3600 for p in PATCH_DATES))
        )
        # Стадия турнира (по event_name)
        df['stage_group'] = df['event_name'].str.contains('Group', case=False, na=False).astype(int)
        df['stage_playoff'] = df['event_name'].str.contains('Playoff|Bracket', case=False, na=False).astype(int)
        df['stage_final'] = df['event_name'].str.contains('Final', case=False, na=False).astype(int)
        
        return df
    
    def _create_player_features(self, df):
        """Создание признаков на основе статистики игроков"""
        logger.info("Загрузка и агрегация статистики игроков...")
        
        # Список всех возможных признаков
        player_features = [
            'team1_avg_rating', 'team1_avg_kd', 'team1_avg_adr', 'team1_avg_kast',
            'team1_max_rating', 'team1_min_rating', 'team2_avg_rating', 'team2_avg_kd',
            'team2_avg_adr', 'team2_avg_kast', 'team2_max_rating', 'team2_min_rating',
            'team1_avg_rating_2_1', 'team1_avg_firepower', 'team1_avg_opening',
            'team1_avg_clutching', 'team1_avg_sniping', 'team2_avg_rating_2_1',
            'team2_avg_firepower', 'team2_avg_opening', 'team2_avg_clutching',
            'team2_avg_sniping',
            # Новые фичи:
            'team1_rating_std', 'team2_rating_std',
            'team1_kd_std', 'team2_kd_std',
            'team1_adr_std', 'team2_adr_std',
            'team1_kast_std', 'team2_kast_std',
            'team1_carry_potential', 'team2_carry_potential'
        ]
        
        # Инициализируем все признаки нулями
        for feature in player_features:
            df[feature] = 0.0
        
        with self._get_connection() as conn:
            # Получаем статистику игроков для каждого матча и команды
            player_stats_full = pd.read_sql_query("SELECT match_id, team_id, rating, kd_ratio, adr, kast FROM player_stats", conn)
            
            # Получаем статистику игроков для каждого матча
            player_stats_query = """
            SELECT 
                ps.match_id,
                ps.team_id,
                AVG(ps.rating) as avg_rating,
                AVG(ps.kd_ratio) as avg_kd,
                AVG(ps.adr) as avg_adr,
                AVG(ps.kast) as avg_kast,
                MAX(ps.rating) as max_rating,
                MIN(ps.rating) as min_rating
            FROM player_stats ps
            GROUP BY ps.match_id, ps.team_id
            """
            player_stats = pd.read_sql_query(player_stats_query, conn)
            
            # Получаем дополнительную информацию об игроках (без агрегатов)
            players_query = """
            SELECT 
                ps.match_id,
                ps.team_id,
                p.rating_2_1,
                p.firepower,
                p.opening,
                p.clutching,
                p.sniping,
                p.majors_played,
                p.age,
                p.utility
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            """
            players_info_full = pd.read_sql_query(players_query, conn)
        
        # Для каждого матча и команды считаем агрегаты по pandas
        def get_player_agg(match_id, team_id, col, func):
            vals = players_info_full[(players_info_full['match_id'] == match_id) & (players_info_full['team_id'] == team_id)][col].dropna()
            if vals.empty:
                return 0.0
            if func == 'median':
                return float(np.median(vals))
            elif func == 'min':
                return float(np.min(vals))
            elif func == 'max':
                return float(np.max(vals))
            elif func == 'avg':
                return float(np.mean(vals))
            else:
                return 0.0
        
        # Обрабатываем для каждого матча отдельно
        result_dfs = []
        for match_id in df['match_id'].unique():
            match_df = df[df['match_id'] == match_id].copy()
            
            # Статистика для команды 1
            team1_id = match_df['team1_id'].iloc[0]
            team1_stats = player_stats[(player_stats['match_id'] == match_id) & (player_stats['team_id'] == team1_id)]
            team1_players = players_info_full[(players_info_full['match_id'] == match_id) & (players_info_full['team_id'] == team1_id)]
            
            if not team1_stats.empty:
                for col in ['avg_rating', 'avg_kd', 'avg_adr', 'avg_kast', 'max_rating', 'min_rating']:
                    match_df[f'team1_{col}'] = team1_stats[col].iloc[0] if col in team1_stats.columns else 0.0
            
            if not team1_players.empty:
                for col in ['rating_2_1', 'firepower', 'opening', 'clutching', 'sniping',
                            'majors_played', 'age', 'utility']:
                    match_df[f'team1_{col}'] = get_player_agg(match_id, team1_id, col, 'avg')
            
            # Статистика для команды 2
            team2_id = match_df['team2_id'].iloc[0]
            team2_stats = player_stats[(player_stats['match_id'] == match_id) & (player_stats['team_id'] == team2_id)]
            team2_players = players_info_full[(players_info_full['match_id'] == match_id) & (players_info_full['team_id'] == team2_id)]
            
            if not team2_stats.empty:
                for col in ['avg_rating', 'avg_kd', 'avg_adr', 'avg_kast', 'max_rating', 'min_rating']:
                    match_df[f'team2_{col}'] = team2_stats[col].iloc[0] if col in team2_stats.columns else 0.0
            
            if not team2_players.empty:
                for col in ['rating_2_1', 'firepower', 'opening', 'clutching', 'sniping',
                            'majors_played', 'age', 'utility']:
                    match_df[f'team2_{col}'] = get_player_agg(match_id, team2_id, col, 'avg')
            
            # --- STD и carry для team1 ---
            t1_stats = player_stats_full[(player_stats_full['match_id'] == match_id) & (player_stats_full['team_id'] == team1_id)]
            if not t1_stats.empty:
                match_df['team1_rating_std'] = t1_stats['rating'].std(ddof=0) if 'rating' in t1_stats else 0.0
                match_df['team1_kd_std'] = t1_stats['kd_ratio'].std(ddof=0) if 'kd_ratio' in t1_stats else 0.0
                match_df['team1_adr_std'] = t1_stats['adr'].std(ddof=0) if 'adr' in t1_stats else 0.0
                match_df['team1_kast_std'] = t1_stats['kast'].std(ddof=0) if 'kast' in t1_stats else 0.0
                if 'rating' in t1_stats:
                    match_df['team1_carry_potential'] = t1_stats['rating'].max() - t1_stats['rating'].median()
                else:
                    match_df['team1_carry_potential'] = 0.0
            else:
                match_df['team1_rating_std'] = 0.0
                match_df['team1_kd_std'] = 0.0
                match_df['team1_adr_std'] = 0.0
                match_df['team1_kast_std'] = 0.0
                match_df['team1_carry_potential'] = 0.0
            # --- STD и carry для team2 ---
            t2_stats = player_stats_full[(player_stats_full['match_id'] == match_id) & (player_stats_full['team_id'] == team2_id)]
            if not t2_stats.empty:
                match_df['team2_rating_std'] = t2_stats['rating'].std(ddof=0) if 'rating' in t2_stats else 0.0
                match_df['team2_kd_std'] = t2_stats['kd_ratio'].std(ddof=0) if 'kd_ratio' in t2_stats else 0.0
                match_df['team2_adr_std'] = t2_stats['adr'].std(ddof=0) if 'adr' in t2_stats else 0.0
                match_df['team2_kast_std'] = t2_stats['kast'].std(ddof=0) if 'kast' in t2_stats else 0.0
                if 'rating' in t2_stats:
                    match_df['team2_carry_potential'] = t2_stats['rating'].max() - t2_stats['rating'].median()
                else:
                    match_df['team2_carry_potential'] = 0.0
            else:
                match_df['team2_rating_std'] = 0.0
                match_df['team2_kd_std'] = 0.0
                match_df['team2_adr_std'] = 0.0
                match_df['team2_kast_std'] = 0.0
                match_df['team2_carry_potential'] = 0.0
            
            # Новые признаки для team1
            match_df['team1_sniping_median'] = get_player_agg(match_id, team1_id, 'sniping', 'median')
            match_df['team1_majors_played_median'] = get_player_agg(match_id, team1_id, 'majors_played', 'median')
            match_df['team1_majors_played_max'] = get_player_agg(match_id, team1_id, 'majors_played', 'max')
            match_df['team1_age_median'] = get_player_agg(match_id, team1_id, 'age', 'median')
            match_df['team1_age_min'] = get_player_agg(match_id, team1_id, 'age', 'min')
            match_df['team1_firepower_max'] = get_player_agg(match_id, team1_id, 'firepower', 'max')
            match_df['team1_utility_min'] = get_player_agg(match_id, team1_id, 'utility', 'min')
            # Новые признаки для team2
            match_df['team2_sniping_median'] = get_player_agg(match_id, team2_id, 'sniping', 'median')
            match_df['team2_majors_played_min'] = get_player_agg(match_id, team2_id, 'majors_played', 'min')
            match_df['team2_age_min'] = get_player_agg(match_id, team2_id, 'age', 'min')
            
            result_dfs.append(match_df)
        
        df = pd.concat(result_dfs, ignore_index=True)
        
        # Создаем относительные признаки
        df['rating_diff'] = df['team1_avg_rating'] - df['team2_avg_rating']
        df['kd_diff'] = df['team1_avg_kd'] - df['team2_avg_kd']
        df['firepower_diff'] = df['team1_avg_firepower'] - df['team2_avg_firepower']
        
        self.feature_columns += [
            'team1_majors_played_max',
            'team1_majors_played_min',
            'team2_majors_played_min',
            'team1_firepower_max',
            'team1_utility_min',
            'team2_sniping_median'
        ]
        
        return df
    
    def _create_recent_form_features(self, df):
        """Создание признаков формы команд за последние матчи + замены"""
        logger.info("Вычисление формы команд и замен...")
        
        with self._get_connection() as conn:
            # Получаем результаты последних матчей для каждой команды
            recent_matches_query = """
            WITH team_matches AS (
                SELECT 
                    team1_id as team_id,
                    datetime,
                    team1_score as score_for,
                    team2_score as score_against,
                    CASE WHEN team1_score > team2_score THEN 1 ELSE 0 END as won
                FROM result_match
                WHERE team1_score IS NOT NULL AND team2_score IS NOT NULL
                UNION ALL
                SELECT 
                    team2_id as team_id,
                    datetime,
                    team2_score as score_for,
                    team1_score as score_against,
                    CASE WHEN team2_score > team1_score THEN 1 ELSE 0 END as won
                FROM result_match
                WHERE team1_score IS NOT NULL AND team2_score IS NOT NULL
            )
            SELECT 
                team_id,
                AVG(won) as recent_winrate,
                AVG(score_for) as avg_score_for,
                AVG(score_against) as avg_score_against,
                COUNT(*) as matches_played
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY datetime DESC) as rn
                FROM team_matches
            ) t
            WHERE rn <= 10  -- Последние 10 матчей
            GROUP BY team_id
            """
            recent_form = pd.read_sql_query(recent_matches_query, conn)
            recent_form['team_id'] = pd.to_numeric(recent_form['team_id'], errors='coerce').astype('Int64')
        
        # Создаём датафреймы с префиксами для обеих команд
        recent_form_team1 = recent_form.add_prefix('team1_')
        recent_form_team2 = recent_form.add_prefix('team2_')
        
        # Приводим id к одному типу
        df['team1_id'] = pd.to_numeric(df['team1_id'], errors='coerce').astype('Int64')
        df['team2_id'] = pd.to_numeric(df['team2_id'], errors='coerce').astype('Int64')
        recent_form_team1['team1_team_id'] = pd.to_numeric(recent_form_team1['team1_team_id'], errors='coerce').astype('Int64')
        recent_form_team2['team2_team_id'] = pd.to_numeric(recent_form_team2['team2_team_id'], errors='coerce').astype('Int64')
        
        # Добавляем форму для каждой команды
        df = df.merge(
            recent_form_team1,
            left_on='team1_id',
            right_on='team1_team_id',
            how='left'
        )
        df = df.merge(
            recent_form_team2,
            left_on='team2_id',
            right_on='team2_team_id',
            how='left'
        )
        
        # Относительные показатели формы
        if 'team1_recent_winrate' in df.columns and 'team2_recent_winrate' in df.columns:
            df['winrate_diff'] = df['team1_recent_winrate'] - df['team2_recent_winrate']
            df['matches_played_diff'] = df['team1_matches_played'] - df['team2_matches_played']
        
        # Заполняем пропуски средними значениями
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())
        
        # Признак замены: если за последние 3 матча состав менялся
        def recent_substitution(team_id, current_match_time, conn, n=3):
            q = f"""
            SELECT ps.player_id, ps.match_id, rm.datetime
            FROM player_stats ps
            JOIN result_match rm ON ps.match_id = rm.match_id
            WHERE (rm.team1_id = ? OR rm.team2_id = ?) AND rm.datetime < ?
            ORDER BY rm.datetime DESC LIMIT {n}
            """
            df_sub = pd.read_sql_query(q, conn, params=[team_id, team_id, current_match_time])
            if df_sub.empty:
                return 0
            players_sets = []
            for match_id in df_sub['match_id'].unique():
                players = set(df_sub[df_sub['match_id'] == match_id]['player_id'])
                players_sets.append(players)
            # Если составы отличаются между матчами — была замена
            for i in range(1, len(players_sets)):
                if players_sets[i] != players_sets[i-1]:
                    return 1
            return 0
        with self._get_connection() as conn:
            df['team1_recent_sub'] = df.apply(lambda row: recent_substitution(row['team1_id'], row['datetime'], conn), axis=1)
            df['team2_recent_sub'] = df.apply(lambda row: recent_substitution(row['team2_id'], row['datetime'], conn), axis=1)
        
        return df
    
    def _create_map_and_clutch_features(self, df):
        # Для каждой команды: доля close-карт, доля овертаймов, доля камбеков
        # close = счет 13-10, 10-13, 13-11, 11-13
        # OT = обе команды >12 раундов (>=13-13)
        # comeback: разрыв в первой половине >=6 раундов, но проигравшая половину команда выиграла карту или ушла в OT
        with self._get_connection() as conn:
            maps = pd.read_sql_query("SELECT match_id, map_name, team1_rounds, team2_rounds, half1_team1, half1_team2, half2_team1, half2_team2, ot_rounds_team1, ot_rounds_team2 FROM result_match_maps", conn)
        def calc_features(match_id, team1_id, team2_id):
            maps_match = maps[maps['match_id'] == match_id]
            t1_close, t2_close = 0, 0
            t1_ot, t2_ot = 0, 0
            t1_cb, t2_cb = 0, 0
            total = len(maps_match)
            for _, row in maps_match.iterrows():
                s1, s2 = row['team1_rounds'], row['team2_rounds']
                # close: разница 2 или 3 и одна из команд набрала 13
                if (s1 == 13 and s2 in [10,11]) or (s2 == 13 and s1 in [10,11]):
                    t1_close += int(s1 == 13)
                    t2_close += int(s2 == 13)
                # OT: если ot_rounds_team1+ot_rounds_team2 > 0
                ot1 = row.get('ot_rounds_team1', 0) or 0
                ot2 = row.get('ot_rounds_team2', 0) or 0
                if ot1 + ot2 > 0:
                    t1_ot += 1
                    t2_ot += 1
                # comeback: проиграл первую половину >=6, но выиграл карту или ушел в OT
                h1t1 = row.get('half1_team1')
                h1t2 = row.get('half1_team2')
                if h1t1 is not None and h1t2 is not None:
                    diff1 = h1t1 - h1t2
                    diff2 = h1t2 - h1t1
                    # t1 comeback
                    if diff1 <= -6 and (s1 > s2 or (ot1 + ot2 > 0)):
                        t1_cb += 1
                    # t2 comeback
                    if diff2 <= -6 and (s2 > s1 or (ot1 + ot2 > 0)):
                        t2_cb += 1
            if total == 0:
                return 0,0,0,0,0,0
            return t1_close/total, t2_close/total, t1_ot/total, t2_ot/total, t1_cb/total, t2_cb/total
        df['team1_close_map_rate'] = 0.0
        df['team2_close_map_rate'] = 0.0
        df['team1_ot_map_rate'] = 0.0
        df['team2_ot_map_rate'] = 0.0
        df['team1_comeback_rate'] = 0.0
        df['team2_comeback_rate'] = 0.0
        for idx, row in df.iterrows():
            t1c, t2c, t1ot, t2ot, t1cb, t2cb = calc_features(row['match_id'], row.get('team1_id'), row.get('team2_id'))
            df.at[idx, 'team1_close_map_rate'] = t1c
            df.at[idx, 'team2_close_map_rate'] = t2c
            df.at[idx, 'team1_ot_map_rate'] = t1ot
            df.at[idx, 'team2_ot_map_rate'] = t2ot
            df.at[idx, 'team1_comeback_rate'] = t1cb
            df.at[idx, 'team2_comeback_rate'] = t2cb
        return df
    
    def prepare_training_data(self, start_date=None, end_date=None):
        """Подготовка данных для обучения"""
        logger.info("Подготовка данных для обучения...")
        
        with self._get_connection() as conn:
            # Базовый запрос для получения матчей
            query = """
            SELECT 
                rm.*,
                CASE WHEN rm.team1_score > rm.team2_score THEN 1 ELSE 0 END as team1_won
            FROM result_match rm
            WHERE rm.team1_score IS NOT NULL 
                AND rm.team2_score IS NOT NULL
            """
            
            # Добавляем фильтры по датам если указаны
            params = []
            if start_date:
                query += " AND rm.datetime >= ?"
                params.append(int(start_date.timestamp()))
            if end_date:
                query += " AND rm.datetime <= ?"
                params.append(int(end_date.timestamp()))
                
            query += " ORDER BY rm.datetime"
            
            df = pd.read_sql_query(query, conn, params=params)
        
        if df.empty:
            logger.error("Нет данных для обучения!")
            return None
        
        logger.info(f"Загружено {len(df)} матчей для обучения")
        
        # Создаем все признаки
        df = self._create_base_features(df)
        df = self._create_player_features(df)
        df = self._create_recent_form_features(df)
        df = self._create_map_and_clutch_features(df)
        
        # Сохраняем список признаков
        feature_cols = [col for col in df.columns if col not in [
            'match_id', 'url', 'team1_won', 'team1_score', 'team2_score',
            'team1_name', 'team2_name', 'event_name', 'parsed_at',
            'team1_team_id', 'team2_team_id',
            'datetime', 'team1_id', 'team1_rank', 'team2_id', 'team2_rank', 'event_id', 'demo_id', 'head_to_head_team1_wins', 'head_to_head_team2_wins'
        ]]
        
        self.feature_columns = feature_cols
        logger.info(f"Создано {len(feature_cols)} признаков")
        
        return df
    
    def train(self, time_limit=60, n_models=15):
        """
        Обучение модели с PyCaret
        
        Args:
            time_limit: Время на обучение каждой модели в минутах (по умолчанию 60)
            n_models: Количество моделей для сравнения
        """
        logger.info(f"Начало обучения модели (time_limit={time_limit} мин на модель)")
        
        # Подготавливаем данные
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        df = self.prepare_training_data(start_date, end_date)
        if df is None:
            return False
        
        # Разделяем на обучающую и калибровочную выборки по времени
        df = df.sort_values('datetime')
        split_idx = int(len(df) * 0.8)
        train_df = df.iloc[:split_idx].copy()
        calib_df = df.iloc[split_idx:].copy()
        
        logger.info(f"Обучающая выборка: {len(train_df)} матчей")
        logger.info(f"Калибровочная выборка: {len(calib_df)} матчей")
        
        # Настройка PyCaret
        logger.info("Инициализация PyCaret...")
        
        # Выбираем только нужные колонки
        train_data = train_df[self.feature_columns + ['team1_won']]
        test_data = calib_df[self.feature_columns + ['team1_won']]
        
        # Настройка эксперимента
        clf1 = setup(
            train_data, 
            target='team1_won',
            test_data=test_data,
            session_id=123,
            train_size=0.8,
            use_gpu=False,
            verbose=False,
            feature_selection=True,
            remove_multicollinearity=True,
            multicollinearity_threshold=0.9
        )
        
        self.setup_done = True
        
        # Сравнение моделей
        logger.info(f"Сравнение {n_models} моделей...")
        best_model = compare_models(
            include=['lr', 'rf', 'xgboost', 'lightgbm', 'catboost', 
                    'et', 'ada', 'gbc', 'lda', 'nb', 'dt', 'svm', 'ridge'],
            fold=5,
            round=4,
            sort='AUC',
            n_select=1,
            budget_time=time_limit  # Время в минутах на модель
        )
        
        # Обучаем финальную модель на train_df
        logger.info("Обучение финальной модели...")
        self.best_model = finalize_model(best_model)
        
        # Сохраняем модель
        save_model(self.best_model, f'{self.model_path}/cs2_predictor')
        
        # Калибровка вероятностей
        X_calib = calib_df[self.feature_columns]
        y_calib = calib_df['team1_won']
        try:
            calibrator = CalibratedClassifierCV(estimator=self.best_model, method='sigmoid', cv='prefit')
            calibrator.fit(X_calib, y_calib)
            self.calibrator = calibrator
            with open(f'{self.model_path}/calibrator.pkl', 'wb') as f:
                pickle.dump(calibrator, f)
            # Сохраняем вероятности до/после калибровки для анализа
            orig_probs = self.best_model.predict_proba(X_calib)[:,1] if hasattr(self.best_model, 'predict_proba') else self.best_model.predict(X_calib)
            cal_probs = calibrator.predict_proba(X_calib)[:,1]
            pd.DataFrame({'orig': orig_probs, 'calibrated': cal_probs, 'y': y_calib}).to_csv(f'{DIAGNOSTICS_PATH}/calib_probs.csv', index=False)
            # Brier score
            from sklearn.metrics import brier_score_loss
            brier_orig = brier_score_loss(y_calib, orig_probs)
            brier_cal = brier_score_loss(y_calib, cal_probs)
            with open(f'{DIAGNOSTICS_PATH}/calibration_metrics.txt', 'w') as f:
                f.write(f'Brier до калибровки: {brier_orig}\nBrier после калибровки: {brier_cal}\n')
            logger.info(f'Brier до калибровки: {brier_orig:.4f}, после: {brier_cal:.4f}')
        except Exception as e:
            logger.warning(f'Ошибка калибровки: {e}')
            self.calibrator = None
        
        # Получаем важность признаков
        try:
            feature_imp = pd.DataFrame({
                'feature': get_config('X_train').columns,
                'importance': self.best_model.feature_importances_ if hasattr(self.best_model, 'feature_importances_') else [1] * len(get_config('X_train').columns)
            }).sort_values('importance', ascending=False)
            
            self.feature_importance = feature_imp
            self._save_feature_importance()
        except:
            logger.warning("Не удалось получить важность признаков для данной модели")
        
        # Оцениваем качество
        self._evaluate_model(calib_df)
        
        logger.info("Обучение завершено успешно!")
        self.predict_upcoming_matches()
        return True
    
    def _evaluate_model(self, test_df):
        """Оценка качества модели"""
        logger.info("Оценка качества модели...")
        
        if not self.setup_done:
            logger.error("Setup не выполнен!")
            return
        
        # Предсказание на тестовых данных
        test_data = test_df[self.feature_columns + ['team1_won']]
        predictions = predict_model(self.best_model, data=test_data)
        
        # Метрики
        from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
        
        accuracy = accuracy_score(predictions['team1_won'], predictions['prediction_label'])
        try:
            auc = roc_auc_score(predictions['team1_won'], predictions['prediction_score'])
        except:
            auc = 0.5
        
        metrics = {
            'accuracy': accuracy,
            'auc': auc,
            'model_type': str(type(self.best_model).__name__)
        }
        
        logger.info(f"Точность: {accuracy:.4f}")
        logger.info(f"AUC: {auc:.4f}")
        
        # Сохраняем результаты
        with open(f"{DIAGNOSTICS_PATH}/evaluation_metrics.json", 'w') as f:
            json.dump(metrics, f, indent=2)
        
        # Сохраняем отчет о моделях
        try:
            models_df = pull()
            models_df.to_csv(f"{DIAGNOSTICS_PATH}/model_leaderboard.csv", index=False)
        except:
            logger.warning("Не удалось сохранить leaderboard моделей")
    
    def _save_feature_importance(self):
        """Сохранение важности признаков"""
        if self.feature_importance is not None:
            # Сохраняем в CSV для анализа
            self.feature_importance.to_csv(
                f"{DIAGNOSTICS_PATH}/feature_importance.csv",
                index=False
            )
            
            # Выводим топ-20 важных признаков
            logger.info("Топ-20 важных признаков:")
            logger.info(f"\n{self.feature_importance.head(20)}")
    
    def load_calibrator(self):
        try:
            with open(f'{self.model_path}/calibrator.pkl', 'rb') as f:
                self.calibrator = pickle.load(f)
        except Exception as e:
            self.calibrator = None
            logger.warning(f'Не удалось загрузить calibrator: {e}')
    
    def predict_match(self, match_id):
        """Предсказание результата конкретного матча"""
        if self.best_model is None:
            logger.error("Модель не обучена!")
            return None
        
        # Получаем данные матча
        with self._get_connection() as conn:
            query = """
            SELECT 
                um.match_id,
                um.datetime,
                um.team1_id,
                um.team1_rank,
                um.team2_id,
                um.team2_rank,
                um.event_id,
                um.event_name,
                COALESCE(h2h.team1_wins, 0) as head_to_head_team1_wins,
                COALESCE(h2h.team2_wins, 0) as head_to_head_team2_wins
            FROM upcoming_match um
            LEFT JOIN (
                SELECT 
                    team1_id,
                    team2_id,
                    COUNT(CASE WHEN team1_score > team2_score THEN 1 END) as team1_wins,
                    COUNT(CASE WHEN team2_score > team1_score THEN 1 END) as team2_wins
                FROM result_match
                WHERE team1_score IS NOT NULL AND team2_score IS NOT NULL
                GROUP BY team1_id, team2_id
            ) h2h ON (um.team1_id = h2h.team1_id AND um.team2_id = h2h.team2_id)
                OR (um.team1_id = h2h.team2_id AND um.team2_id = h2h.team1_id)
            WHERE um.match_id = ?
            """
            df = pd.read_sql_query(query, conn, params=[match_id])
            
            # Логируем полученные данные
            logger.info(f"Получены данные для матча {match_id}:")
            logger.info(f"Колонки в DataFrame: {df.columns.tolist()}")
            logger.info(f"Первая строка данных: {df.iloc[0].to_dict() if not df.empty else 'Нет данных'}")
        
        if df.empty:
            logger.error(f"Матч {match_id} не найден")
            return None
            
        # Проверяем наличие всех необходимых колонок
        required_columns = ['match_id', 'datetime', 'team1_id', 'team1_rank', 
                          'team2_id', 'team2_rank', 'event_id', 'event_name',
                          'head_to_head_team1_wins', 'head_to_head_team2_wins']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Отсутствуют необходимые колонки: {missing_columns}")
            return None
        
        # Проверяем, что team1_id и team2_id определены
        team1_id = df['team1_id'].iloc[0] if 'team1_id' in df.columns and not df.empty else None
        team2_id = df['team2_id'].iloc[0] if 'team2_id' in df.columns and not df.empty else None
        if pd.isnull(team1_id) or pd.isnull(team2_id):
            logger.error(f"match_id={match_id}: team1_id или team2_id не определен, пропуск предсказания")
            return None
        
        # Создаем признаки
        try:
            # Создаем копию исходных данных для создания признаков
            features_df = df.copy()
            
            # Создаем все необходимые признаки
            features_df = self._create_base_features(features_df)
            logger.info("Базовые признаки созданы успешно")
            logger.info(f"Колонки после базовых признаков: {features_df.columns.tolist()}")
            features_df = self._create_player_features(features_df)
            logger.info("Признаки игроков созданы успешно")
            logger.info(f"Колонки после признаков игроков: {features_df.columns.tolist()}")
            features_df = self._create_recent_form_features(features_df)
            logger.info("Признаки формы созданы успешно")
            logger.info(f"Колонки после признаков формы: {features_df.columns.tolist()}")
            features_df = self._create_map_and_clutch_features(features_df)
            logger.info("Фичи по картам и клатчам созданы успешно")
            logger.info(f"Колонки после фичей по картам: {features_df.columns.tolist()}")
            
            # Получаем список признаков, которые нужно использовать для предсказания
            prediction_features = self.feature_columns
            
            # Проверяем наличие всех необходимых признаков для предсказания
            missing_features = [col for col in prediction_features if col not in features_df.columns]
            if missing_features:
                logger.error(f"Отсутствуют необходимые признаки для предсказания: {missing_features}")
                return None
            
            # Предсказываем
            match_data = features_df[self.feature_columns]
            if hasattr(self, 'calibrator') and self.calibrator is not None:
                probabilities = self.calibrator.predict_proba(match_data)
                prob_team1_win = probabilities[0][1]
            elif hasattr(self.best_model, 'predict_proba'):
                probabilities = self.best_model.predict_proba(match_data)
                prob_team1_win = probabilities[0][1]
            else:
                prediction = self.best_model.predict(match_data)
                prob_team1_win = float(prediction[0])
                
        except Exception as e:
            logger.error(f"Ошибка при создании признаков или предсказании: {str(e)}")
            logger.error(f"Текущие колонки в DataFrame: {features_df.columns.tolist()}")
            return None
            
        prob_team2_win = 1 - prob_team1_win
        
        # Получаем предсказанный счёт
        if prob_team1_win > 0.7:
            score_team1, score_team2 = 2, 0
        elif prob_team1_win > 0.55:
            score_team1, score_team2 = 2, 1
        elif prob_team1_win < 0.3:
            score_team1, score_team2 = 0, 2
        elif prob_team1_win < 0.45:
            score_team1, score_team2 = 1, 2
        else:
            if prob_team1_win > 0.5:
                score_team1, score_team2 = 2, 1
            else:
                score_team1, score_team2 = 1, 2
        
        # Вычисляем confidence
        confidence = abs(prob_team1_win - 0.5) * 2
        
        result = {
            'match_id': int(match_id),
            'team1_score': float(prob_team1_win),
            'team2_score': float(prob_team2_win),
            'team1_score_final': int(score_team1),
            'team2_score_final': int(score_team2),
            'confidence': float(confidence),
            'model_version': 'pycaret_v1',
            'prediction_time': datetime.now()
        }
        
        logger.info(f"Предсказание для матча {match_id}: {result}")
        return result
    
    def predict_upcoming_matches(self, force_update_all=False):
        """Предсказание всех предстоящих матчей"""
        logger.info("Предсказание предстоящих матчей...")
        with self._get_connection() as conn:
            if force_update_all:
                # Обновляем все будущие матчи, для которых нет результата
                query = """
                SELECT um.match_id
                FROM upcoming_match um
                LEFT JOIN result_match r ON um.match_id = r.match_id
                WHERE r.match_id IS NULL
                """
                upcoming_matches = pd.read_sql_query(query, conn)
            else:
                # Только те, у которых нет прогноза или он устарел
                query = """
                SELECT DISTINCT um.match_id
                FROM upcoming_match um
                LEFT JOIN predict p ON um.match_id = p.match_id
                WHERE p.match_id IS NULL
                    OR p.last_updated < datetime('now', '-1 day')
                """
                upcoming_matches = pd.read_sql_query(query, conn)
        if upcoming_matches.empty:
            logger.info("Нет матчей для предсказания")
            return
        logger.info(f"Найдено {len(upcoming_matches)} матчей для предсказания")
        self.load_calibrator()
        predictions = []
        for match_id in upcoming_matches['match_id']:
            try:
                pred = self.predict_match(match_id)
                if pred:
                    predictions.append(pred)
            except Exception as e:
                logger.error(f"Ошибка при предсказании матча {match_id}: {e}")
        # Сохраняем предсказания в БД
        if predictions:
            self._save_predictions(predictions)
            logger.info(f"Сохранено {len(predictions)} предсказаний")
    
    def _save_predictions(self, predictions):
        """Сохранение предсказаний в БД"""
        with self._get_connection() as conn:
            # Создаем таблицу если не существует
            conn.execute("""
            CREATE TABLE IF NOT EXISTS predict (
                match_id INTEGER PRIMARY KEY,
                team1_score REAL,
                team2_score REAL,
                team1_score_final INTEGER,
                team2_score_final INTEGER,
                confidence REAL,
                model_version TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Сохраняем предсказания
            for pred in predictions:
                conn.execute("""
                INSERT OR REPLACE INTO predict 
                (match_id, team1_score, team2_score, team1_score_final, 
                 team2_score_final, confidence, model_version, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pred['match_id'],
                    pred['team1_score'],
                    pred['team2_score'],
                    pred['team1_score_final'],
                    pred['team2_score_final'],
                    pred['confidence'],
                    pred['model_version'],
                    pred['prediction_time']
                ))
            
            conn.commit()
    
    def retrain(self):
        """Дообучение модели на новых данных"""
        logger.info("Дообучение модели на новых данных")
        # Проверяем, есть ли существующая модель
        model_file = f"{self.model_path}/cs2_predictor.pkl"
        if os.path.exists(model_file):
            # Сохраняем текущую модель как backup
            backup_path = f"{model_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.rename(model_file, backup_path)
            logger.info(f"Создан backup модели: {backup_path}")
        # Обучаем новую модель
        success = self.train(time_limit=120, n_models=20)  # 2 часа, больше моделей
        if not success:
            # Восстанавливаем старую модель если обучение не удалось
            if os.path.exists(backup_path):
                os.rename(backup_path, model_file)
                logger.error("Обучение не удалось, восстановлена предыдущая модель")
            return False
        logger.info("Дообучение завершено успешно!")
        # После retrain обновляем ВСЕ прогнозы для будущих матчей
        self.predict_upcoming_matches(force_update_all=True)
        return True


def main():
    """Главная функция"""
    predictor = CS2MatchPredictor()
    
    # Проверяем, есть ли обученная модель
    model_file = f"{MODEL_PATH}/cs2_predictor.pkl"
    if not os.path.exists(model_file):
        logger.info("Модель не найдена, начинаем обучение...")
        predictor.train(time_limit=60, n_models=15)
    else:
        logger.info("Загружаем существующую модель...")
        from pycaret.classification import load_model
        predictor.best_model = load_model(f"{MODEL_PATH}/cs2_predictor")
    
    # Предсказываем предстоящие матчи
    predictor.predict_upcoming_matches()
    
    logger.info("Работа предиктора завершена!")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "retrain":
        predictor = CS2MatchPredictor()
        predictor.retrain()
    else:
        main() 