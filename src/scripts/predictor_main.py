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
            'rank_diff', 'rank_ratio', 'h2h_total', 'h2h_winrate_team1',
            'hour', 'weekday', 'log_rank_team1', 'log_rank_team2',
            'team1_avg_rating', 'team1_avg_kd', 'team1_avg_adr', 'team1_avg_kast',
            'team1_max_rating', 'team1_min_rating', 'team2_avg_rating', 'team2_avg_kd',
            'team2_avg_adr', 'team2_avg_kast', 'team2_max_rating', 'team2_min_rating',
            'team1_avg_rating_2_1', 'team1_avg_firepower', 'team1_avg_opening',
            'team1_avg_clutching', 'team1_avg_sniping', 'team2_avg_rating_2_1',
            'team2_avg_firepower', 'team2_avg_opening', 'team2_avg_clutching',
            'team2_avg_sniping', 'rating_diff', 'kd_diff', 'firepower_diff',
            'team1_recent_winrate', 'team1_avg_score_for', 'team1_avg_score_against',
            'team1_matches_played', 'team2_recent_winrate', 'team2_avg_score_for',
            'team2_avg_score_against', 'team2_matches_played', 'winrate_diff',
            'matches_played_diff'
        ]
        
        logger.info("Инициализация CS2MatchPredictor с PyCaret")
        
    def _get_connection(self):
        """Получение соединения с БД"""
        return sqlite3.connect(self.db_path)
    
    def _create_base_features(self, df):
        """Создание базовых признаков из сырых данных"""
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
            'team2_avg_sniping'
        ]
        
        # Инициализируем все признаки нулями
        for feature in player_features:
            df[feature] = 0.0
        
        with self._get_connection() as conn:
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
            
            # Получаем дополнительную информацию об игроках
            players_query = """
            SELECT 
                ps.match_id,
                ps.team_id,
                AVG(p.rating_2_1) as avg_rating_2_1,
                AVG(p.firepower) as avg_firepower,
                AVG(p.opening) as avg_opening,
                AVG(p.clutching) as avg_clutching,
                AVG(p.sniping) as avg_sniping
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            GROUP BY ps.match_id, ps.team_id
            """
            players_info = pd.read_sql_query(players_query, conn)
        
        # Обрабатываем для каждого матча отдельно
        result_dfs = []
        for match_id in df['match_id'].unique():
            match_df = df[df['match_id'] == match_id].copy()
            
            # Статистика для команды 1
            team1_id = match_df['team1_id'].iloc[0]
            team1_stats = player_stats[(player_stats['match_id'] == match_id) & (player_stats['team_id'] == team1_id)]
            team1_players = players_info[(players_info['match_id'] == match_id) & (players_info['team_id'] == team1_id)]
            
            if not team1_stats.empty:
                for col in ['avg_rating', 'avg_kd', 'avg_adr', 'avg_kast', 'max_rating', 'min_rating']:
                    match_df[f'team1_{col}'] = team1_stats[col].iloc[0] if col in team1_stats.columns else 0.0
            
            if not team1_players.empty:
                for col in ['avg_rating_2_1', 'avg_firepower', 'avg_opening', 'avg_clutching', 'avg_sniping']:
                    match_df[f'team1_{col}'] = team1_players[col].iloc[0] if col in team1_players.columns else 0.0
            
            # Статистика для команды 2
            team2_id = match_df['team2_id'].iloc[0]
            team2_stats = player_stats[(player_stats['match_id'] == match_id) & (player_stats['team_id'] == team2_id)]
            team2_players = players_info[(players_info['match_id'] == match_id) & (players_info['team_id'] == team2_id)]
            
            if not team2_stats.empty:
                for col in ['avg_rating', 'avg_kd', 'avg_adr', 'avg_kast', 'max_rating', 'min_rating']:
                    match_df[f'team2_{col}'] = team2_stats[col].iloc[0] if col in team2_stats.columns else 0.0
            
            if not team2_players.empty:
                for col in ['avg_rating_2_1', 'avg_firepower', 'avg_opening', 'avg_clutching', 'avg_sniping']:
                    match_df[f'team2_{col}'] = team2_players[col].iloc[0] if col in team2_players.columns else 0.0
            
            result_dfs.append(match_df)
        
        df = pd.concat(result_dfs, ignore_index=True)
        
        # Создаем относительные признаки
        df['rating_diff'] = df['team1_avg_rating'] - df['team2_avg_rating']
        df['kd_diff'] = df['team1_avg_kd'] - df['team2_avg_kd']
        df['firepower_diff'] = df['team1_avg_firepower'] - df['team2_avg_firepower']
        
        return df
    
    def _create_recent_form_features(self, df):
        """Создание признаков формы команд за последние матчи"""
        logger.info("Вычисление формы команд...")
        
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
        
        # Разделяем на обучающую и валидационную выборки по времени
        split_date = end_date - timedelta(days=30)  # Последние 30 дней для валидации
        split_timestamp = int(split_date.timestamp())
        
        train_df = df[df['datetime'] < split_timestamp].copy()
        test_df = df[df['datetime'] >= split_timestamp].copy()
        
        logger.info(f"Обучающая выборка: {len(train_df)} матчей")
        logger.info(f"Тестовая выборка: {len(test_df)} матчей")
        
        # Настройка PyCaret
        logger.info("Инициализация PyCaret...")
        
        # Выбираем только нужные колонки
        train_data = train_df[self.feature_columns + ['team1_won']]
        test_data = test_df[self.feature_columns + ['team1_won']]
        
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
        
        # Обучаем финальную модель на всех данных
        logger.info("Обучение финальной модели...")
        self.best_model = finalize_model(best_model)
        
        # Сохраняем модель
        save_model(self.best_model, f'{self.model_path}/cs2_predictor')
        
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
        self._evaluate_model(test_df)
        
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
                          'team2_id', 'team2_rank', 'event_id',
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
            
            # Получаем список признаков, которые нужно использовать для предсказания
            prediction_features = [
                'rank_diff', 'rank_ratio', 'h2h_total', 'h2h_winrate_team1',
                'hour', 'weekday', 'log_rank_team1', 'log_rank_team2',
                'team1_avg_rating', 'team1_avg_kd', 'team1_avg_adr', 'team1_avg_kast',
                'team1_max_rating', 'team1_min_rating', 'team2_avg_rating', 'team2_avg_kd',
                'team2_avg_adr', 'team2_avg_kast', 'team2_max_rating', 'team2_min_rating',
                'team1_avg_rating_2_1', 'team1_avg_firepower', 'team1_avg_opening',
                'team1_avg_clutching', 'team1_avg_sniping', 'team2_avg_rating_2_1',
                'team2_avg_firepower', 'team2_avg_opening', 'team2_avg_clutching',
                'team2_avg_sniping', 'rating_diff', 'kd_diff', 'firepower_diff',
                'team1_recent_winrate', 'team1_avg_score_for', 'team1_avg_score_against',
                'team1_matches_played', 'team2_recent_winrate', 'team2_avg_score_for',
                'team2_avg_score_against', 'team2_matches_played', 'winrate_diff',
                'matches_played_diff'
            ]
            
            # Проверяем наличие всех необходимых признаков для предсказания
            missing_features = [col for col in prediction_features if col not in features_df.columns]
            if missing_features:
                logger.error(f"Отсутствуют необходимые признаки для предсказания: {missing_features}")
                return None
            
            # Предсказываем
            match_data = features_df[[
                'rank_diff', 'rank_ratio', 'h2h_total', 'h2h_winrate_team1',
                'hour', 'weekday', 'log_rank_team1', 'log_rank_team2',
                'team1_avg_rating', 'team1_avg_kd', 'team1_avg_adr', 'team1_avg_kast',
                'team1_max_rating', 'team1_min_rating', 'team2_avg_rating', 'team2_avg_kd',
                'team2_avg_adr', 'team2_avg_kast', 'team2_max_rating', 'team2_min_rating',
                'team1_avg_rating_2_1', 'team1_avg_firepower', 'team1_avg_opening',
                'team1_avg_clutching', 'team1_avg_sniping', 'team2_avg_rating_2_1',
                'team2_avg_firepower', 'team2_avg_opening', 'team2_avg_clutching',
                'team2_avg_sniping', 'rating_diff', 'kd_diff', 'firepower_diff',
                'team1_recent_winrate', 'team1_avg_score_for', 'team1_avg_score_against',
                'team1_matches_played', 'team2_recent_winrate', 'team2_avg_score_for',
                'team2_avg_score_against', 'team2_matches_played', 'winrate_diff',
                'matches_played_diff'
            ]]
            
            # Используем модель напрямую
            if hasattr(self.best_model, 'predict_proba'):
                probabilities = self.best_model.predict_proba(match_data)
                prob_team1_win = probabilities[0][1]
            else:
                # Если модель не поддерживает вероятности
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
    
    def predict_upcoming_matches(self):
        """Предсказание всех предстоящих матчей"""
        logger.info("Предсказание предстоящих матчей...")
        
        with self._get_connection() as conn:
            # Получаем непредсказанные матчи
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
        self.predict_upcoming_matches()
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