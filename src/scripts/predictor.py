import os
import sys
import json
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm
from loguru import logger
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, mean_absolute_error
import joblib

# ML models
from lightgbm import LGBMClassifier, LGBMRegressor
from catboost import CatBoostClassifier, CatBoostRegressor
from pytorch_tabnet.tab_model import TabNetClassifier, TabNetRegressor

# --- Константы ---
DB_PATH = os.getenv('HLTV_DB_PATH', 'hltv.db')
FEATURES_DIR = 'storage/json/predict_features'
LOG_PATH = 'logs/predict.log'
MODEL_PATH = 'storage/model_predictor.pkl'
MODEL_VERSION = 'v1.0'

os.makedirs(FEATURES_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)
os.makedirs('storage', exist_ok=True)

logger.add(LOG_PATH, rotation="1 week", retention="4 weeks")

# --- Вспомогательные функции ---
def fetch_df(query, db_path=DB_PATH):
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(query, conn)

def save_features_json(match_id, features, map_name=None):
    fname = f"{match_id}.json" if map_name is None else f"{match_id}_{map_name}.json"
    path = os.path.join(FEATURES_DIR, fname)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(features, f, ensure_ascii=False, indent=2)

def save_model(model, path=MODEL_PATH):
    joblib.dump(model, path)

def load_model(path=MODEL_PATH):
    return joblib.load(path)

# --- Основной класс ---
class Predictor:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.model = None
        self.model_version = MODEL_VERSION

    def load_data(self):
        # Загрузка всех нужных таблиц
        logger.info('Загрузка данных из базы...')
        self.matches = fetch_df('SELECT * FROM result_match', self.db_path)
        self.players_stats = fetch_df('SELECT * FROM player_stats', self.db_path)
        self.maps = fetch_df('SELECT * FROM result_match_maps', self.db_path)
        self.players = fetch_df('SELECT * FROM players', self.db_path)
        self.upcoming = fetch_df('SELECT * FROM upcoming_match', self.db_path)
        self.upcoming_players = fetch_df('SELECT * FROM upcoming_match_players', self.db_path)

    def get_recent_map_stats(self, team_id, map_name, n=5):
        # Возвращает winrate, средний счет, средний ранг соперников за последние n матчей на карте
        maps = self.maps[(self.maps['team1_id'] == team_id) & (self.maps['map_name'] == map_name)]
        # --- Новый блок: если нет столбца 'date' или все значения пустые, подставляем фиксированную дату ---
        fixed_date = pd.Timestamp('2025-05-01')
        if 'date' not in maps.columns or maps['date'].isnull().all():
            maps = maps.copy()
            maps['date'] = fixed_date
        else:
            # Если есть хотя бы одно значение, но есть и пропуски, заполняем их фиксированной датой
            maps = maps.copy()
            maps['date'] = maps['date'].fillna(fixed_date)
        maps = maps.sort_values('date', ascending=False).head(n)
        if maps.empty:
            return 0, 0, 0
        winrate = (maps['team1_rounds'] > maps['team2_rounds']).mean()
        mean_score = maps['team1_rounds'].mean()
        mean_opp_rank = maps['team2_rank'].mean() if 'team2_rank' in maps.columns else 0
        return winrate, mean_score, mean_opp_rank

    def get_lineup_stability(self, match_id, team_id, player_ids):
        # Количество матчей этим составом и есть ли замена
        prev_matches = self.players_stats[(self.players_stats['team_id'] == team_id) & (self.players_stats['match_id'] < match_id)]
        prev_lineups = prev_matches.groupby('match_id')['player_id'].apply(set)
        current_set = set(player_ids)
        same_lineup_count = sum(1 for lineup in prev_lineups if lineup == current_set)
        has_sub = any(current_set != lineup for lineup in prev_lineups)
        return same_lineup_count, int(has_sub)

    def get_last_match_days(self, team_id, match_date):
        # Сколько дней с последнего матча
        team_matches = self.matches[(self.matches['team1_id'] == team_id) | (self.matches['team2_id'] == team_id)]
        team_matches = team_matches[team_matches['datetime'] < match_date]
        if team_matches.empty:
            return -1
        last_date = team_matches['datetime'].max()
        return (match_date - last_date) // (24*3600)

    def get_matches_last_30d(self, team_id, match_date):
        # Количество матчей за последние 30 дней
        team_matches = self.matches[(self.matches['team1_id'] == team_id) | (self.matches['team2_id'] == team_id)]
        team_matches = team_matches[(team_matches['datetime'] < match_date) & (team_matches['datetime'] > match_date - 30*24*3600)]
        return team_matches.shape[0]

    def aggregate_player_stats_time_weighted(self, player_ids, team_id, match_date):
        # Взвешенная по времени агрегация статистики игроков
        stats = self.players_stats[(self.players_stats['player_id'].isin(player_ids)) & (self.players_stats['team_id'] == team_id)]
        if stats.empty:
            return {k: 0 for k in ['kills', 'deaths', 'kd_ratio', 'plus_minus', 'adr', 'kast', 'rating', 'rating_std']}
        stats = stats.copy()
        fixed_date = pd.Timestamp('2025-05-01').timestamp()
        if 'datetime' in stats.columns:
            # Если есть хотя бы одно значение, но есть и пропуски, заполняем их фиксированной датой
            stats['datetime'] = stats['datetime'].fillna(fixed_date)
            stats['days_ago'] = (match_date - stats['datetime']) // (24*3600)
            stats['weight'] = np.exp(-stats['days_ago']/30)
        else:
            stats['weight'] = 1  # если нет даты — все веса равны
        agg = {}
        for field in ['kills', 'deaths', 'kd_ratio', 'plus_minus', 'adr', 'kast', 'rating']:
            if field in stats.columns:
                agg[field] = np.average(stats[field], weights=stats['weight'])
            else:
                agg[field] = 0
        agg['rating_std'] = stats['rating'].std() if 'rating' in stats.columns else 0
        return agg

    def get_common_features(self, match, t1_players, t2_players):
        # Агрегация формы игроков (players) ДО матча
        t1_feats = self.players[self.players['player_id'].isin(t1_players)].select_dtypes(include=[np.number])
        t2_feats = self.players[self.players['player_id'].isin(t2_players)].select_dtypes(include=[np.number])
        t1_agg = t1_feats.mean().add_prefix('t1_mean_').to_dict()
        t2_agg = t2_feats.mean().add_prefix('t2_mean_').to_dict()
        # --- Новая агрегация по player_stats с учетом времени ---
        match_date = match['datetime'] if 'datetime' in match else 0
        t1_stats_agg = self.aggregate_player_stats_time_weighted(t1_players, match['team1_id'], match_date)
        t2_stats_agg = self.aggregate_player_stats_time_weighted(t2_players, match['team2_id'], match_date)
        t1_stats_agg = {f't1_agg_{k}': v for k, v in t1_stats_agg.items()}
        t2_stats_agg = {f't2_agg_{k}': v for k, v in t2_stats_agg.items()}
        # Head-to-head
        h2h = self.matches[((self.matches['team1_id'] == match['team1_id']) & (self.matches['team2_id'] == match['team2_id'])) |
                           ((self.matches['team1_id'] == match['team2_id']) & (self.matches['team2_id'] == match['team1_id']))]
        h2h_count = len(h2h)
        h2h_team1_wins = h2h[(h2h['team1_id'] == match['team1_id']) & (h2h['team1_score'] > h2h['team2_score'])].shape[0] + \
                         h2h[(h2h['team2_id'] == match['team1_id']) & (h2h['team2_score'] > h2h['team1_score'])].shape[0]
        h2h_team2_wins = h2h_count - h2h_team1_wins
        # Числовые признаки из upcoming_match (train: из result_match, если есть)
        exclude_cols = ['match_id', 'team1_id', 'team2_id', 'event_id', 'team1_score', 'team2_score']
        numeric_cols = [col for col in match.index if col not in exclude_cols and pd.api.types.is_numeric_dtype(type(match[col]))]
        match_numeric = {col: match[col] for col in numeric_cols}
        # --- Новые метрики по картам ---
        t1_maps = self.maps[self.maps['team1_id'] == match['team1_id']]
        t2_maps = self.maps[self.maps['team2_id'] == match['team2_id']]
        t1_maps_mean_rounds = t1_maps['team1_rounds'].mean() if not t1_maps.empty else 0
        t2_maps_mean_rounds = t2_maps['team2_rounds'].mean() if not t2_maps.empty else 0
        t1_maps_count = t1_maps.shape[0]
        t2_maps_count = t2_maps.shape[0]
        # --- Новые динамические признаки по картам ---
        t1_win3, t1_score3, t1_opp3 = self.get_recent_map_stats(match['team1_id'], match.get('map_name', ''), n=3)
        t1_win5, t1_score5, t1_opp5 = self.get_recent_map_stats(match['team1_id'], match.get('map_name', ''), n=5)
        t2_win3, t2_score3, t2_opp3 = self.get_recent_map_stats(match['team2_id'], match.get('map_name', ''), n=3)
        t2_win5, t2_score5, t2_opp5 = self.get_recent_map_stats(match['team2_id'], match.get('map_name', ''), n=5)
        # --- Стабильность состава ---
        t1_lineup_count, t1_has_sub = self.get_lineup_stability(match['match_id'], match['team1_id'], t1_players)
        t2_lineup_count, t2_has_sub = self.get_lineup_stability(match['match_id'], match['team2_id'], t2_players)
        # --- Временные признаки ---
        t1_days_last = self.get_last_match_days(match['team1_id'], match_date)
        t2_days_last = self.get_last_match_days(match['team2_id'], match_date)
        t1_matches_30d = self.get_matches_last_30d(match['team1_id'], match_date)
        t2_matches_30d = self.get_matches_last_30d(match['team2_id'], match_date)
        feats = {
            'team1_id': match['team1_id'],
            'team2_id': match['team2_id'],
            'team1_rank': match.get('team1_rank', None),
            'team2_rank': match.get('team2_rank', None),
            'event_id': match.get('event_id', None),
            'head_to_head_count': h2h_count,
            'head_to_head_team1_wins': h2h_team1_wins,
            'head_to_head_team2_wins': h2h_team2_wins,
            'team1_maps_mean_rounds': t1_maps_mean_rounds,
            'team2_maps_mean_rounds': t2_maps_mean_rounds,
            'team1_maps_count': t1_maps_count,
            'team2_maps_count': t2_maps_count,
            't1_winrate_last3': t1_win3,
            't1_winrate_last5': t1_win5,
            't1_score_last3': t1_score3,
            't1_score_last5': t1_score5,
            't1_opp_rank_last3': t1_opp3,
            't1_opp_rank_last5': t1_opp5,
            't2_winrate_last3': t2_win3,
            't2_winrate_last5': t2_win5,
            't2_score_last3': t2_score3,
            't2_score_last5': t2_score5,
            't2_opp_rank_last3': t2_opp3,
            't2_opp_rank_last5': t2_opp5,
            't1_lineup_stability': t1_lineup_count,
            't2_lineup_stability': t2_lineup_count,
            't1_has_sub': t1_has_sub,
            't2_has_sub': t2_has_sub,
            't1_days_last_match': t1_days_last,
            't2_days_last_match': t2_days_last,
            't1_matches_30d': t1_matches_30d,
            't2_matches_30d': t2_matches_30d,
        }
        feats.update(match_numeric)
        feats.update(t1_agg)
        feats.update(t2_agg)
        feats.update(t1_stats_agg)
        feats.update(t2_stats_agg)
        return feats

    def feature_engineering(self, for_train=True):
        logger.info('Формирование признаков (универсально для train и predict)...')
        if for_train:
            matches = self.matches.copy()
            players_stats = self.players_stats
        else:
            matches = self.upcoming.copy()
            players_stats = self.upcoming_players
        features = []
        for _, match in matches.iterrows():
            match_id = match['match_id']
            if for_train:
                t1_players = players_stats[(players_stats['match_id'] == match_id) & (players_stats['team_id'] == match['team1_id'])]['player_id'].tolist()
                t2_players = players_stats[(players_stats['match_id'] == match_id) & (players_stats['team_id'] == match['team2_id'])]['player_id'].tolist()
            else:
                t1_players = players_stats[(players_stats['match_id'] == match_id) & (players_stats['team_id'] == match['team1_id'])]['player_id'].tolist()
                t2_players = players_stats[(players_stats['match_id'] == match_id) & (players_stats['team_id'] == match['team2_id'])]['player_id'].tolist()
            feats = self.get_common_features(match, t1_players, t2_players)
            feats['match_id'] = match_id
            # --- Новые признаки по картам для train ---
            if for_train:
                t1_map = self.maps[self.maps['team1_id'] == match['team1_id']]
                t2_map = self.maps[self.maps['team2_id'] == match['team2_id']]
                feats['t1_map_mean_rounds'] = t1_map['team1_rounds'].mean() if not t1_map.empty else 0
                feats['t2_map_mean_rounds'] = t2_map['team2_rounds'].mean() if not t2_map.empty else 0
                feats['t1_map_count'] = t1_map.shape[0]
                feats['t2_map_count'] = t2_map.shape[0]
                feats['t1_map_winrate'] = (t1_map['team1_rounds'] > t1_map['team2_rounds']).mean() if not t1_map.empty else 0
                feats['t2_map_winrate'] = (t2_map['team2_rounds'] > t2_map['team1_rounds']).mean() if not t2_map.empty else 0
            features.append(feats)
        if for_train:
            self.features = pd.DataFrame(features)
            # Добавляем целевые переменные из result_match
            scores = self.matches[['match_id', 'team1_score', 'team2_score']]
            self.features = self.features.merge(scores, on='match_id', how='left')
        else:
            self.upcoming_features = pd.DataFrame(features)

    def train(self):
        logger.info('Обучение модели...')
        self.feature_engineering(for_train=True)
        # Используем эталонный список признаков
        with open('storage/model_features_etalon.json', 'r') as f:
            features_etalon = json.load(f)
        feature_list = features_etalon['team1'] + features_etalon['team2']
        X = self.features[feature_list]
        logger.info(f'Используемые признаки: {feature_list}')
        y1 = self.features['team1_score']
        y2 = self.features['team2_score']
        # Классификация победителя
        y_win = (self.features['team1_score'] > self.features['team2_score']).astype(int)
        X_train, X_val, y1_train, y1_val = train_test_split(X, y1, test_size=0.2, random_state=42)
        X_train2, X_val2, y2_train, y2_val = train_test_split(X, y2, test_size=0.2, random_state=42)
        X_train_clf, X_val_clf, y_win_train, y_win_val = train_test_split(X, y_win, test_size=0.2, random_state=42)
        model1 = LGBMRegressor(n_estimators=200)
        model2 = LGBMRegressor(n_estimators=200)
        clf = LGBMClassifier(n_estimators=200)
        model1.fit(X_train, y1_train)
        model2.fit(X_train2, y2_train)
        clf.fit(X_train_clf, y_win_train)
        y1_pred = model1.predict(X_val)
        y2_pred = model2.predict(X_val2)
        y_win_pred = clf.predict(X_val_clf)
        logger.info(f"MAE team1_score: {mean_absolute_error(y1_val, y1_pred):.3f}")
        logger.info(f"MAE team2_score: {mean_absolute_error(y2_val, y2_pred):.3f}")
        logger.info(f"Accuracy победителя: {accuracy_score(y_win_val, y_win_pred):.3f}")
        self.model = (model1, model2, clf)
        save_model(self.model)
        # Сохраняем список признаков (эталонный)
        with open('storage/model_features.json', 'w') as f:
            json.dump(feature_list, f)
        logger.info('Модель и признаки сохранены.')

    def postprocess_score(self, score, max_score):
        return int(min(max(round(score), 0), max_score))

    def postprocess_four_outcomes(self, team1_pred, team2_pred, strong_win_threshold=1.0):
        diff = team1_pred - team2_pred
        if diff >= strong_win_threshold:
            return 2, 0  # Уверенная победа первой команды
        elif diff > 0:
            return 2, 1  # Победа первой команды с борьбой
        elif diff <= -strong_win_threshold:
            return 0, 2  # Уверенная победа второй команды
        else:
            return 1, 2  # Победа второй команды с борьбой

    def postprocess_bo3(self, team1_pred, team2_pred):
        t1 = int(round(team1_pred))
        t2 = int(round(team2_pred))
        t1 = min(max(t1, 0), 2)
        t2 = min(max(t2, 0), 2)
        if t1 == t2:
            if team1_pred >= team2_pred:
                t1, t2 = 2, t2 if t2 < 2 else 1
            else:
                t1, t2 = t1 if t1 < 2 else 1, 2
        elif t1 < 2 and t2 < 2:
            if team1_pred > team2_pred:
                t1, t2 = 2, t2
            else:
                t1, t2 = t1, 2
        if t1 > 2 or t2 > 2:
            if team1_pred > team2_pred:
                t1, t2 = 2, 1
            else:
                t1, t2 = 1, 2
        if t1 == 2:
            t2 = min(t2, 1)
        if t2 == 2:
            t1 = min(t1, 1)
        return t1, t2

    def postprocess_map_score(self, team1_pred, team2_pred, max_score=13, loser_max=11):
        diff = abs(team1_pred - team2_pred)
        # Калиброванная формула: loser_score = -0.58 * diff_pred + 8.61
        loser_score = int(round(max(0, min(loser_max, -0.58 * diff + 8.61))))
        if team1_pred >= team2_pred:
            return max_score, loser_score
        else:
            return loser_score, max_score

    def calc_confidence(self, team1_id, team2_id, by_map=False, map_name=None, match_row=None):
        # Новый advanced_confidence
        # 1. Объем данных
        if by_map and map_name:
            t1_maps = self.maps[(self.maps['team1_id'] == team1_id) & (self.maps['map_name'] == map_name)]
            t2_maps = self.maps[(self.maps['team2_id'] == team2_id) & (self.maps['map_name'] == map_name)]
        else:
            t1_maps = self.maps[self.maps['team1_id'] == team1_id]
            t2_maps = self.maps[self.maps['team2_id'] == team2_id]
        t1_matches = self.matches[self.matches['team1_id'] == team1_id]
        t2_matches = self.matches[self.matches['team2_id'] == team2_id]
        t1_stats = self.players_stats[self.players_stats['team_id'] == team1_id]
        t2_stats = self.players_stats[self.players_stats['team_id'] == team2_id]
        t1_vol = len(t1_maps) + len(t1_matches) + len(t1_stats)
        t2_vol = len(t2_maps) + len(t2_matches) + len(t2_stats)
        # 2. Нормировка
        all_team_ids = pd.concat([
            self.maps['team1_id'], self.maps['team2_id'],
            self.matches['team1_id'], self.matches['team2_id'],
            self.players_stats['team_id']
        ]).unique()
        max_data_volume = 1
        for tid in all_team_ids:
            maps_count = ((self.maps['team1_id'] == tid) | (self.maps['team2_id'] == tid)).sum()
            matches_count = ((self.matches['team1_id'] == tid) | (self.matches['team2_id'] == tid)).sum()
            stats_count = (self.players_stats['team_id'] == tid).sum()
            volume = maps_count + matches_count + stats_count
            if volume > max_data_volume:
                max_data_volume = volume
        conf = min(t1_vol, t2_vol) / max_data_volume
        # 3. Penalty for top teams (rank < 20)
        t1_rank = match_row.get('team1_rank', 100) if match_row is not None else 100
        t2_rank = match_row.get('team2_rank', 100) if match_row is not None else 100
        if t1_rank < 20 and t2_rank < 20:
            conf *= 0.3
        elif t1_rank < 20 or t2_rank < 20:
            conf *= 0.5
        # 4. Penalty for high-error tournaments/pairs (если есть)
        # Для production: можно подгружать из файла/БД, здесь просто пример
        high_error_tournaments = set([
            # 'kleverr Virsliga Season 4 Finals', ...
        ])
        high_error_pairs = set([
            # ('Alliance', 'SABRE'), ...
        ])
        event_name = match_row.get('event_name', None) if match_row is not None else None
        team1_name = match_row.get('team1_name', None) if match_row is not None else None
        team2_name = match_row.get('team2_name', None) if match_row is not None else None
        if event_name in high_error_tournaments:
            conf *= 0.5
        if (team1_name, team2_name) in high_error_pairs:
            conf *= 0.5
        # 5. Penalty за мало матчей/нестабильный состав
        t1_matches_30d = match_row.get('t1_matches_30d', 5) if match_row is not None else 5
        t2_matches_30d = match_row.get('t2_matches_30d', 5) if match_row is not None else 5
        t1_lineup_stability = match_row.get('t1_lineup_stability', 5) if match_row is not None else 5
        t2_lineup_stability = match_row.get('t2_lineup_stability', 5) if match_row is not None else 5
        if t1_matches_30d < 3 or t2_matches_30d < 3:
            conf *= 0.8
        if t1_lineup_stability < 2 or t2_lineup_stability < 2:
            conf *= 0.7
        # 6. Penalty за отсутствие важных признаков
        important_features = ['t1_agg_rating', 't2_agg_rating', 't1_winrate_last3', 't2_winrate_last3']
        missing = 0
        if match_row is not None:
            for f in important_features:
                if pd.isnull(match_row.get(f, None)) or match_row.get(f, 0) == 0:
                    missing += 1
        if missing > 0:
            conf *= (1 - 0.1 * missing)
        return round(conf, 3)

    def predict_upcoming(self):
        logger.info('Прогноз для будущих матчей...')
        self.load_data()
        self.feature_engineering(for_train=False)
        # Загружаем список признаков
        with open('storage/model_features.json', 'r') as f:
            feature_list = json.load(f)
        # Для simplicity: прогнозируем только для матчей, которых нет в predict
        with sqlite3.connect(self.db_path) as conn:
            existing = pd.read_sql_query('SELECT match_id FROM predict', conn)
        to_predict = self.upcoming_features[~self.upcoming_features['match_id'].isin(existing['match_id'])]
        # Добавляю имена команд для фильтрации по TBD/winner/loser
        to_predict = to_predict.merge(self.upcoming[['match_id', 'team1_name', 'team2_name', 'team1_id', 'team2_id']], on='match_id', how='left')
        results = []
        for _, row in tqdm(to_predict.iterrows(), total=to_predict.shape[0]):
            match_id = row['match_id']
            # Получаем id команд из self.upcoming по match_id
            match_row = self.upcoming[self.upcoming['match_id'] == match_id].iloc[0]
            t1_id = match_row['team1_id']
            t2_id = match_row['team2_id']
            # Пропуск матчей с TBD/winner/loser в названии команды
            t1_name = str(row.get('team1_name', '')).lower()
            t2_name = str(row.get('team2_name', '')).lower()
            if any(x in t1_name for x in ['tbd', 'winner', 'loser']) or any(x in t2_name for x in ['tbd', 'winner', 'loser']):
                continue
            feats = row.drop(['match_id', 'team1_id', 'team2_id'], errors='ignore').to_frame().T
            for col in feature_list:
                if col not in feats.columns:
                    feats[col] = 0
            feats = feats[feature_list]
            feats = feats.astype(float)
            team1_score = float(self.model[0].predict(feats)[0])
            team2_score = float(self.model[1].predict(feats)[0])
            team1_score_final, team2_score_final = self.postprocess_four_outcomes(team1_score, team2_score)
            confidence = self.calc_confidence(t1_id, t2_id, by_map=False, match_row=match_row)
            save_features_json(match_id, feats.iloc[0].to_dict())
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''INSERT INTO predict (match_id, team1_score, team2_score, team1_score_final, team2_score_final, model_version, last_updated, confidence) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                             (match_id, team1_score, team2_score, team1_score_final, team2_score_final, self.model_version, datetime.now().isoformat(), confidence))
                conn.commit()
            results.append((match_id, team1_score, team2_score, confidence))
        logger.info(f'Сделано прогнозов: {len(results)}')
        # Прогноз по картам (перезаписываем старые значения)
        map_names = ['Nuke', 'Mirage', 'Inferno', 'Ancient', 'Anubis', 'Vertigo', 'Dust2']
        with sqlite3.connect(self.db_path) as conn:
            for _, match in self.upcoming.iterrows():
                t1_id = match['team1_id']
                t2_id = match['team2_id']
                # Пропуск матчей с TBD/winner/loser в названии команды
                t1_name = str(match.get('team1_name', '')).lower()
                t2_name = str(match.get('team2_name', '')).lower()
                if any(x in t1_name for x in ['tbd', 'winner', 'loser']) or any(x in t2_name for x in ['tbd', 'winner', 'loser']):
                    continue
                match_id = match['match_id']
                t1_players = self.upcoming_players[(self.upcoming_players['match_id'] == match_id) & (self.upcoming_players['team_id'] == t1_id)]['player_id'].tolist()
                t2_players = self.upcoming_players[(self.upcoming_players['match_id'] == match_id) & (self.upcoming_players['team_id'] == t2_id)]['player_id'].tolist()
                for map_name in map_names:
                    # Индивидуальные признаки по карте для обеих команд
                    feats = self.get_common_features(match, t1_players, t2_players)
                    t1_map = self.maps[(self.maps['team1_id'] == t1_id) & (self.maps['map_name'] == map_name)]
                    t2_map = self.maps[(self.maps['team2_id'] == t2_id) & (self.maps['map_name'] == map_name)]
                    feats['t1_map_mean_rounds'] = t1_map['team1_rounds'].mean() if not t1_map.empty else 0
                    feats['t2_map_mean_rounds'] = t2_map['team2_rounds'].mean() if not t2_map.empty else 0
                    feats['t1_map_count'] = t1_map.shape[0]
                    feats['t2_map_count'] = t2_map.shape[0]
                    feats['t1_map_winrate'] = (t1_map['team1_rounds'] > t1_map['team2_rounds']).mean() if not t1_map.empty else 0
                    feats['t2_map_winrate'] = (t2_map['team2_rounds'] > t2_map['team1_rounds']).mean() if not t2_map.empty else 0
                    for col in feature_list:
                        if col not in feats:
                            feats[col] = 0
                    feats_df = pd.DataFrame([feats])[feature_list]
                    feats_df = feats_df.astype(float)
                    team1_score = float(self.model[0].predict(feats_df)[0])
                    team2_score = float(self.model[1].predict(feats_df)[0])
                    team1_score_final, team2_score_final = self.postprocess_map_score(team1_score, team2_score, max_score=13)
                    confidence = self.calc_confidence(t1_id, t2_id, by_map=True, map_name=map_name, match_row=match)
                    save_features_json(f"{match_id}_{map_name}", feats_df.iloc[0].to_dict(), map_name=map_name)
                    # Удаляем старый прогноз для этой пары (match_id, map_name)
                    conn.execute('DELETE FROM predict_map WHERE match_id = ? AND map_name = ?', (match_id, map_name))
                    conn.execute('''INSERT INTO predict_map (match_id, map_name, team1_rounds, team2_rounds, team1_rounds_final, team2_rounds_final, model_version, last_updated, confidence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                 (match_id, map_name, team1_score, team2_score, team1_score_final, team2_score_final, self.model_version, datetime.now().isoformat(), confidence))
            conn.commit()
        logger.info(f'Сделано прогнозов по картам для всех матчей.')

    def predict_past_maps(self):
        logger.info('Прогноз для прошедших матчей (по картам)...')
        self.load_data()
        # Используем прошедшие матчи и карты
        past_matches = self.matches.copy()
        past_maps = self.maps.copy()
        # Загружаем список признаков
        with open('storage/model_features.json', 'r') as f:
            feature_list = json.load(f)
        results = []
        for _, match in past_matches.iterrows():
            match_id = match['match_id']
            t1_players = self.players_stats[(self.players_stats['match_id'] == match_id) & (self.players_stats['team_id'] == match['team1_id'])]['player_id'].tolist()
            t2_players = self.players_stats[(self.players_stats['match_id'] == match_id) & (self.players_stats['team_id'] == match['team2_id'])]['player_id'].tolist()
            for map_name in past_maps[past_maps['match_id'] == match_id]['map_name'].unique():
                # Пропуск если нет карты
                if not map_name:
                    continue
                # Признаки как для predict_upcoming
                feats = self.get_common_features(match, t1_players, t2_players)
                t1_map = past_maps[(past_maps['team1_id'] == match['team1_id']) & (past_maps['map_name'] == map_name)]
                t2_map = past_maps[(past_maps['team2_id'] == match['team2_id']) & (past_maps['map_name'] == map_name)]
                feats['t1_map_mean_rounds'] = t1_map['team1_rounds'].mean() if not t1_map.empty else 0
                feats['t2_map_mean_rounds'] = t2_map['team2_rounds'].mean() if not t2_map.empty else 0
                feats['t1_map_count'] = t1_map.shape[0]
                feats['t2_map_count'] = t2_map.shape[0]
                feats['t1_map_winrate'] = (t1_map['team1_rounds'] > t1_map['team2_rounds']).mean() if not t1_map.empty else 0
                feats['t2_map_winrate'] = (t2_map['team2_rounds'] > t2_map['team1_rounds']).mean() if not t2_map.empty else 0
                for col in feature_list:
                    if col not in feats:
                        feats[col] = 0
                feats_df = pd.DataFrame([feats])[feature_list]
                feats_df = feats_df.astype(float)
                team1_pred = float(self.model[0].predict(feats_df)[0])
                team2_pred = float(self.model[1].predict(feats_df)[0])
                # Реальные значения
                real_row = past_maps[(past_maps['match_id'] == match_id) & (past_maps['map_name'] == map_name)]
                if real_row.empty:
                    continue
                team1_real = real_row.iloc[0]['team1_rounds']
                team2_real = real_row.iloc[0]['team2_rounds']
                confidence = self.calc_confidence(match['team1_id'], match['team2_id'], by_map=True, map_name=map_name, match_row=match)
                results.append({
                    'match_id': match_id,
                    'map_name': map_name,
                    'team1_pred': team1_pred,
                    'team2_pred': team2_pred,
                    'team1_real': team1_real,
                    'team2_real': team2_real,
                    'confidence': confidence
                })
        # Сохраняем в CSV
        if os.path.exists('predicted_past_maps.csv'):
            os.remove('predicted_past_maps.csv')
        df_out = pd.DataFrame(results)
        df_out.to_csv('predicted_past_maps.csv', index=False)
        logger.info(f'Сырые предсказания для прошедших карт сохранены в predicted_past_maps.csv')

    def run(self, mode):
        if mode == 'train':
            self.load_data()
            self.train()
        elif mode == 'predict':
            self.model = load_model()
            self.predict_upcoming()
        elif mode == 'predict_past':
            self.model = load_model()
            self.predict_past_maps()
        else:
            logger.error('Неизвестный режим. Используй train или predict.')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='HLTV Predictor')
    parser.add_argument('--mode', choices=['train', 'predict', 'predict_past'], required=True, help='Режим: train, predict или predict_past')
    args = parser.parse_args()
    predictor = Predictor()
    predictor.run(args.mode) 