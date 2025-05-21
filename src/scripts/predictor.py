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

    def aggregate_player_stats(self, player_ids, team_id):
        stats = self.players_stats[(self.players_stats['player_id'].isin(player_ids)) & (self.players_stats['team_id'] == team_id)]
        agg_fields = ['kills', 'deaths', 'kd_ratio', 'plus_minus', 'adr', 'kast', 'rating']
        agg = {}
        for field in agg_fields:
            if field in stats.columns:
                agg[field] = stats[field].mean() if not stats.empty else 0
            else:
                agg[field] = 0
        return agg

    def get_common_features(self, match, t1_players, t2_players):
        # Агрегация формы игроков (players) ДО матча
        t1_feats = self.players[self.players['player_id'].isin(t1_players)].select_dtypes(include=[np.number])
        t2_feats = self.players[self.players['player_id'].isin(t2_players)].select_dtypes(include=[np.number])
        t1_agg = t1_feats.mean().add_prefix('t1_mean_').to_dict()
        t2_agg = t2_feats.mean().add_prefix('t2_mean_').to_dict()
        # --- Новая агрегация по player_stats ---
        t1_stats_agg = self.aggregate_player_stats(t1_players, match['team1_id'])
        t2_stats_agg = self.aggregate_player_stats(t2_players, match['team2_id'])
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
        X_train, X_val, y1_train, y1_val = train_test_split(X, y1, test_size=0.2, random_state=42)
        X_train2, X_val2, y2_train, y2_val = train_test_split(X, y2, test_size=0.2, random_state=42)
        model1 = LGBMRegressor(n_estimators=200)
        model2 = LGBMRegressor(n_estimators=200)
        model1.fit(X_train, y1_train)
        model2.fit(X_train2, y2_train)
        y1_pred = model1.predict(X_val)
        y2_pred = model2.predict(X_val2)
        logger.info(f"MAE team1_score: {mean_absolute_error(y1_val, y1_pred):.3f}")
        logger.info(f"MAE team2_score: {mean_absolute_error(y2_val, y2_pred):.3f}")
        self.model = (model1, model2)
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
        to_predict = to_predict.merge(self.upcoming[['match_id', 'team1_name', 'team2_name']], on='match_id', how='left')
        results = []
        for _, row in tqdm(to_predict.iterrows(), total=to_predict.shape[0]):
            match_id = row['match_id']
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
            save_features_json(match_id, feats.iloc[0].to_dict())
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''INSERT INTO predict (match_id, team1_score, team2_score, team1_score_final, team2_score_final, model_version, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)''',
                             (match_id, team1_score, team2_score, team1_score_final, team2_score_final, self.model_version, datetime.now().isoformat()))
                conn.commit()
            results.append((match_id, team1_score, team2_score))
        logger.info(f'Сделано прогнозов: {len(results)}')
        # Прогноз по картам (перезаписываем старые значения)
        map_names = ['Nuke', 'Mirage', 'Inferno', 'Ancient', 'Anubis', 'Vertigo', 'Dust2']
        with sqlite3.connect(self.db_path) as conn:
            for _, match in self.upcoming.iterrows():
                # Пропуск матчей с TBD/winner/loser в названии команды
                t1_name = str(match.get('team1_name', '')).lower()
                t2_name = str(match.get('team2_name', '')).lower()
                if any(x in t1_name for x in ['tbd', 'winner', 'loser']) or any(x in t2_name for x in ['tbd', 'winner', 'loser']):
                    continue
                match_id = match['match_id']
                t1_players = self.upcoming_players[(self.upcoming_players['match_id'] == match_id) & (self.upcoming_players['team_id'] == match['team1_id'])]['player_id'].tolist()
                t2_players = self.upcoming_players[(self.upcoming_players['match_id'] == match_id) & (self.upcoming_players['team_id'] == match['team2_id'])]['player_id'].tolist()
                for map_name in map_names:
                    h1 = self.maps[(self.maps['map_name'] == map_name) & ((self.maps['match_id'].isin(self.matches[self.matches['team1_id'] == match['team1_id']]['match_id'])) | (self.maps['match_id'].isin(self.matches[self.matches['team2_id'] == match['team1_id']]['match_id'])))]
                    h2 = self.maps[(self.maps['map_name'] == map_name) & ((self.maps['match_id'].isin(self.matches[self.matches['team1_id'] == match['team2_id']]['match_id'])) | (self.maps['match_id'].isin(self.matches[self.matches['team2_id'] == match['team2_id']]['match_id'])))]
                    t1_map_stats = h1.select_dtypes(include=[np.number]).mean().add_prefix('t1_map_mean_').to_dict()
                    t2_map_stats = h2.select_dtypes(include=[np.number]).mean().add_prefix('t2_map_mean_').to_dict()
                    feats = self.get_common_features(match, t1_players, t2_players)
                    feats.update(t1_map_stats)
                    feats.update(t2_map_stats)
                    for col in feature_list:
                        if col not in feats:
                            feats[col] = 0
                    feats_df = pd.DataFrame([feats])[feature_list]
                    feats_df = feats_df.astype(float)
                    team1_score = float(self.model[0].predict(feats_df)[0])
                    team2_score = float(self.model[1].predict(feats_df)[0])
                    team1_score_final, team2_score_final = self.postprocess_map_score(team1_score, team2_score, max_score=13)
                    save_features_json(f"{match_id}_{map_name}", feats_df.iloc[0].to_dict(), map_name=map_name)
                    # Удаляем старый прогноз для этой пары (match_id, map_name)
                    conn.execute('DELETE FROM predict_map WHERE match_id = ? AND map_name = ?', (match_id, map_name))
                    conn.execute('''INSERT INTO predict_map (match_id, map_name, team1_rounds, team2_rounds, team1_rounds_final, team2_rounds_final, model_version, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                 (match_id, map_name, team1_score, team2_score, team1_score_final, team2_score_final, self.model_version, datetime.now().isoformat()))
                    # --- Уникальные признаки по карте для каждой команды ---
                    t1_map = self.maps[(self.maps['team1_id'] == match['team1_id']) & (self.maps['map_name'] == map_name)]
                    t2_map = self.maps[(self.maps['team2_id'] == match['team2_id']) & (self.maps['map_name'] == map_name)]
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
                    save_features_json(f"{match_id}_{map_name}", feats_df.iloc[0].to_dict(), map_name=map_name)
                    # Удаляем старый прогноз для этой пары (match_id, map_name)
                    conn.execute('DELETE FROM predict_map WHERE match_id = ? AND map_name = ?', (match_id, map_name))
                    conn.execute('''INSERT INTO predict_map (match_id, map_name, team1_rounds, team2_rounds, team1_rounds_final, team2_rounds_final, model_version, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                 (match_id, map_name, team1_score, team2_score, team1_score_final, team2_score_final, self.model_version, datetime.now().isoformat()))
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
                results.append({
                    'match_id': match_id,
                    'map_name': map_name,
                    'team1_pred': team1_pred,
                    'team2_pred': team2_pred,
                    'team1_real': team1_real,
                    'team2_real': team2_real
                })
        # Сохраняем в CSV
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