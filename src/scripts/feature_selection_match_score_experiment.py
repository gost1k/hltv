import pandas as pd
import numpy as np
from lightgbm import LGBMRegressor, LGBMClassifier
from sklearn.metrics import mean_absolute_error, accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from src.scripts.predictor import Predictor
import sqlite3

# --- Честный feature engineering для матчей ---
def build_honest_match_features():
    predictor = Predictor()
    predictor.load_data()
    predictor.feature_engineering(for_train=True)
    # Загружаем матчи
    with sqlite3.connect('hltv.db') as conn:
        matches_df = pd.read_sql_query('SELECT * FROM result_match', conn)
    features = []
    for idx, row in matches_df.iterrows():
        match_id = row['match_id']
        # Получаем игроков
        t1_id = row['team1_id']
        t2_id = row['team2_id']
        t1_players = predictor.players_stats[(predictor.players_stats['match_id'] == match_id) & (predictor.players_stats['team_id'] == t1_id)]['player_id'].tolist()
        t2_players = predictor.players_stats[(predictor.players_stats['match_id'] == match_id) & (predictor.players_stats['team_id'] == t2_id)]['player_id'].tolist()
        match_series = row.copy()
        # Формируем признаки только по истории до этого матча
        feats = predictor.get_common_features(match_series, t1_players, t2_players)
        feats['team1_score'] = row['team1_score']
        feats['team2_score'] = row['team2_score']
        features.append(feats)
    df = pd.DataFrame(features)
    # Исключаем team1_score, team2_score, demo_id из признаков
    honest_match_features = [f for f in df.columns if f not in ['team1_score', 'team2_score', 'demo_id']]
    X = df[honest_match_features].dropna()
    return df, honest_match_features, X

# --- Формируем целевую переменную: обе команды выиграли хотя бы одну карту (bo3 не 2:0) ---
def build_target_both_teams_win():
    with sqlite3.connect('hltv.db') as conn:
        maps = pd.read_sql_query('SELECT * FROM result_match_maps', conn)
    def has_both_teams_win(group):
        t1_wins = (group['team1_rounds'] > group['team2_rounds']).sum()
        t2_wins = (group['team2_rounds'] > group['team1_rounds']).sum()
        return int(t1_wins > 0 and t2_wins > 0)
    target_df = maps.groupby('match_id').apply(has_both_teams_win).reset_index()
    target_df.columns = ['match_id', 'has_both_teams_win']
    return target_df

if __name__ == "__main__":
    # --- Честные признаки для матчей ---
    df, honest_match_features, X = build_honest_match_features()
    print('\nЧестные признаки для обучения по матчам:')
    for feat in honest_match_features:
        print(feat)

    # --- Целевая переменная: победитель матча ---
    df = df.dropna(subset=['team1_score', 'team2_score'])
    X = df[honest_match_features].dropna()
    y = (df.loc[X.index, 'team1_score'] > df.loc[X.index, 'team2_score']).astype(int)

    if len(X) > 0:
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
        clf = LGBMClassifier(n_estimators=200)
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_val)
        y_proba = clf.predict_proba(X_val)
        acc = accuracy_score(y_val, y_pred)
        roc_auc = roc_auc_score(y_val, y_proba[:, 1])
        print(f'Accuracy (win_match): {acc:.3f}')
        print(f'ROC-AUC: {roc_auc:.3f}')
        print('\n=== Сравнительная таблица (win_match) ===')
        print(f"{'Метрика':<12} | {'Значение':<8}")
        print('-'*24)
        print(f"{'Accuracy':<12} | {acc:.4f}")
        print(f"{'ROC-AUC':<12} | {roc_auc:.4f}")
        # Пример вероятностей для первых 10 матчей
        print('\nПримеры вероятностей победы team1/team2:')
        for i in range(min(10, len(y_proba))):
            print(f"team1: {y_proba[i,1]:.3f}  team2: {y_proba[i,0]:.3f}  (реальный winner: {y_val.iloc[i]})")
    else:
        print('Нет данных для обучения по задаче win_match!') 