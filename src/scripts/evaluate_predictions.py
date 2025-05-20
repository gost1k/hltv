import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import argparse
import os

LOG_PATH = 'logs/eval_predictions.log'
DB_PATH = 'hltv.db'

def evaluate(period='all'):
    with sqlite3.connect(DB_PATH) as conn:
        # Матчи
        matches = pd.read_sql_query('SELECT match_id, datetime, team1_score, team2_score FROM result_match', conn)
        preds = pd.read_sql_query('SELECT match_id, team1_score_final, team2_score_final FROM predict', conn)
        df = matches.merge(preds, on='match_id')
        # Фильтр по времени
        if period == 'week':
            one_week_ago = int((datetime.now() - timedelta(days=7)).timestamp())
            df = df[df['datetime'] > one_week_ago]
        # Метрики по матчам
        mae_team1 = (df['team1_score'] - df['team1_score_final']).abs().mean()
        mae_team2 = (df['team2_score'] - df['team2_score_final']).abs().mean()
        df['winner_real'] = (df['team1_score'] > df['team2_score']).astype(int)
        df['winner_pred'] = (df['team1_score_final'] > df['team2_score_final']).astype(int)
        accuracy = (df['winner_real'] == df['winner_pred']).mean()
        exact_score = ((df['team1_score'] == df['team1_score_final']) & (df['team2_score'] == df['team2_score_final'])).mean()
        # Карты
        maps_real = pd.read_sql_query('SELECT match_id, map_name, team1_rounds, team2_rounds FROM result_match_maps', conn)
        maps_pred = pd.read_sql_query('SELECT match_id, map_name, team1_rounds, team2_rounds, team1_rounds_final, team2_rounds_final FROM predict_map', conn)
        df_map = maps_real.merge(maps_pred, on=['match_id', 'map_name'])
        if period == 'week':
            match_ids = set(df['match_id'])
            df_map = df_map[df_map['match_id'].isin(match_ids)]
        mae_map_team1 = (df_map['team1_rounds'] - df_map['team1_rounds_final']).abs().mean()
        mae_map_team2 = (df_map['team2_rounds'] - df_map['team2_rounds_final']).abs().mean()
        exact_map_score = ((df_map['team1_rounds'] == df_map['team1_rounds_final']) & (df_map['team2_rounds'] == df_map['team2_rounds_final'])).mean()
    # Пишем в лог
    os.makedirs('logs', exist_ok=True)
    with open(LOG_PATH, 'a', encoding='utf-8') as f:
        f.write(f"\n=== Evaluation report ({period}) {datetime.now().isoformat()} ===\n")
        f.write(f"Matches: {len(df)}\n")
        f.write(f"MAE team1: {mae_team1:.3f}, MAE team2: {mae_team2:.3f}\n")
        f.write(f"Winner accuracy: {accuracy:.3%}\n")
        f.write(f"Exact score accuracy: {exact_score:.3%}\n")
        f.write(f"Maps: {len(df_map)}\n")
        f.write(f"MAE map team1: {mae_map_team1:.3f}, MAE map team2: {mae_map_team2:.3f}\n")
        f.write(f"Exact map score accuracy: {exact_map_score:.3%}\n")
        f.write("===============================\n")
    print(f"Evaluation complete. Results written to {LOG_PATH}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Evaluate prediction quality')
    parser.add_argument('--period', choices=['all', 'week'], default='all', help='Период анализа: all или week')
    args = parser.parse_args()
    evaluate(args.period) 