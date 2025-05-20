import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = 'hltv.db'

def main():
    conn = sqlite3.connect(DB_PATH)
    # Загружаем данные
    pred = pd.read_sql_query('SELECT match_id, team1_score_final, team2_score_final FROM predict', conn)
    real = pd.read_sql_query('SELECT match_id, team1_score, team2_score, datetime FROM result_match', conn)
    conn.close()
    # Оставляем только те строки, где team1_score и team2_score не больше 2
    real = real[(real['team1_score'] <= 2) & (real['team2_score'] <= 2)]
    # Объединяем по match_id
    df = pd.merge(pred, real, on='match_id', how='inner')
    # Считаем метрики по всем данным
    mae_team1 = (df['team1_score_final'] - df['team1_score']).abs().mean()
    mae_team2 = (df['team2_score_final'] - df['team2_score']).abs().mean()
    exact_mask = (df['team1_score_final'] == df['team1_score']) & (df['team2_score_final'] == df['team2_score'])
    exact = exact_mask.mean()
    total_exact = exact_mask.sum()
    print(f"Всего матчей: {len(df)}")
    print(f"MAE team1: {mae_team1:.3f}")
    print(f"MAE team2: {mae_team2:.3f}")
    print(f"Точное совпадение счёта: {exact:.2%} (всего: {total_exact})")
    # За последнюю неделю
    one_week_ago = int((datetime.now() - timedelta(days=7)).timestamp())
    df_week = df[df['datetime'] > one_week_ago]
    if not df_week.empty:
        mae_team1_w = (df_week['team1_score_final'] - df_week['team1_score']).abs().mean()
        mae_team2_w = (df_week['team2_score_final'] - df_week['team2_score']).abs().mean()
        exact_mask_w = (df_week['team1_score_final'] == df_week['team1_score']) & (df_week['team2_score_final'] == df_week['team2_score'])
        exact_w = exact_mask_w.mean()
        total_exact_w = exact_mask_w.sum()
        print(f"\nЗа последнюю неделю:")
        print(f"Матчей: {len(df_week)}")
        print(f"MAE team1: {mae_team1_w:.3f}")
        print(f"MAE team2: {mae_team2_w:.3f}")
        print(f"Точное совпадение счёта: {exact_w:.2%} (всего: {total_exact_w})")
    else:
        print("\nЗа последнюю неделю матчей нет.")
    # Можно вывести примеры ошибок
    print("\nОшибки (первые 10):")
    print(df[(df['team1_score_final'] != df['team1_score']) | (df['team2_score_final'] != df['team2_score'])].head(10))

if __name__ == "__main__":
    main() 