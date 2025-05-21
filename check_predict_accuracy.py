import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
import os

DB_PATH = 'hltv.db'

def analyze_loser_distribution(real_df, pred_df, real_score_cols, pred_score_cols, label_prefix=""):
    # Определяем счет проигравшей стороны для реальных и предсказанных данных
    loser_real = []
    loser_pred = []
    for idx, row in real_df.iterrows():
        t1 = row[real_score_cols[0]]
        t2 = row[real_score_cols[1]]
        if t1 == 13:
            loser_real.append(t2)
        elif t2 == 13:
            loser_real.append(t1)
    for idx, row in pred_df.iterrows():
        t1 = row[pred_score_cols[0]]
        t2 = row[pred_score_cols[1]]
        if t1 == 13:
            loser_pred.append(t2)
        elif t2 == 13:
            loser_pred.append(t1)
    # Гистограмма
    plt.figure(figsize=(8,4))
    plt.hist(loser_real, bins=range(0,14), alpha=0.5, label=f'{label_prefix}Реальные данные')
    plt.hist(loser_pred, bins=range(0,14), alpha=0.5, label=f'{label_prefix}Предсказания')
    plt.xlabel('Раунды проигравшей стороны')
    plt.ylabel('Частота')
    plt.title(f'Сравнение распределения проигравших ({label_prefix.strip()})')
    plt.legend()
    plt.tight_layout()
    plt.show()

def main():
    conn = sqlite3.connect(DB_PATH)
    # Загружаем данные для матчей
    pred = pd.read_sql_query('SELECT match_id, team1_score_final, team2_score_final FROM predict', conn)
    real = pd.read_sql_query('SELECT match_id, team1_score, team2_score, datetime FROM result_match', conn)
    names = pd.read_sql_query('SELECT match_id, team1_name, team2_name FROM result_match', conn)
    # Загружаем данные для карт
    pred_map = pd.read_sql_query('SELECT match_id, map_name, team1_rounds_final, team2_rounds_final FROM predict_map', conn)
    real_map = pd.read_sql_query('SELECT match_id, map_name, team1_rounds, team2_rounds FROM result_match_maps', conn)
    conn.close()
    # Оставляем только те строки, где team1_score и team2_score не больше 2 (для матчей)
    real = real[(real['team1_score'] <= 2) & (real['team2_score'] <= 2)]
    # Объединяем по match_id (матчи)
    df = pd.merge(pred, real, on='match_id', how='inner')
    df = pd.merge(df, names, on='match_id', how='left')
    # Считаем метрики по всем данным
    mae_team1 = (df['team1_score_final'] - df['team1_score']).abs().mean()
    mae_team2 = (df['team2_score_final'] - df['team2_score']).abs().mean()
    exact_mask = (df['team1_score_final'] == df['team1_score']) & (df['team2_score_final'] == df['team2_score'])
    exact = exact_mask.mean()
    total_exact = exact_mask.sum()
    # Процент угадывания победителя (у кого 2)
    real_winner = (df['team1_score'] == 2).astype(int) - (df['team2_score'] == 2).astype(int)
    pred_winner = (df['team1_score_final'] == 2).astype(int) - (df['team2_score_final'] == 2).astype(int)
    winner_correct = (real_winner == pred_winner).mean()
    print(f"Всего матчей: {len(df)}")
    print(f"MAE team1: {mae_team1:.3f}")
    print(f"MAE team2: {mae_team2:.3f}")
    print(f"Точное совпадение счёта: {exact:.2%} (всего: {total_exact})")
    print(f"Успешное угадывание победителя: {winner_correct:.2%}")
    # За последнюю неделю
    one_week_ago = int((datetime.now() - timedelta(days=7)).timestamp())
    df_week = df[df['datetime'] > one_week_ago]
    if not df_week.empty:
        mae_team1_w = (df_week['team1_score_final'] - df_week['team1_score']).abs().mean()
        mae_team2_w = (df_week['team2_score_final'] - df_week['team2_score']).abs().mean()
        exact_mask_w = (df_week['team1_score_final'] == df_week['team1_score']) & (df_week['team2_score_final'] == df_week['team2_score'])
        exact_w = exact_mask_w.mean()
        total_exact_w = exact_mask_w.sum()
        real_winner_w = (df_week['team1_score'] == 2).astype(int) - (df_week['team2_score'] == 2).astype(int)
        pred_winner_w = (df_week['team1_score_final'] == 2).astype(int) - (df_week['team2_score_final'] == 2).astype(int)
        winner_correct_w = (real_winner_w == pred_winner_w).mean()
        print(f"\nЗа последнюю неделю:")
        print(f"Матчей: {len(df_week)}")
        print(f"MAE team1: {mae_team1_w:.3f}")
        print(f"MAE team2: {mae_team2_w:.3f}")
        print(f"Точное совпадение счёта: {exact_w:.2%} (всего: {total_exact_w})")
        print(f"Успешное угадывание победителя: {winner_correct_w:.2%}")
    else:
        print("\nЗа последнюю неделю матчей нет.")

    # Ошибки по матчам (все)
    errors = df[(df['team1_score_final'] != df['team1_score']) | (df['team2_score_final'] != df['team2_score'])]
    print("\nОшибки по матчам (все):")
    print(f"{'Дата и время':<17} | {'match_id':<8} | {'Команды':<35} | {'Прогноз':<9} | {'Реальный':<9}")
    print('-'*90)
    for _, row in errors.iterrows():
        dt = datetime.fromtimestamp(row['datetime']).strftime('%d.%m.%Y %H:%M')
        match_id = str(row['match_id'])
        teams = f"{row['team1_name']} vs {row['team2_name']}"
        pred = f"{row['team1_score_final']}-{row['team2_score_final']}"
        real = f"{row['team1_score']}-{row['team2_score']}"
        print(f"{dt:<17} | {match_id:<8} | {teams:<35} | {pred:<9} | {real:<9}")
    errors.to_csv('errors_matches.csv', index=False)

    # Аналитика для карт
    df_map = pd.merge(pred_map, real_map, on=['match_id', 'map_name'], how='inner', suffixes=('_pred', '_real'))
    # Метрики для карт
    mae_team1_map = (df_map['team1_rounds_final'] - df_map['team1_rounds']).abs().mean()
    mae_team2_map = (df_map['team2_rounds_final'] - df_map['team2_rounds']).abs().mean()
    exact_mask_map = (df_map['team1_rounds_final'] == df_map['team1_rounds']) & (df_map['team2_rounds_final'] == df_map['team2_rounds'])
    exact_map = exact_mask_map.mean()
    total_exact_map = exact_mask_map.sum()
    print(f"\nАнализ по картам:")
    print(f"Всего карт: {len(df_map)}")
    print(f"MAE team1: {mae_team1_map:.3f}")
    print(f"MAE team2: {mae_team2_map:.3f}")
    print(f"Точное совпадение счёта: {exact_map:.2%} (всего: {total_exact_map})")
    # Ошибки по картам (все)
    errors_map = df_map[(df_map['team1_rounds_final'] != df_map['team1_rounds']) | (df_map['team2_rounds_final'] != df_map['team2_rounds'])]
    print("\nОшибки по картам (все):")
    print(f"{'match_id':<8} | {'map_name':<8} | {'Прогноз':<9} | {'Реальный':<9}")
    print('-'*45)
    for _, row in errors_map.iterrows():
        match_id = str(row['match_id'])
        map_name = row['map_name']
        pred = f"{row['team1_rounds_final']}-{row['team2_rounds_final']}"
        real = f"{row['team1_rounds']}-{row['team2_rounds']}"
        print(f"{match_id:<8} | {map_name:<8} | {pred:<9} | {real:<9}")
    errors_map.to_csv('errors_maps.csv', index=False)

    # Графики распределения проигравших
    print("\nГрафики распределения проигравших по матчам:")
    analyze_loser_distribution(real, pred, ['team1_score', 'team2_score'], ['team1_score_final', 'team2_score_final'], label_prefix="Матчи ")
    print("\nГрафики распределения проигравших по картам:")
    analyze_loser_distribution(real_map, pred_map, ['team1_rounds', 'team2_rounds'], ['team1_rounds_final', 'team2_rounds_final'], label_prefix="Карты ")

if __name__ == "__main__":
    main() 