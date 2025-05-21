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
    # Определяем счет проигравшей стороны для реальных и предсказанных данных (только для совпавших пар)
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
    # Исторические данные (все реальные карты/матчи, даже если нет предсказания)
    if 'map_name' in real_df.columns:
        # Для карт
        all_real_df = pd.read_sql_query('SELECT team1_rounds, team2_rounds FROM result_match_maps', sqlite3.connect(DB_PATH))
        loser_hist = []
        for idx, row in all_real_df.iterrows():
            t1 = row['team1_rounds']
            t2 = row['team2_rounds']
            if t1 == 13:
                loser_hist.append(t2)
            elif t2 == 13:
                loser_hist.append(t1)
    else:
        # Для матчей
        all_real_df = pd.read_sql_query('SELECT team1_score, team2_score FROM result_match', sqlite3.connect(DB_PATH))
        loser_hist = []
        for idx, row in all_real_df.iterrows():
            t1 = row['team1_score']
            t2 = row['team2_score']
            if t1 == 2:
                loser_hist.append(t2)
            elif t2 == 2:
                loser_hist.append(t1)
    # Гистограмма
    plt.figure(figsize=(10,5))
    bins = range(0, 14)
    real_hist = plt.hist(loser_real, bins=bins, alpha=0.5, label=f'{label_prefix}Реальные данные')
    pred_hist = plt.hist(loser_pred, bins=bins, alpha=0.5, label=f'{label_prefix}Предсказания')
    hist_hist = plt.hist(loser_hist, bins=bins, alpha=0.3, label=f'{label_prefix}Исторические данные', color='green')
    plt.xlabel('Раунды проигравшей стороны')
    plt.ylabel('Частота')
    plt.title(f'Сравнение распределения проигравших ({label_prefix.strip()})')
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    # Подписи к столбцам
    for hist, color in zip([real_hist, pred_hist, hist_hist], ['blue', 'orange', 'green']):
        for x, y in zip(hist[1][:-1], hist[0]):
            if y > 0:
                plt.text(x + 0.1, y + 0.2, f'{int(y)}', color=color, fontsize=9, ha='center')
    # Средние значения
    if loser_real:
        mean_real = np.mean(loser_real)
        plt.axvline(mean_real, color='blue', linestyle='--', linewidth=1.5, label=f'Среднее (реальные): {mean_real:.2f}')
        plt.text(mean_real + 0.2, plt.ylim()[1]*0.9, f'{mean_real:.2f}', color='blue', fontsize=10)
    if loser_pred:
        mean_pred = np.mean(loser_pred)
        plt.axvline(mean_pred, color='orange', linestyle='--', linewidth=1.5, label=f'Среднее (предсказания): {mean_pred:.2f}')
        plt.text(mean_pred + 0.2, plt.ylim()[1]*0.8, f'{mean_pred:.2f}', color='orange', fontsize=10)
    if loser_hist:
        mean_hist = np.mean(loser_hist)
        plt.axvline(mean_hist, color='green', linestyle='--', linewidth=1.5, label=f'Среднее (исторические): {mean_hist:.2f}')
        plt.text(mean_hist + 0.2, plt.ylim()[1]*0.7, f'{mean_hist:.2f}', color='green', fontsize=10)
    plt.tight_layout()
    plt.show()

def main():
    conn = sqlite3.connect(DB_PATH)
    # Загружаем данные для матчей
    pred = pd.read_sql_query('SELECT match_id, team1_score_final, team2_score_final FROM predict', conn)
    real = pd.read_sql_query('SELECT match_id, team1_score, team2_score, datetime FROM result_match', conn)
    names = pd.read_sql_query('SELECT match_id, team1_name, team2_name FROM result_match', conn)
    # Попробуем получить сырые предсказания, если есть
    try:
        pred_raw = pd.read_sql_query('SELECT match_id, team1_score, team2_score FROM predict', conn)
        pred_raw = pred_raw.rename(columns={'team1_score': 'team1_pred_raw', 'team2_score': 'team2_pred_raw'})
    except Exception:
        pred_raw = None
    # Загружаем данные для карт
    pred_map = pd.read_sql_query('SELECT match_id, map_name, team1_rounds_final, team2_rounds_final FROM predict_map', conn)
    real_map = pd.read_sql_query('SELECT match_id, map_name, team1_rounds, team2_rounds FROM result_match_maps', conn)
    # Попробуем получить сырые предсказания для карт
    try:
        pred_map_raw = pd.read_sql_query('SELECT match_id, map_name, team1_rounds, team2_rounds FROM predict_map', conn)
        pred_map_raw = pred_map_raw.rename(columns={'team1_rounds': 'team1_pred_raw', 'team2_rounds': 'team2_pred_raw'})
    except Exception:
        pred_map_raw = None
    conn.close()
    # Оставляем только те строки, где team1_score и team2_score не больше 2 (для матчей)
    real = real[(real['team1_score'] <= 2) & (real['team2_score'] <= 2)]
    # Объединяем по match_id (матчи)
    df = pd.merge(pred, real, on='match_id', how='inner')
    df = pd.merge(df, names, on='match_id', how='left')
    if pred_raw is not None:
        df = pd.merge(df, pred_raw, on='match_id', how='left')
    # Метки для победителя и точного счета
    df['winner_correct'] = ((df['team1_score'] == 2) == (df['team1_score_final'] == 2)).map({True: '✔', False: '✗'})
    df['score_exact'] = ((df['team1_score_final'] == df['team1_score']) & (df['team2_score_final'] == df['team2_score'])).map({True: '✔', False: '✗'})
    # Считаем метрики по всем данным
    mae_team1 = (df['team1_score_final'] - df['team1_score']).abs().mean()
    mae_team2 = (df['team2_score_final'] - df['team2_score']).abs().mean()
    exact_mask = (df['team1_score_final'] == df['team1_score']) & (df['team2_score_final'] == df['team2_score'])
    exact = exact_mask.mean()
    total_exact = exact_mask.sum()
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

    # Выводим все строки (совпадения и ошибки)
    print("\nВсе матчи:")
    print(f"{'Дата и время':<17} | {'match_id':<8} | {'Команды':<35} | {'Прогноз':<9} | {'Реальный':<9} | {'team1_pred_raw':<8} | {'team2_pred_raw':<8} | {'Победитель':<10} | {'Счет':<6}")
    print('-'*120)
    for _, row in df.iterrows():
        dt = datetime.fromtimestamp(row['datetime']).strftime('%d.%m.%Y %H:%M')
        match_id = str(row['match_id'])
        teams = f"{row['team1_name']} vs {row['team2_name']}"
        pred = f"{row['team1_score_final']}-{row['team2_score_final']}"
        real = f"{row['team1_score']}-{row['team2_score']}"
        t1_raw = row['team1_pred_raw'] if 'team1_pred_raw' in row else ''
        t2_raw = row['team2_pred_raw'] if 'team2_pred_raw' in row else ''
        print(f"{dt:<17} | {match_id:<8} | {teams:<35} | {pred:<9} | {real:<9} | {t1_raw!s:<8} | {t2_raw!s:<8} | {row['winner_correct']:<10} | {row['score_exact']:<6}")
    df.to_csv('all_matches.csv', index=False)

    # Аналитика для карт
    df_map = pd.merge(pred_map, real_map, on=['match_id', 'map_name'], how='inner', suffixes=('_pred', '_real'))
    if pred_map_raw is not None:
        df_map = pd.merge(df_map, pred_map_raw, on=['match_id', 'map_name'], how='left')
    df_map['winner_correct'] = ((df_map['team1_rounds'] == 13) == (df_map['team1_rounds_final'] == 13)).map({True: '✔', False: '✗'})
    df_map['score_exact'] = ((df_map['team1_rounds_final'] == df_map['team1_rounds']) & (df_map['team2_rounds_final'] == df_map['team2_rounds'])).map({True: '✔', False: '✗'})
    print("\nВсе карты:")
    print(f"{'match_id':<8} | {'map_name':<8} | {'Прогноз':<9} | {'Реальный':<9} | {'team1_pred_raw':<8} | {'team2_pred_raw':<8} | {'Победитель':<10} | {'Счет':<6}")
    print('-'*100)
    for _, row in df_map.iterrows():
        match_id = str(row['match_id'])
        map_name = row['map_name']
        pred = f"{row['team1_rounds_final']}-{row['team2_rounds_final']}"
        real = f"{row['team1_rounds']}-{row['team2_rounds']}"
        t1_raw = row['team1_pred_raw'] if 'team1_pred_raw' in row else ''
        t2_raw = row['team2_pred_raw'] if 'team2_pred_raw' in row else ''
        print(f"{match_id:<8} | {map_name:<8} | {pred:<9} | {real:<9} | {t1_raw!s:<8} | {t2_raw!s:<8} | {row['winner_correct']:<10} | {row['score_exact']:<6}")
    df_map.to_csv('all_maps.csv', index=False)

    # Графики распределения проигравших
    print("\nГрафики распределения проигравших по матчам:")
    analyze_loser_distribution(real, pred, ['team1_score', 'team2_score'], ['team1_score_final', 'team2_score_final'], label_prefix="Матчи ")
    print("\nГрафики распределения проигравших по картам:")
    analyze_loser_distribution(real_map, pred_map, ['team1_rounds', 'team2_rounds'], ['team1_rounds_final', 'team2_rounds_final'], label_prefix="Карты ")

if __name__ == "__main__":
    main() 