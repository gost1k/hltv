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
    # Можно вывести примеры ошибок
    print("\nОшибки (первые 10):")
    errors = df[(df['team1_score_final'] != df['team1_score']) | (df['team2_score_final'] != df['team2_score'])].head(10)
    # Заголовок с выравниванием
    print(f"{'Дата и время':<17} | {'match_id':<8} | {'Команды':<35} | {'Прогноз':<9} | {'Реальный':<9}")
    print('-'*90)
    for _, row in errors.iterrows():
        dt = datetime.fromtimestamp(row['datetime']).strftime('%d.%m.%Y %H:%M')
        match_id = str(row['match_id'])
        teams = f"{row['team1_name']} vs {row['team2_name']}"
        pred = f"{row['team1_score_final']}-{row['team2_score_final']}"
        real = f"{row['team1_score']}-{row['team2_score']}"
        print(f"{dt:<17} | {match_id:<8} | {teams:<35} | {pred:<9} | {real:<9}")
    # Аналитика для карт
    # Объединяем по match_id и map_name
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
    # Примеры ошибок по картам
    print("\nОшибки по картам (первые 10):")
    errors_map = df_map[(df_map['team1_rounds_final'] != df_map['team1_rounds']) | (df_map['team2_rounds_final'] != df_map['team2_rounds'])].head(10)
    print(f"{'match_id':<8} | {'map_name':<8} | {'Прогноз':<9} | {'Реальный':<9}")
    print('-'*45)
    for _, row in errors_map.iterrows():
        match_id = str(row['match_id'])
        map_name = row['map_name']
        pred = f"{row['team1_rounds_final']}-{row['team2_rounds_final']}"
        real = f"{row['team1_rounds']}-{row['team2_rounds']}"
        print(f"{match_id:<8} | {map_name:<8} | {pred:<9} | {real:<9}")
    # === Калибровка формулы перевода сырых предсказаний в счет ===
    print("\n=== Калибровка формулы перевода сырых предсказаний в счет (predict_map/result_match_maps) ===")
    if os.path.exists('predicted_past_maps.csv'):
        print('Использую predicted_past_maps.csv для калибровки...')
        df = pd.read_csv('predicted_past_maps.csv')
        print('Размерность df:', df.shape)
        print(df.head())
        if 'team1_pred' in df.columns and 'team2_pred' in df.columns and 'team1_real' in df.columns and 'team2_real' in df.columns:
            df['diff_pred'] = abs(df['team1_pred'] - df['team2_pred'])
            df['loser_real'] = df[['team1_real', 'team2_real']].min(axis=1)
            X = df[['diff_pred']]
            y = df['loser_real']
            if len(df) == 0:
                print('Нет данных для калибровки!')
            else:
                reg = LinearRegression().fit(X, y)
                a, b = reg.coef_[0], reg.intercept_
                print(f"Лучшая линейная формула: loser_score = {a:.2f} * diff_pred + {b:.2f}")
                df['loser_pred'] = a * df['diff_pred'] + b
                print("MAE по проигравшему (линейная формула):", mean_absolute_error(df['loser_real'], df['loser_pred']))
        else:
            print('В predicted_past_maps.csv не найдены нужные колонки!')
            print('Колонки:', list(df.columns))
    else:
        # Старый универсальный блок для merge predict_map и real_map
        df_map_calib = pd.merge(pred_map, real_map, on=['match_id', 'map_name'], how='inner', suffixes=('_pred', '_real'))
        real_cols = [col for col in df_map_calib.columns if (('real' in col or 'rounds' in col) and not 'pred' in col and col not in ['map_name', 'match_id'])]
        print('Колонки с реальным счетом:', real_cols)
        if len(real_cols) >= 2:
            df_map_calib['loser_real'] = df_map_calib[real_cols].min(axis=1)
        else:
            print('Не удалось найти две колонки с реальным счетом для loser_real!')
            print('Все колонки:', list(df_map_calib.columns))
            return
        if 'team1_rounds_final' in df_map_calib.columns and 'team2_rounds_final' in df_map_calib.columns:
            df_map_calib['diff_pred'] = abs(df_map_calib['team1_rounds_final'] - df_map_calib['team2_rounds_final'])
        elif 'team1_pred' in df_map_calib.columns and 'team2_pred' in df_map_calib.columns:
            df_map_calib['diff_pred'] = abs(df_map_calib['team1_pred'] - df_map_calib['team2_pred'])
        else:
            print('Не удалось найти колонки с сырыми предсказаниями для diff_pred!')
            print('Все колонки:', list(df_map_calib.columns))
            return
        X = df_map_calib[['diff_pred']]
        y = df_map_calib['loser_real']
        if len(df_map_calib) == 0:
            print('Нет данных для калибровки!')
        else:
            reg = LinearRegression().fit(X, y)
            a, b = reg.coef_[0], reg.intercept_
            print(f"Лучшая линейная формула: loser_score = {a:.2f} * diff_pred + {b:.2f}")
            df_map_calib['loser_pred'] = a * df_map_calib['diff_pred'] + b
            print("MAE по проигравшему (линейная формула):", mean_absolute_error(df_map_calib['loser_real'], df_map_calib['loser_pred']))
    # Графики распределения проигравших
    print("\nГрафики распределения проигравших по матчам:")
    analyze_loser_distribution(real, pred, ['team1_score', 'team2_score'], ['team1_score_final', 'team2_score_final'], label_prefix="Матчи ")
    print("\nГрафики распределения проигравших по картам:")
    analyze_loser_distribution(real_map, pred_map, ['team1_rounds', 'team2_rounds'], ['team1_rounds_final', 'team2_rounds_final'], label_prefix="Карты ")

if __name__ == "__main__":
    main() 