import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np
import os
import colorama
from colorama import Fore, Style
colorama.init(autoreset=True)

DB_PATH = 'hltv.db'

def analyze_loser_distribution(real_df, pred_df, real_score_cols, pred_score_cols, label_prefix=""):
    # Анализ проигравших (без графиков)
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
    if loser_real:
        mean_real = np.mean(loser_real)
        print(f"{label_prefix}Среднее проигравшего (реальные): {mean_real:.2f}")
    if loser_pred:
        mean_pred = np.mean(loser_pred)
        print(f"{label_prefix}Среднее проигравшего (предсказания): {mean_pred:.2f}")

def main():
    conn = sqlite3.connect(DB_PATH)
    pred = pd.read_sql_query('SELECT match_id, team1_score_final, team2_score_final, confidence FROM predict', conn)
    real = pd.read_sql_query('SELECT match_id, team1_score, team2_score, datetime FROM result_match', conn)
    names = pd.read_sql_query('SELECT match_id, team1_name, team2_name FROM result_match', conn)
    try:
        pred_raw = pd.read_sql_query('SELECT match_id, team1_score, team2_score FROM predict', conn)
        pred_raw = pred_raw.rename(columns={'team1_score': 'team1_pred_raw', 'team2_score': 'team2_pred_raw'})
    except Exception:
        pred_raw = None
    pred_map = pd.read_sql_query('SELECT match_id, map_name, team1_rounds_final, team2_rounds_final, confidence FROM predict_map', conn)
    real_map = pd.read_sql_query('SELECT match_id, map_name, team1_rounds, team2_rounds FROM result_match_maps', conn)
    try:
        pred_map_raw = pd.read_sql_query('SELECT match_id, map_name, team1_rounds, team2_rounds FROM predict_map', conn)
        pred_map_raw = pred_map_raw.rename(columns={'team1_rounds': 'team1_pred_raw', 'team2_rounds': 'team2_pred_raw'})
    except Exception:
        pred_map_raw = None
    conn.close()
    # Копии для вывода
    df_matches = pred.copy()
    df_matches = df_matches.merge(names, on='match_id', how='left')
    df_matches = df_matches.merge(real[['match_id', 'team1_score', 'team2_score', 'datetime']], on='match_id', how='left')
    if pred_raw is not None:
        df_matches = df_matches.merge(pred_raw, on='match_id', how='left')
    df_maps = pred_map.copy()
    df_maps = df_maps.merge(real_map[['match_id', 'map_name', 'team1_rounds', 'team2_rounds']], on=['match_id', 'map_name'], how='left')
    if pred_map_raw is not None:
        df_maps = df_maps.merge(pred_map_raw, on=['match_id', 'map_name'], how='left')
    conn = sqlite3.connect(DB_PATH)
    match_info = pd.read_sql_query('SELECT match_id, datetime, team1_name, team2_name FROM result_match', conn)
    conn.close()
    df_maps = df_maps.merge(match_info, on='match_id', how='left')
    df_matches = df_matches.sort_values('confidence', ascending=False)
    df_maps = df_maps.sort_values('confidence', ascending=False)
    # Оставляем только сыгранные матчи (есть результат)
    df_matches = df_matches[df_matches['team1_score'].notnull() & df_matches['team2_score'].notnull()]
    df_maps = df_maps[df_maps['team1_rounds'].notnull() & df_maps['team2_rounds'].notnull()]
    print("\n")  # двойной отступ перед таблицей матчей
    print(f"{'Дата и время':<17} | {'match_id':<8} | {'Команды':<35} | {'Прогноз':<9} | {'Реальный':<9} | {'team1_pred_raw':<12} | {'team2_pred_raw':<12} | {'conf':<8}")
    print('-'*120)
    for _, row in df_matches.iterrows():
        dt = datetime.fromtimestamp(row['datetime']).strftime('%d.%m.%Y %H:%M') if 'datetime' in row and not pd.isnull(row['datetime']) else ''
        match_id = str(row['match_id'])
        teams = f"{row['team1_name']} vs {row['team2_name']}"
        pred1, pred2 = row['team1_score_final'], row['team2_score_final']
        real1, real2 = int(row['team1_score']), int(row['team2_score'])
        pred = f"{pred1}-{pred2}"
        real = f"{real1}-{real2}"
        t1_raw = f"{row['team1_pred_raw']:.6f}" if 'team1_pred_raw' in row and pd.notnull(row['team1_pred_raw']) else ''
        t2_raw = f"{row['team2_pred_raw']:.6f}" if 'team2_pred_raw' in row and pd.notnull(row['team2_pred_raw']) else ''
        conf_val = row['confidence'] if 'confidence' in row and pd.notnull(row['confidence']) else None
        conf = f"{conf_val*100:.1f} %" if conf_val is not None else ''
        # Цвет для confidence (абсолютное значение, 3 знака после запятой)
        if conf_val is None:
            conf_col = ''
        elif conf_val < 0.1:
            conf_col = Fore.LIGHTRED_EX + f"{conf_val:.3f}" + Style.RESET_ALL
        elif conf_val < 0.15:
            conf_col = Fore.YELLOW + f"{conf_val:.3f}" + Style.RESET_ALL
        elif conf_val < 0.2:
            conf_col = Fore.GREEN + f"{conf_val:.3f}" + Style.RESET_ALL
        else:
            conf_col = Fore.LIGHTGREEN_EX + f"{conf_val:.3f}" + Style.RESET_ALL
        # Цвет для счета (матчи)
        exact = (pred1 == real1 and pred2 == real2)
        # Определяем победителя по картам
        if real1 > real2:
            real_winner = 1
        elif real2 > real1:
            real_winner = 2
        else:
            real_winner = 0
        if pred1 > pred2:
            pred_winner = 1
        elif pred2 > pred1:
            pred_winner = 2
        else:
            pred_winner = 0
        if exact:
            score_col = Fore.GREEN
        elif real_winner == pred_winner and real_winner != 0:
            score_col = Fore.YELLOW
        else:
            score_col = Fore.RED
        print(f"{dt:<17} | {match_id:<8} | {teams:<35} | {score_col}{pred:<9}{Style.RESET_ALL} | {score_col}{real:<9}{Style.RESET_ALL} | {t1_raw:<12} | {t2_raw:<12} | {conf_col:<8}")
    print("\n")  # двойной отступ перед таблицей карт
    print(f"{'Дата и время':<17} | {'match_id':<8} | {'map_name':<8} | {'Команды':<35} | {'Прогноз':<9} | {'Реальный':<9} | {'team1_pred_raw':<12} | {'team2_pred_raw':<12} | {'conf':<8}")
    print('-'*130)
    sep_printed = False
    for i, row in df_maps.iterrows():
        dt = datetime.fromtimestamp(row['datetime']).strftime('%d.%m.%Y %H:%M') if 'datetime' in row and not pd.isnull(row['datetime']) else ''
        match_id = str(row['match_id'])
        map_name = row['map_name']
        teams = f"{row['team1_name']} vs {row['team2_name']}"
        pred1, pred2 = row['team1_rounds_final'], row['team2_rounds_final']
        real1, real2 = int(row['team1_rounds']), int(row['team2_rounds'])
        pred = f"{pred1}-{pred2}"
        real = f"{real1}-{real2}"
        t1_raw = f"{row['team1_pred_raw']:.6f}" if 'team1_pred_raw' in row and pd.notnull(row['team1_pred_raw']) else ''
        t2_raw = f"{row['team2_pred_raw']:.6f}" if 'team2_pred_raw' in row and pd.notnull(row['team2_pred_raw']) else ''
        conf_val = row['confidence'] if 'confidence' in row and pd.notnull(row['confidence']) else None
        conf = f"{conf_val*100:.1f} %" if conf_val is not None else ''
        # Цвет для confidence (абсолютное значение, 3 знака после запятой)
        if conf_val is None:
            conf_col = ''
        elif conf_val < 0.1:
            conf_col = Fore.LIGHTRED_EX + f"{conf_val:.3f}" + Style.RESET_ALL
        elif conf_val < 0.15:
            conf_col = Fore.YELLOW + f"{conf_val:.3f}" + Style.RESET_ALL
        elif conf_val < 0.2:
            conf_col = Fore.GREEN + f"{conf_val:.3f}" + Style.RESET_ALL
        else:
            conf_col = Fore.LIGHTGREEN_EX + f"{conf_val:.3f}" + Style.RESET_ALL
        # Цвет для счета (карты)
        # Определяем победителя по раундам
        if real1 > real2:
            real_winner = 1
        elif real2 > real1:
            real_winner = 2
        else:
            real_winner = 0
        if pred1 > pred2:
            pred_winner = 1
        elif pred2 > pred1:
            pred_winner = 2
        else:
            pred_winner = 0
        if real_winner == pred_winner and real_winner != 0:
            if abs(pred1 - real1) <= 2 and abs(pred2 - real2) <= 2:
                score_col = Fore.GREEN
            else:
                score_col = Fore.YELLOW
        else:
            score_col = Fore.RED
        if not sep_printed and conf_val is not None and conf_val < 0.15:
            print('-'*130)
            sep_printed = True
        print(f"{dt:<17} | {match_id:<8} | {map_name:<8} | {teams:<35} | {score_col}{pred:<9}{Style.RESET_ALL} | {score_col}{real:<9}{Style.RESET_ALL} | {t1_raw:<12} | {t2_raw:<12} | {conf_col:<8}")

    # Сводная таблица по confidence
    # Бины и подписи
    bins = [0, 0.05, 0.1, 0.15, 0.2, 0.25, 1.0]
    columns = ["0-0.05", "0.05-0.1", "0.1-0.15", "0.15-0.2", "0.2-0.25", "0.25+"]
    # Для матчей
    df_matches['conf_bin'] = pd.cut(df_matches['confidence'], bins, labels=columns, include_lowest=True)
    # Для карт
    df_maps['conf_bin'] = pd.cut(df_maps['confidence'], bins, labels=columns, include_lowest=True)
    # Метки строк
    row_labels = [
        'Угадывание победителя матчи',
        'Угадывание победителя карты',
        'Точное угадывание счета матчи',
        'Точное угадывание счета карты'
    ]
    # mask для точного счета
    exact_match = (df_matches['team1_score_final'] == df_matches['team1_score']) & (df_matches['team2_score_final'] == df_matches['team2_score'])
    exact_map = (df_maps['team1_rounds_final'] == df_maps['team1_rounds']) & (df_maps['team2_rounds_final'] == df_maps['team2_rounds'])
    # mask для победителя
    match_winner = ((df_matches['team1_score_final'] > df_matches['team2_score_final']) & (df_matches['team1_score'] > df_matches['team2_score'])) | \
                   ((df_matches['team2_score_final'] > df_matches['team1_score_final']) & (df_matches['team2_score'] > df_matches['team1_score']))
    map_winner = ((df_maps['team1_rounds_final'] > df_maps['team2_rounds_final']) & (df_maps['team1_rounds'] > df_maps['team2_rounds'])) | \
                ((df_maps['team2_rounds_final'] > df_maps['team1_rounds_final']) & (df_maps['team2_rounds'] > df_maps['team1_rounds']))
    # Считаем проценты по каждому бину
    def percent_bin(df, mask):
        total = df.groupby('conf_bin', observed=False).size()
        correct = df[mask].groupby('conf_bin', observed=False).size()
        percent = (correct / total).reindex(columns, fill_value=0)
        return percent
    data = [
        percent_bin(df_matches, match_winner),
        percent_bin(df_maps, map_winner),
        percent_bin(df_matches, exact_match),
        percent_bin(df_maps, exact_map)
    ]
    # Универсальный шаблон для всей таблицы
    n_cols = 1 + len(columns)
    widths = [36] + [12]*len(columns)
    row_fmt = "|".join([f" {{:>{w}}} " for w in widths])
    sep_line = "+".join(["-"*w for w in widths])
    # Первая строка — заголовок + бины
    print("\n" + row_fmt.format('Сводная таблица по confidence:', *columns))
    print(sep_line)
    for label, row in zip(row_labels, data):
        cells = []
        for v in row:
            if pd.isna(v):
                cells.append('-')
            else:
                cells.append(f"{v*100:.1f} %")
        print(row_fmt.format(label, *cells))

if __name__ == "__main__":
    main() 