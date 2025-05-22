import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.table import Table
from datetime import datetime, timezone
import pytz
from colorama import Fore, Style

DB_PATH = 'hltv.db'

# Цвета для confidence (возвращает hex)
def conf_color(conf):
    if conf < 0.1:
        return '#ffb3b3'  # светло-красный
    elif conf < 0.15:
        return '#fff799'  # желтый
    elif conf < 0.2:
        return '#7fff7f'  # темно-зеленый
    else:
        return '#b3ffb3'  # светло-зеленый

def main():
    conn = sqlite3.connect(DB_PATH)
    # Матчи без результата
    matches = pd.read_sql_query('''
        SELECT p.match_id, p.team1_score_final, p.team2_score_final, p.confidence, u.datetime, u.team1_name, u.team2_name
        FROM predict p
        LEFT JOIN result_match r ON p.match_id = r.match_id
        LEFT JOIN result_match rm ON p.match_id = rm.match_id
        LEFT JOIN upcoming_match u ON p.match_id = u.match_id
        WHERE rm.match_id IS NULL
    ''', conn)
    try:
        pred_raw = pd.read_sql_query('SELECT match_id, team1_score, team2_score FROM predict', conn)
        pred_raw = pred_raw.rename(columns={'team1_score': 'team1_pred_raw', 'team2_score': 'team2_pred_raw'})
    except Exception:
        pred_raw = None
    maps = pd.read_sql_query('''
        SELECT pm.match_id, pm.map_name, pm.team1_rounds_final, pm.team2_rounds_final, pm.confidence
        FROM predict_map pm
        LEFT JOIN result_match_maps rm ON pm.match_id = rm.match_id AND pm.map_name = rm.map_name
        WHERE rm.match_id IS NULL
    ''', conn)
    try:
        maps_raw = pd.read_sql_query('SELECT match_id, map_name, team1_rounds, team2_rounds FROM predict_map', conn)
        maps_raw = maps_raw.rename(columns={'team1_rounds': 'team1_pred_raw', 'team2_rounds': 'team2_pred_raw'})
    except Exception:
        maps_raw = None
    conn.close()
    if pred_raw is not None:
        matches = matches.merge(pred_raw, on='match_id', how='left')
    if maps_raw is not None:
        maps = maps.merge(maps_raw, on=['match_id', 'map_name'], how='left')
    # 1. Убираем строки с confidence == 0
    matches = matches[matches['confidence'] > 0]
    maps = maps[maps['confidence'] > 0]
    # 2. Сортировка матчей по confidence (по убыванию)
    matches = matches.sort_values(['confidence', 'match_id'], ascending=[False, True])
    maps = maps.sort_values(['match_id', 'map_name'])
    grouped = maps.groupby('match_id')
    # Формируем строки для вывода
    rows = []
    for _, m in matches.iterrows():
        # 5. Время матча под командами
        dt = ''
        if not pd.isnull(m['datetime']):
            dt_utc = datetime.fromtimestamp(m['datetime'], tz=timezone.utc)
            msk = pytz.timezone('Europe/Moscow')
            dt = dt_utc.astimezone(msk).strftime('%d.%m.%Y %H:%M')
        teams = f"{m['team1_name']} vs {m['team2_name']}"
        pred = f"{m['team1_score_final']}-{m['team2_score_final']}"
        t1_raw = f"{m['team1_pred_raw']:.3f}" if pd.notnull(m['team1_pred_raw']) else ''
        t2_raw = f"{m['team2_pred_raw']:.3f}" if pd.notnull(m['team2_pred_raw']) else ''
        conf = m['confidence']
        # Строка с командами (font size больше)
        rows.append({'type': 'match', 'teams': teams, 'dt': dt, 'pred': pred, 't1_raw': t1_raw, 't2_raw': t2_raw, 'conf': conf, 'conf_color': conf_color(conf) if conf is not None else '#ffffff'})
        # Карты этого матча
        if m['match_id'] in grouped.groups:
            for _, row in grouped.get_group(m['match_id']).iterrows():
                pred_map = f"{row['team1_rounds_final']}-{row['team2_rounds_final']}"
                t1_map = f"{row['team1_pred_raw']:.3f}" if pd.notnull(row['team1_pred_raw']) else ''
                t2_map = f"{row['team2_pred_raw']:.3f}" if pd.notnull(row['team2_pred_raw']) else ''
                conf_map = row['confidence']
                rows.append({'type': 'map', 'map_name': row['map_name'], 'pred': pred_map, 't1_raw': t1_map, 't2_raw': t2_map, 'conf': conf_map, 'conf_color': conf_color(conf_map) if conf_map is not None else '#ffffff'})
        # Пустая строка-отступ
        rows.append({'type': 'sep'})
    # --- Вывод в консоль ---
    col_labels = ['Команды / Карта', 'Прогноз', 'team1_pred_raw', 'team2_pred_raw', 'conf']
    col_widths = [28, 10, 13, 13, 7]
    def print_row(row):
        print(" | ".join(f"{str(val):<{w}}" for val, w in zip(row, col_widths)))
    print("\n" + " | ".join(f"{label:<{w}}" for label, w in zip(col_labels, col_widths)))
    print("-" * (sum(col_widths) + 3*len(col_widths)))
    for row in rows:
        if row['type'] == 'match':
            print_row([row['teams'], row['pred'], row['t1_raw'], row['t2_raw'], f"{row['conf']:.3f}" if row['conf'] is not None else ''])
            print_row([row['dt'], '', '', '', ''])
        elif row['type'] == 'map':
            print_row([row['map_name'], row['pred'], row['t1_raw'], row['t2_raw'], f"{row['conf']:.3f}" if row['conf'] is not None else ''])
        elif row['type'] == 'dt':
            print_row([row['dt'], '', '', '', ''])
        elif row['type'] == 'sep':
            print()
    # Формируем таблицу для matplotlib
    fig, ax = plt.subplots(figsize=(7, max(6, len(rows)*0.35)))
    ax.axis('off')
    tbl = Table(ax, bbox=[0,0,1,1])
    nrows, ncols = len(rows)+1, len(col_labels)
    width = 1.0 / ncols
    height = 1.0 / (nrows * 3)
    # Заголовки
    for j, label in enumerate(col_labels):
        tbl.add_cell(0, j, width, height, text=label, loc='center', facecolor='#d0d0d0')
    # --- Формируем данные для PNG-таблицы ---
    table_data = []
    cell_colors = []
    cell_types = []
    for row in rows:
        if row['type'] == 'match':
            table_data.append([row['teams'], row['pred'], row['t1_raw'], row['t2_raw'], f"{row['conf']:.3f}" if row['conf'] is not None else ''])
            cell_colors.append(['#f0f0f0']*4 + [row['conf_color']])
            cell_types.append(['match', None, None, None, None])
            table_data.append([row['dt'], '', '', '', ''])
            cell_colors.append(['#ffffff']*5)
            cell_types.append(['dt', None, None, None, None])
        elif row['type'] == 'map':
            table_data.append([row['map_name'], row['pred'], row['t1_raw'], row['t2_raw'], f"{row['conf']:.3f}" if row['conf'] is not None else ''])
            cell_colors.append(['#ffffff']*4 + [row['conf_color']])
            cell_types.append([None, None, None, None, None])
        elif row['type'] == 'dt':
            table_data.append([row['dt'], '', '', '', ''])
            cell_colors.append(['#ffffff']*5)
            cell_types.append(['dt', None, None, None, None])
        elif row['type'] == 'sep':
            table_data.append(['']*5)
            cell_colors.append(['#ffffff']*5)
            cell_types.append([None]*5)
    for i, (row, colors, types) in enumerate(zip(table_data, cell_colors, cell_types)):
        for j, (val, color, typ) in enumerate(zip(row, colors, types)):
            cell = tbl.add_cell(i+1, j, width, height, text=val, loc='center', facecolor=color)
            if typ == 'match':
                cell.get_text().set_fontsize(14)
                cell.get_text().set_weight('bold')
            elif typ == 'dt':
                cell.get_text().set_fontsize(10)
    ax.add_table(tbl)
    plt.savefig('upcoming_predictions.png', bbox_inches='tight', dpi=200)
    print('Экспортировано в upcoming_predictions.png')

if __name__ == "__main__":
    main() 