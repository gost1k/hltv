import sqlite3
import re
import pandas as pd

DB_PATH = 'hltv.db'

def parse_rounds(rounds_str):
    # Пример: (9:3; 3:9) (4:2) или (10:2; 3:1) или (5:7; 7:5) (16:13)
    if not isinstance(rounds_str, str):
        return None, None, None, None, 0, 0
    # Ищем все скобки
    parts = re.findall(r'\(([^)]+)\)', rounds_str)
    if not parts:
        return None, None, None, None, 0, 0
    # Первая скобка — основное время
    halves = parts[0].split(';')
    if len(halves) == 2:
        h1 = halves[0].strip()
        h2 = halves[1].strip()
        m1 = re.match(r'(\d+):(\d+)', h1)
        m2 = re.match(r'(\d+):(\d+)', h2)
        if m1 and m2:
            half1_team1, half1_team2 = int(m1.group(1)), int(m1.group(2))
            half2_team1, half2_team2 = int(m2.group(1)), int(m2.group(2))
        else:
            half1_team1 = half1_team2 = half2_team1 = half2_team2 = None
    else:
        # Иногда только одна половина (например, короткая карта)
        m = re.match(r'(\d+):(\d+)', halves[0].strip())
        if m:
            half1_team1, half1_team2 = int(m.group(1)), int(m.group(2))
            half2_team1 = half2_team2 = None
        else:
            half1_team1 = half1_team2 = half2_team1 = half2_team2 = None
    # Овертаймы — все остальные скобки
    ot1, ot2 = 0, 0
    for ot in parts[1:]:
        m = re.match(r'(\d+):(\d+)', ot.strip())
        if m:
            ot1 += int(m.group(1))
            ot2 += int(m.group(2))
    return half1_team1, half1_team2, half2_team1, half2_team2, ot1, ot2

def main():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('SELECT * FROM result_match_maps', conn)
    # Добавляем колонки если их нет
    for col in ['half1_team1', 'half1_team2', 'half2_team1', 'half2_team2', 'ot_rounds_team1', 'ot_rounds_team2']:
        if col not in df.columns:
            conn.execute(f'ALTER TABLE result_match_maps ADD COLUMN {col} INTEGER')
    # Парсим и обновляем
    for idx, row in df.iterrows():
        h1t1, h1t2, h2t1, h2t2, ot1, ot2 = parse_rounds(row.get('rounds'))
        conn.execute('''UPDATE result_match_maps SET half1_team1=?, half1_team2=?, half2_team1=?, half2_team2=?, ot_rounds_team1=?, ot_rounds_team2=? WHERE id=?''',
            (h1t1, h1t2, h2t1, h2t2, ot1, ot2, row['id']))
    conn.commit()
    conn.close()
    print('Готово!')

if __name__ == '__main__':
    main() 