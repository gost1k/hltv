import sqlite3
import sys

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else 'hltv.db'

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS result_match_maps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    map_name TEXT NOT NULL,
    team1_rounds INTEGER,
    team2_rounds INTEGER,
    rounds TEXT
);
'''

def main():
    print(f"Создание таблицы result_match_maps в базе данных: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()
    conn.close()
    print("Таблица успешно создана (или уже существует).")

if __name__ == "__main__":
    main() 