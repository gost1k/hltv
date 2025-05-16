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

CREATE_TABLE_STREAMERS_SQL = '''
CREATE TABLE IF NOT EXISTS upcoming_match_streamers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    name TEXT,
    lang TEXT,
    url TEXT,
    FOREIGN KEY (match_id) REFERENCES upcoming_urls (id)
);
'''

def main():
    print(f"Создание таблицы result_match_maps и upcoming_match_streamers в базе данных: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)
    cursor.execute(CREATE_TABLE_STREAMERS_SQL)
    conn.commit()
    conn.close()
    print("Таблицы успешно созданы (или уже существуют).")

if __name__ == "__main__":
    main() 