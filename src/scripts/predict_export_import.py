import sys
import sqlite3
import pandas as pd

USAGE = """
Использование:
  python predict_export_import.py export <db_path> <csv_path>
  python predict_export_import.py import <db_path> <csv_path>

  export - экспортирует таблицу predict из <db_path> в <csv_path>
  import - импортирует <csv_path> в таблицу predict в <db_path>
"""

def export_predict(db_path, csv_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM predict", conn)
    df.to_csv(csv_path, index=False)
    conn.close()
    print(f"Экспортировано {len(df)} строк в {csv_path}")

def import_predict(db_path, csv_path):
    df = pd.read_csv(csv_path)
    conn = sqlite3.connect(db_path)
    # Проверяем, что таблица predict существует
    conn.execute("""
    CREATE TABLE IF NOT EXISTS predict (
        match_id INTEGER PRIMARY KEY,
        team1_score REAL,
        team2_score REAL,
        team1_score_final INTEGER,
        team2_score_final INTEGER,
        confidence REAL,
        model_version TEXT,
        last_updated TIMESTAMP
    )
    """)
    df.to_sql('predict', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Импортировано {len(df)} строк из {csv_path} в {db_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(USAGE)
        sys.exit(1)
    mode, db_path, csv_path = sys.argv[1:4]
    if mode == "export":
        export_predict(db_path, csv_path)
    elif mode == "import":
        import_predict(db_path, csv_path)
    else:
        print(USAGE)
        sys.exit(1) 