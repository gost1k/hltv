import sqlite3

DB_PATH = 'hltv.db'

def add_columns_if_not_exist(conn, table, columns):
    cursor = conn.cursor()
    for col, col_type in columns.items():
        cursor.execute(f"PRAGMA table_info({table})")
        if col not in [row[1] for row in cursor.fetchall()]:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Добавляем поля, если их нет
    add_columns_if_not_exist(conn, 'result_match_maps', {
        'team1_id': 'INTEGER',
        'team2_id': 'INTEGER'
    })

    # Получаем все match_id из upcoming_match_maps
    cursor.execute("SELECT match_id FROM result_match_maps")
    match_ids = [row[0] for row in cursor.fetchall()]

    for match_id in match_ids:
        # Получаем team1_id и team2_id из result_match
        cursor.execute("SELECT team1_id, team2_id FROM result_match WHERE match_id = ?", (match_id,))
        row = cursor.fetchone()
        if row:
            team1_id, team2_id = row
            # Обновляем upcoming_match_maps
            cursor.execute(
                "UPDATE result_match_maps SET team1_id = ?, team2_id = ? WHERE match_id = ?",
                (team1_id, team2_id, match_id)
            )

    conn.commit()
    conn.close()
    print("Готово!")

if __name__ == "__main__":
    main()