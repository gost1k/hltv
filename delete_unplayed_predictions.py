import sqlite3

DB_PATH = 'hltv.db'

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Удаляем из predict только те матчи, которых нет ни в result_match, ни в upcoming_match
    cur.execute('''
        DELETE FROM predict
        WHERE match_id NOT IN (
            SELECT match_id FROM result_match
            UNION
            SELECT match_id FROM upcoming_match
        )
    ''')
    print(f"Удалено предсказаний матчей: {cur.rowcount}")
    # Удаляем из predict_map все карты, которых нет в result_match_maps
    cur.execute('''
        DELETE FROM predict_map
        WHERE (match_id, map_name) NOT IN (
            SELECT match_id, map_name FROM result_match_maps
        )
    ''')
    print(f"Удалено предсказаний карт: {cur.rowcount}")
    conn.commit()
    conn.close()
    print("Готово!")

if __name__ == "__main__":
    main() 