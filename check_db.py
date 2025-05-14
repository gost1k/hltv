import sqlite3
import sys

def check_db_structure():
    try:
        conn = sqlite3.connect('hltv.db')
        cursor = conn.cursor()
        
        # Показать все таблицы
        print("Таблицы в базе данных:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for table in tables:
            print(f"- {table[0]}")
        
        # Проверить наличие таблицы match_upcoming
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='match_upcoming'")
        if cursor.fetchone():
            print("\nТаблица match_upcoming существует")
            
            # Показать структуру таблицы match_upcoming
            print("\nСтруктура таблицы match_upcoming:")
            cursor.execute("PRAGMA table_info(match_upcoming)")
            columns = cursor.fetchall()
            for col in columns:
                print(f"- {col[1]} ({col[2]})")
                
            # Добавить колонку parsed если ее нет
            cursor.execute("PRAGMA table_info(match_upcoming)")
            columns = [col[1] for col in cursor.fetchall()]
            if "parsed" not in columns:
                print("\nДобавляем колонку 'parsed'...")
                cursor.execute("ALTER TABLE match_upcoming ADD COLUMN parsed INTEGER DEFAULT 0")
                conn.commit()
                print("Колонка 'parsed' успешно добавлена")
            else:
                print("\nКолонка 'parsed' уже существует")
                
            # Обновляем тестовую запись
            print("\nОбновляем тестовую запись...")
            cursor.execute("UPDATE match_upcoming SET parsed = 1 WHERE rowid = 1")
            
            # Проверяем обновление
            cursor.execute("SELECT * FROM match_upcoming LIMIT 1")
            row = cursor.fetchone()
            if row:
                print(f"Первая запись в таблице: {row}")
        else:
            print("\nТаблица match_upcoming не существует")
            
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Ошибка SQLite: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Неожиданная ошибка: {e}", file=sys.stderr)

if __name__ == "__main__":
    check_db_structure() 