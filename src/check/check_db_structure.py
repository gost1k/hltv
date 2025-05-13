"""
Скрипт для проверки структуры базы данных
"""
import sqlite3
import os
import sys

def check_db(db_path="hltv.db"):
    """
    Проверка структуры базы данных
    """
    print(f"Проверка базы данных: {os.path.abspath(db_path)}")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print(f"Найдено таблиц: {len(tables)}")
        for table in tables:
            table_name = table[0]
            print(f"\nТаблица: {table_name}")
            
            # Получаем структуру таблицы
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print(f"Столбцы ({len(columns)}):")
            for column in columns:
                col_id, name, type_, not_null, default_value, pk = column
                print(f"  {name} ({type_})", end="")
                if pk:
                    print(" PRIMARY KEY", end="")
                if not_null:
                    print(" NOT NULL", end="")
                if default_value is not None:
                    print(f" DEFAULT {default_value}", end="")
                print()
            
            # Получаем количество записей
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"Количество записей: {count}")
            
            # Показываем пример данных
            if count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                print("Пример данных:")
                for row in rows:
                    print(f"  {row}")
            
            print("-" * 50)
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Ошибка при работе с базой данных: {e}")

if __name__ == "__main__":
    db_path = "hltv.db"
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    check_db(db_path) 