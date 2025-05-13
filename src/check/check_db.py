"""
Скрипт для проверки состояния базы данных HLTV Parser
Отображает содержимое всех таблиц и примеры записей для анализа
"""
import sqlite3
import os
import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь для импортов
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Подключаемся к базе данных
db_path = os.path.join(project_root, 'hltv.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def main():
    # Получаем список таблиц
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
    tables = cursor.fetchall()

    print(f'Проверка базы данных: {db_path}')
    print('=' * 50)
    print('Таблицы в базе данных:')
    for table in tables:
        print(f"\n- {table[0]}")
        
        # Получаем количество записей в таблице
        cursor.execute(f'SELECT COUNT(*) FROM {table[0]}')
        count = cursor.fetchone()[0]
        print(f"  Количество записей: {count}")
        
        # Получаем первые 3 записи из таблицы
        try:
            cursor.execute(f'SELECT * FROM {table[0]} LIMIT 3')
            columns = [description[0] for description in cursor.description]
            print(f"  Столбцы: {columns}")
            
            rows = cursor.fetchall()
            if rows:
                print("  Первые 3 записи:")
                for row in rows:
                    print(f"    {row}")
            else:
                print("  Таблица пуста.")
        except sqlite3.Error as e:
            print(f"  Ошибка при чтении данных: {e}")
        
        print('-' * 40)

    # Закрываем соединение
    conn.close()
    
if __name__ == "__main__":
    main()
    print("\nПроверка базы данных завершена.") 