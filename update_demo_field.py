"""
Скрипт для обновления поля demo_url на demo_id в таблице match_details
"""
import sqlite3
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def update_match_details_table(db_path="hltv.db"):
    """
    Обновляет структуру таблицы match_details, заменяя поле demo_url на demo_id
    """
    try:
        # Подключаемся к БД
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем, существует ли таблица match_details
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='match_details'")
        if not cursor.fetchone():
            logger.error("Таблица match_details не существует")
            return False
        
        # Получаем текущие колонки таблицы
        cursor.execute("PRAGMA table_info(match_details)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        logger.info(f"Текущие колонки таблицы match_details: {column_names}")
        
        has_demo_url = 'demo_url' in column_names
        has_demo_id = 'demo_id' in column_names
        has_status = 'status' in column_names
        
        # Если уже есть поле demo_id, пропускаем обновление
        if has_demo_id:
            logger.info("Поле demo_id уже существует в таблице match_details")
            return True
        
        # Создаем транзакцию для безопасного обновления структуры
        cursor.execute("BEGIN TRANSACTION")
        
        # Если поле demo_url существует, переименовываем его
        if has_demo_url:
            logger.info("Переименование поля demo_url в demo_id...")
            
            # В SQLite нет прямого способа переименовать столбцы
            # Поэтому создаем новую таблицу и копируем данные
            
            # 1. Создаем строку для определения столбцов новой таблицы
            new_columns = []
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                if col_name == 'demo_url':
                    new_columns.append(f"demo_id {col_type}")
                else:
                    new_columns.append(f"{col_name} {col_type}")
                    
            # Если поля status нет, добавляем его
            if not has_status:
                new_columns.append("status TEXT")
                
            # Собираем SQL для создания новой таблицы
            create_table_sql = f"CREATE TABLE match_details_temp ({', '.join(new_columns)})"
            
            # 2. Создаем временную таблицу
            logger.info(f"Создание временной таблицы: {create_table_sql}")
            cursor.execute(create_table_sql)
            
            # 3. Формируем SQL для вставки данных
            select_columns = []
            for col in column_names:
                if col == 'demo_url':
                    select_columns.append("""
                        CASE 
                            WHEN demo_url IS NULL THEN NULL
                            WHEN demo_url = '' THEN NULL
                            ELSE CAST(SUBSTR(demo_url, INSTR(demo_url, '/demo/') + 6) AS INTEGER)
                        END as demo_id
                    """)
                else:
                    select_columns.append(col)
                    
            # Если поля status нет, добавляем его со значением по умолчанию
            if not has_status:
                select_columns.append("'completed' as status")
                
            # Копируем данные
            insert_sql = f"INSERT INTO match_details_temp SELECT {', '.join(select_columns)} FROM match_details"
            logger.info(f"Копирование данных: {insert_sql}")
            cursor.execute(insert_sql)
            
            # 4. Удаляем старую таблицу
            cursor.execute("DROP TABLE match_details")
            
            # 5. Переименовываем временную таблицу
            cursor.execute("ALTER TABLE match_details_temp RENAME TO match_details")
            
            # 6. Создаем индекс для match_id если он был
            if 'match_id' in column_names:
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_match_details_match_id ON match_details(match_id)")
            
            logger.info("Структура таблицы match_details успешно обновлена")
        else:
            # Если поле demo_url не существует, добавляем поле demo_id
            logger.info("Добавление поля demo_id в таблицу match_details...")
            cursor.execute("ALTER TABLE match_details ADD COLUMN demo_id INTEGER")
            
            # Если поля status нет, добавляем его
            if not has_status:
                logger.info("Добавление поля status в таблицу match_details...")
                cursor.execute("ALTER TABLE match_details ADD COLUMN status TEXT DEFAULT 'completed'")
                
            logger.info("Поля успешно добавлены в таблицу match_details")
        
        # Фиксируем изменения
        cursor.execute("COMMIT")
        conn.close()
        
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при обновлении структуры таблицы: {str(e)}")
        # Откатываем изменения в случае ошибки
        try:
            cursor.execute("ROLLBACK")
            conn.close()
        except:
            pass
        return False

if __name__ == "__main__":
    logger.info("Запуск обновления структуры базы данных")
    success = update_match_details_table()
    
    if success:
        logger.info("Обновление структуры базы данных успешно завершено")
    else:
        logger.error("Не удалось выполнить обновление структуры базы данных") 