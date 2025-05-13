"""
Скрипт для миграции базы данных HLTV Parser
Изменение структуры таблиц:
1. upcoming_matches -> url_upcoming
2. past_matches -> url_result
3. Удаление старых таблиц (matches, results)
"""
import sqlite3
import logging
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def backup_database(db_path):
    """Создает резервную копию базы данных перед миграцией"""
    backup_path = f"{db_path}.backup"
    try:
        if os.path.exists(db_path):
            import shutil
            shutil.copy2(db_path, backup_path)
            logger.info(f"Создана резервная копия базы данных: {backup_path}")
            return True
        else:
            logger.warning(f"База данных {db_path} не найдена, резервная копия не создана")
            return False
    except Exception as e:
        logger.error(f"Ошибка при создании резервной копии: {str(e)}")
        return False

def migrate_database(db_path="hltv.db"):
    """Выполняет миграцию базы данных на новую структуру"""
    try:
        # Создаем резервную копию базы данных
        if not backup_database(db_path):
            if not os.path.exists(db_path):
                logger.info(f"База данных {db_path} не существует, будет создана новая")
                
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем существование таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Найдены таблицы: {', '.join(tables)}")
        
        # Создаем новые таблицы
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS url_upcoming (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                date INTEGER NOT NULL,
                toParse INTEGER NOT NULL DEFAULT 1
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS url_result (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                toParse INTEGER NOT NULL DEFAULT 1
            )
        ''')
        logger.info("Созданы новые таблицы url_upcoming и url_result")
        
        # Переносим данные из upcoming_matches, если таблица существует
        if 'upcoming_matches' in tables:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO url_upcoming (id, url, date, toParse)
                    SELECT id, url, date, toParse FROM upcoming_matches
                ''')
                migrated_upcoming = cursor.rowcount
                logger.info(f"Перенесено {migrated_upcoming} записей из upcoming_matches в url_upcoming")
            except sqlite3.Error as e:
                logger.error(f"Ошибка при переносе данных из upcoming_matches: {str(e)}")
        
        # Переносим данные из past_matches, если таблица существует
        if 'past_matches' in tables:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO url_result (id, url, toParse)
                    SELECT id, url, toParse FROM past_matches
                ''')
                migrated_past = cursor.rowcount
                logger.info(f"Перенесено {migrated_past} записей из past_matches в url_result")
            except sqlite3.Error as e:
                logger.error(f"Ошибка при переносе данных из past_matches: {str(e)}")
        
        # Удаляем старые таблицы
        tables_to_drop = ['upcoming_matches', 'past_matches', 'matches', 'results']
        for table in tables_to_drop:
            if table in tables:
                try:
                    cursor.execute(f"DROP TABLE {table}")
                    logger.info(f"Удалена таблица {table}")
                except sqlite3.Error as e:
                    logger.error(f"Ошибка при удалении таблицы {table}: {str(e)}")
        
        # Подтверждаем транзакцию
        conn.commit()
        logger.info("Миграция базы данных успешно завершена")
        
        # Закрываем соединение
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при миграции базы данных: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Начало миграции базы данных HLTV Parser")
    success = migrate_database()
    if success:
        logger.info("Миграция успешно завершена")
    else:
        logger.error("Миграция завершилась с ошибками") 