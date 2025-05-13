"""
Скрипт для проверки структуры базы данных
"""
import sqlite3
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def verify_database(db_path="hltv.db"):
    """Проверяет структуру базы данных"""
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Найдены таблицы: {', '.join(tables)}")
        
        # Проверяем наличие новых таблиц
        expected_tables = ['url_upcoming', 'url_result']
        missing_tables = [table for table in expected_tables if table not in tables]
        
        if missing_tables:
            logger.error(f"Отсутствуют требуемые таблицы: {', '.join(missing_tables)}")
        else:
            logger.info("Все требуемые таблицы существуют")
        
        # Получаем статистику по таблицам
        for table in expected_tables:
            if table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"Таблица {table} содержит {count} записей")
        
        # Проверяем отсутствие старых таблиц
        old_tables = ['upcoming_matches', 'past_matches', 'matches', 'results']
        remaining_old_tables = [table for table in old_tables if table in tables]
        
        if remaining_old_tables:
            logger.warning(f"Найдены старые таблицы, которые должны быть удалены: {', '.join(remaining_old_tables)}")
        else:
            logger.info("Все старые таблицы успешно удалены")
            
        # Закрываем соединение
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка при проверке базы данных: {str(e)}")

if __name__ == "__main__":
    logger.info("Проверка структуры базы данных")
    verify_database()
    logger.info("Проверка завершена") 