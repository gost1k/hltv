import os
import json
import logging
import sqlite3
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Константы
JSON_OUTPUT_DIR = "storage/json"
UPCOMING_MATCHES_JSON_FILE = os.path.join(JSON_OUTPUT_DIR, "upcoming_matches.json")
PAST_MATCHES_JSON_FILE = os.path.join(JSON_OUTPUT_DIR, "past_matches.json")
DATABASE_FILE = "hltv.db"

class MatchesLoader:
    """
    Класс для загрузки списков матчей из JSON в базу данных
    """
    def __init__(self, db_path=DATABASE_FILE):
        self.db_path = db_path
        
    def load_all(self):
        """
        Загружает все данные из JSON файлов в базу данных
        
        Returns:
            dict: Статистика загрузки
        """
        # Создаем таблицы, если их нет
        self._create_tables()
        
        stats = {
            'upcoming_matches_processed': 0,
            'upcoming_matches_success': 0,
            'upcoming_matches_error': 0,
            'past_matches_processed': 0,
            'past_matches_success': 0,
            'past_matches_error': 0
        }
        
        # Загружаем предстоящие матчи
        if os.path.exists(UPCOMING_MATCHES_JSON_FILE):
            try:
                stats['upcoming_matches_processed'] += 1
                self._load_upcoming_matches()
                stats['upcoming_matches_success'] += 1
                # Опционально можно удалить файл после обработки
                # os.remove(UPCOMING_MATCHES_JSON_FILE)
                # logger.info(f"Файл {os.path.basename(UPCOMING_MATCHES_JSON_FILE)} удален после обработки")
            except Exception as e:
                logger.error(f"Ошибка при загрузке предстоящих матчей: {str(e)}")
                stats['upcoming_matches_error'] += 1
        else:
            logger.info(f"Файл с предстоящими матчами не найден: {UPCOMING_MATCHES_JSON_FILE}")
        
        # Загружаем прошедшие матчи
        if os.path.exists(PAST_MATCHES_JSON_FILE):
            try:
                stats['past_matches_processed'] += 1
                self._load_past_matches()
                stats['past_matches_success'] += 1
                # Опционально можно удалить файл после обработки
                # os.remove(PAST_MATCHES_JSON_FILE)
                # logger.info(f"Файл {os.path.basename(PAST_MATCHES_JSON_FILE)} удален после обработки")
            except Exception as e:
                logger.error(f"Ошибка при загрузке прошедших матчей: {str(e)}")
                stats['past_matches_error'] += 1
        else:
            logger.info(f"Файл с прошедшими матчами не найден: {PAST_MATCHES_JSON_FILE}")
        
        return stats
    
    def _create_tables(self):
        """Создает необходимые таблицы в базе данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Создаем таблицу для предстоящих матчей, если она не существует
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_upcoming (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    date INTEGER NOT NULL,
                    toParse INTEGER NOT NULL DEFAULT 1
                )
            ''')
            
            # Создаем таблицу для результатов матчей, если она не существует
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_result (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    toParse INTEGER NOT NULL DEFAULT 1
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Таблицы успешно созданы/проверены")
            
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц: {str(e)}")
            raise
    
    def _load_upcoming_matches(self):
        """
        Загружает предстоящие матчи из JSON файла в базу данных
        """
        try:
            # Чтение файла
            with open(UPCOMING_MATCHES_JSON_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'matches' not in data or not data['matches']:
                logger.warning(f"В файле {UPCOMING_MATCHES_JSON_FILE} отсутствуют данные о предстоящих матчах")
                return
                
            matches = data['matches']
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Получаем список всех ID матчей в текущих данных
            current_match_ids = [match['id'] for match in matches]
            
            # Получаем список всех ID в базе данных
            cursor.execute('SELECT id FROM url_upcoming')
            db_match_ids = [row[0] for row in cursor.fetchall()]
            
            # Находим ID матчей, которые есть в БД, но отсутствуют в текущих данных
            obsolete_ids = [match_id for match_id in db_match_ids if match_id not in current_match_ids]
            
            # Удаляем устаревшие матчи
            deleted_count = 0
            if obsolete_ids:
                placeholders = ','.join(['?'] * len(obsolete_ids))
                delete_query = f'DELETE FROM url_upcoming WHERE id IN ({placeholders})'
                cursor.execute(delete_query, obsolete_ids)
                deleted_count = cursor.rowcount
                logger.info(f"Удалено {deleted_count} устаревших матчей из таблицы url_upcoming")
            
            # Обрабатываем каждый матч
            new_count = 0
            updated_count = 0
            
            for match in matches:
                # Проверяем, существует ли матч в базе данных
                cursor.execute('SELECT toParse FROM url_upcoming WHERE id = ?', (match['id'],))
                result = cursor.fetchone()
                
                if result is not None:
                    # Обновляем существующий матч (сохраняем текущее значение toParse)
                    to_parse = result[0]
                    cursor.execute('''
                        UPDATE url_upcoming SET date = ?, toParse = ?
                        WHERE id = ?
                    ''', (match['date'], to_parse, match['id']))
                    updated_count += 1
                else:
                    # Добавляем новый матч
                    to_parse = match.get('toParse', 1)  # Используем значение из JSON или 1 по умолчанию
                    cursor.execute('''
                        INSERT INTO url_upcoming (id, url, date, toParse)
                        VALUES (?, ?, ?, ?)
                    ''', (match['id'], match['url'], match['date'], to_parse))
                    new_count += 1
                
                # Удаляем дублирующиеся записи из таблицы результатов (если матч переместился)
                cursor.execute('DELETE FROM url_result WHERE id = ?', (match['id'],))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Загрузка предстоящих матчей завершена: новых - {new_count}, обновлено - {updated_count}, удалено - {deleted_count}")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке предстоящих матчей: {str(e)}")
            raise
    
    def _load_past_matches(self):
        """
        Загружает прошедшие матчи из JSON файла в базу данных
        """
        try:
            # Чтение файла
            with open(PAST_MATCHES_JSON_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'matches' not in data or not data['matches']:
                logger.warning(f"В файле {PAST_MATCHES_JSON_FILE} отсутствуют данные о прошедших матчах")
                return
                
            matches = data['matches']
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Обрабатываем каждый матч
            new_count = 0
            updated_count = 0
            
            for match in matches:
                # Проверяем, существует ли матч в базе данных
                cursor.execute('SELECT toParse FROM url_result WHERE id = ?', (match['id'],))
                result = cursor.fetchone()
                
                if result is not None:
                    # Матч уже существует, обновляем toParse если нужно
                    to_parse = result[0]
                    if to_parse != match.get('toParse', 1):
                        cursor.execute('''
                            UPDATE url_result SET toParse = ?
                            WHERE id = ?
                        ''', (match['toParse'], match['id']))
                    updated_count += 1
                else:
                    # Добавляем новый матч
                    to_parse = match.get('toParse', 1)  # Используем значение из JSON или 1 по умолчанию
                    cursor.execute('''
                        INSERT INTO url_result (id, url, toParse)
                        VALUES (?, ?, ?)
                    ''', (match['id'], match['url'], to_parse))
                    new_count += 1
                    
                    # Удаляем матч из предстоящих, если он перешел в результаты
                    cursor.execute('DELETE FROM url_upcoming WHERE id = ?', (match['id'],))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Загрузка прошедших матчей завершена: новых - {new_count}, обновлено - {updated_count}")
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке прошедших матчей: {str(e)}")
            raise

    def load_upcoming_matches_only(self):
        """
        Загружает только предстоящие матчи из JSON файла в базу данных
        
        Returns:
            dict: Статистика загрузки
        """
        # Создаем таблицы, если их нет
        self._create_tables()
        
        stats = {
            'processed': 0,
            'success': 0,
            'error': 0
        }
        
        # Загружаем предстоящие матчи
        if os.path.exists(UPCOMING_MATCHES_JSON_FILE):
            try:
                stats['processed'] += 1
                self._load_upcoming_matches()
                stats['success'] += 1
                logger.info(f"Файл {os.path.basename(UPCOMING_MATCHES_JSON_FILE)} успешно обработан")
            except Exception as e:
                logger.error(f"Ошибка при загрузке предстоящих матчей: {str(e)}")
                stats['error'] += 1
        else:
            logger.info(f"Файл с предстоящими матчами не найден: {UPCOMING_MATCHES_JSON_FILE}")
        
        return stats

    def load_past_matches_only(self):
        """
        Загружает только прошедшие матчи из JSON файла в базу данных
        
        Returns:
            dict: Статистика загрузки
        """
        # Создаем таблицы, если их нет
        self._create_tables()
        
        stats = {
            'processed': 0,
            'success': 0,
            'error': 0
        }
        
        # Загружаем прошедшие матчи
        if os.path.exists(PAST_MATCHES_JSON_FILE):
            try:
                stats['processed'] += 1
                self._load_past_matches()
                stats['success'] += 1
                logger.info(f"Файл {os.path.basename(PAST_MATCHES_JSON_FILE)} успешно обработан")
            except Exception as e:
                logger.error(f"Ошибка при загрузке прошедших матчей: {str(e)}")
                stats['error'] += 1
        else:
            logger.info(f"Файл с прошедшими матчами не найден: {PAST_MATCHES_JSON_FILE}")
        
        return stats

if __name__ == "__main__":
    loader = MatchesLoader()
    stats = loader.load_all()
    
    # Вывод статистики
    logger.info("======== Загрузка предстоящих матчей ========")
    logger.info(f"Обработано файлов: {stats['upcoming_matches_processed']}")
    logger.info(f"Успешно загружено: {stats['upcoming_matches_success']}")
    logger.info(f"Ошибок: {stats['upcoming_matches_error']}")
    
    logger.info("======== Загрузка прошедших матчей ========")
    logger.info(f"Обработано файлов: {stats['past_matches_processed']}")
    logger.info(f"Успешно загружено: {stats['past_matches_success']}")
    logger.info(f"Ошибок: {stats['past_matches_error']}") 