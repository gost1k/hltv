import os
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

class MatchesCollector:
    def __init__(self, html_dir: str = "storage/html", db_path: str = "hltv.db"):
        self.html_dir = html_dir
        self.db_path = db_path
        
    def _get_match_id(self, url: str) -> int:
        """Извлекает ID матча из URL"""
        match = re.search(r'/matches/(\d+)/', url)
        if match:
            return int(match.group(1))
        return None
        
    def _parse_matches_file(self, soup) -> list:
        """Парсит страницу предстоящих матчей"""
        matches = []
        
        # Находим основной контент
        main_content = soup.select_one('.mainContent')
        if not main_content:
            return matches
            
        # Находим все матчи
        match_elements = main_content.select('.match')
        for match in match_elements:
            try:
                # Получаем ссылку на матч
                match_link = match.select_one('a')
                if not match_link:
                    continue
                    
                url = match_link.get('href', '')
                match_id = self._get_match_id(url)
                
                if not match_id:
                    continue
                
                # Получаем время матча
                time_element = match.select_one('.match-time')
                if not time_element:
                    continue
                    
                unix_time = time_element.get('data-unix', '')
                if not unix_time:
                    continue
                
                # Формируем данные матча
                match_data = {
                    'id': match_id,
                    'url': url,
                    'date': int(unix_time),
                    'toParse': 1
                }
                matches.append(match_data)
                
            except Exception as e:
                logger.error(f"Ошибка при парсинге матча: {str(e)}")
                continue
                
        return matches

    def _parse_results_file(self, soup) -> list:
        """Парсит страницу результатов"""
        matches = []
        
        # Находим основной контент
        results_content = soup.select_one('.results')
        if not results_content:
            return matches
            
        # Находим все матчи
        result_elements = results_content.select('.result-con')
        for result in result_elements:
            try:
                # Получаем ссылку на матч
                match_link = result.select_one('a.a-reset')
                if not match_link:
                    continue
                    
                url = match_link.get('href', '')
                match_id = self._get_match_id(url)
                
                if not match_id:
                    continue
                
                # Формируем данные матча
                match_data = {
                    'id': match_id,
                    'url': url,
                    'toParse': 1  # Для результатов ставим флаг необходимости парсинга
                }
                matches.append(match_data)
                
            except Exception as e:
                logger.error(f"Ошибка при парсинге результата: {str(e)}")
                continue
                
        return matches

    def _parse_html_file(self, file_path: str) -> list:
        """Парсит HTML файл и возвращает список матчей"""
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')

        # Определяем тип страницы по имени файла
        if 'matches' in file_path.lower():
            return self._parse_matches_file(soup)
        elif 'results' in file_path.lower():
            return self._parse_results_file(soup)
        return []
        
    def _create_tables(self):
        """Создает необходимые таблицы в базе данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Создаем таблицу для предстоящих матчей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_upcoming (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    date INTEGER NOT NULL,
                    toParse INTEGER NOT NULL DEFAULT 1
                )
            ''')
            
            # Создаем таблицу для результатов матчей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_result (
                    id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    toParse INTEGER NOT NULL DEFAULT 1
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Таблицы успешно созданы")
            
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц: {str(e)}")
    
    def _save_upcoming_matches(self, matches: list) -> int:
        """Сохраняет предстоящие матчи в базу данных"""
        if not matches:
            return 0
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_matches = 0
        updated_matches = 0
        
        for match in matches:
            # Проверяем, существует ли матч в таблице предстоящих матчей
            cursor.execute('SELECT 1 FROM url_upcoming WHERE id = ?', (match['id'],))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Обновляем существующий матч
                cursor.execute('''
                    UPDATE url_upcoming 
                    SET date = ?, toParse = ?
                    WHERE id = ?
                ''', (match['date'], match['toParse'], match['id']))
                updated_matches += 1
            else:
                # Добавляем новый матч
                cursor.execute('''
                    INSERT INTO url_upcoming (id, url, date, toParse)
                    VALUES (?, ?, ?, ?)
                ''', (match['id'], match['url'], match['date'], match['toParse']))
                new_matches += 1
                
            # Проверяем, не переместился ли матч из результатов в предстоящие (очень маловероятно, но возможно)
            cursor.execute('DELETE FROM url_result WHERE id = ?', (match['id'],))
        
        conn.commit()
        conn.close()
        logger.info(f"Добавлено новых предстоящих матчей: {new_matches}, обновлено: {updated_matches}")
        return new_matches
        
    def _save_past_matches(self, matches: list) -> int:
        """Сохраняет результаты матчей в базу данных"""
        if not matches:
            return 0
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_matches = 0
        updated_matches = 0
        
        for match in matches:
            # Проверяем, существует ли матч в таблице результатов
            cursor.execute('SELECT 1 FROM url_result WHERE id = ?', (match['id'],))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Обновляем существующий матч
                cursor.execute('''
                    UPDATE url_result 
                    SET toParse = ?
                    WHERE id = ?
                ''', (match['toParse'], match['id']))
                updated_matches += 1
            else:
                # Добавляем новый матч
                cursor.execute('''
                    INSERT INTO url_result (id, url, toParse)
                    VALUES (?, ?, ?)
                ''', (match['id'], match['url'], match['toParse']))
                new_matches += 1
                
            # Удаляем матч из предстоящих, если он перешел в результаты
            cursor.execute('DELETE FROM url_upcoming WHERE id = ?', (match['id'],))
        
        conn.commit()
        conn.close()
        logger.info(f"Добавлено новых прошедших матчей: {new_matches}, обновлено: {updated_matches}")
        return new_matches
        
    def collect(self):
        """Собирает данные из всех HTML файлов в папке"""
        if not os.path.exists(self.html_dir):
            logger.error(f"Папка {self.html_dir} не найдена")
            return
            
        # Создаем таблицы, если их нет
        self._create_tables()
            
        # Собираем все HTML файлы matches и results
        for file_name in os.listdir(self.html_dir):
            if file_name.endswith('.html'):
                is_matches = 'matches' in file_name.lower()
                is_results = 'results' in file_name.lower()
                
                if is_matches or is_results:
                    file_path = os.path.join(self.html_dir, file_name)
                    file_type = 'matches' if is_matches else 'results'
                    logger.info(f"Обработка файла {file_type}: {file_name}")
                    
                    # Парсим файл
                    matches = self._parse_html_file(file_path)
                    logger.info(f"Найдено матчей: {len(matches)}")
                    
                    # Сохраняем в соответствующую таблицу
                    if matches:
                        if is_matches:
                            new_matches = self._save_upcoming_matches(matches)
                            logger.info(f"Обработано предстоящих матчей: {new_matches}")
                        else:
                            new_matches = self._save_past_matches(matches)
                            logger.info(f"Обработано прошедших матчей: {new_matches}")
                    
                    # Можно раскомментировать, если нужно удалять обработанные файлы
                    # os.remove(file_path)
                    # logger.info(f"Файл {file_name} удален")

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    collector = MatchesCollector()
    collector.collect()
    logger.info("Collection completed")
