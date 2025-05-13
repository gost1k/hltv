import os
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re

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
                print(f"Ошибка при парсинге матча: {str(e)}")
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
                    'date': 0,  # Для результатов ставим 0
                    'toParse': 1
                }
                matches.append(match_data)
                
            except Exception as e:
                print(f"Ошибка при парсинге результата: {str(e)}")
                continue
                
        return matches

    def _parse_html_file(self, file_path: str) -> list:
        """Парсит HTML файл и возвращает список матчей"""
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')

        # Определяем тип страницы по имени файла
        if 'matches_' in file_path.lower():
            return self._parse_matches_file(soup)
        elif 'results_' in file_path.lower():
            return self._parse_results_file(soup)
        return []
                
        return matches
        
    def _check_existing_match(self, cursor, match_id: int) -> bool:
        """Проверяет существование матча в базе данных"""
        cursor.execute('SELECT 1 FROM matches WHERE id = ?', (match_id,))
        return cursor.fetchone() is not None  # Возвращает True если матч существует

    def _save_to_db(self, matches: list) -> int:
        """Сохраняет новые матчи в базу данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу, если её нет
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY,
                url TEXT,
                date INTEGER,
                toParse INTEGER
            )
        ''')
        
        new_matches = 0
        # Вставляем только новые матчи или те, которые ещё не обработаны
        for match in matches:
            if not self._check_existing_match(cursor, match['id']):
                cursor.execute('''
                    INSERT OR REPLACE INTO matches (id, url, date, toParse)
                    VALUES (?, ?, ?, ?)
                ''', (match['id'], match['url'], match['date'], match['toParse']))
                new_matches += 1
            
        conn.commit()
        conn.close()
        return new_matches
        
    def collect(self):
        """Собирает данные из всех HTML файлов в папке"""
        if not os.path.exists(self.html_dir):
            print(f"Папка {self.html_dir} не найдена")
            return
            
        # Собираем все HTML файлы matches и results
        for file_name in os.listdir(self.html_dir):
            if (('matches_' in file_name.lower() or 'results_' in file_name.lower()) and 
                file_name.endswith('.html')):
                file_path = os.path.join(self.html_dir, file_name)
                file_type = 'matches' if 'matches_' in file_name.lower() else 'results'
                print(f"Обработка файла {file_type}: {file_name}")
                
                # Парсим файл
                matches = self._parse_html_file(file_path)
                print(f"Найдено матчей: {len(matches)}")
                
                # Сохраняем в базу
                if matches:
                    new_matches = self._save_to_db(matches)
                    print(f"Добавлено новых матчей в базу данных: {new_matches}")
                    
                # Удаляем обработанный файл
                # os.remove(file_path)
                # print(f"Файл {file_name} удален")

if __name__ == "__main__":
    collector = MatchesCollector()
    collector.collect()
    print("Collection completed")
