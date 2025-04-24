import os
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re

class MatchesCollector:
    def __init__(self, html_dir: str = "html", db_path: str = "hltv.db"):
        self.html_dir = html_dir
        self.db_path = db_path
        
    def _get_match_id(self, url: str) -> int:
        """Извлекает ID матча из URL"""
        match = re.search(r'/matches/(\d+)/', url)
        if match:
            return int(match.group(1))
        return None
        
    def _parse_html_file(self, file_path: str) -> list:
        """Парсит HTML файл и возвращает список матчей"""
        matches = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
            
        # Находим основной контент
        main_content = soup.select_one('.mainContent')
        if not main_content:
            print(f"Не найден .mainContent в {file_path}")
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
        
    def _save_to_db(self, matches: list):
        """Сохраняет матчи в базу данных"""
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
        
        # Вставляем или обновляем данные
        for match in matches:
            cursor.execute('''
                INSERT OR REPLACE INTO matches (id, url, date, toParse)
                VALUES (?, ?, ?, ?)
            ''', (match['id'], match['url'], match['date'], match['toParse']))
            
        conn.commit()
        conn.close()
        
    def collect(self):
        """Собирает данные из всех HTML файлов в папке"""
        if not os.path.exists(self.html_dir):
            print(f"Папка {self.html_dir} не найдена")
            return
            
        # Собираем все HTML файлы с matches в названии
        for file_name in os.listdir(self.html_dir):
            if 'matches_' in file_name.lower() and file_name.endswith('.html'):
                file_path = os.path.join(self.html_dir, file_name)
                print(f"Обработка файла: {file_name}")
                
                # Парсим файл
                matches = self._parse_html_file(file_path)
                print(f"Найдено матчей: {len(matches)}")
                
                # Сохраняем в базу
                if matches:
                    self._save_to_db(matches)
                    print(f"Матчи сохранены в базу данных")

if __name__ == "__main__":
    collector = MatchesCollector()
    collector.collect()
    print("Collection completed")
