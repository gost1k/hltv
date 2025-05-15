import os
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re
import logging
from src.config.constants import HTML_DIR, MATCHES_HTML_FILE, RESULTS_HTML_FILE, DATABASE_FILE
import time

logger = logging.getLogger(__name__)

class MatchesCollector:
    def __init__(self, html_dir: str = HTML_DIR, db_path: str = DATABASE_FILE):
        self.html_dir = html_dir
        self.db_path = db_path
        
    def _get_match_id(self, url: str) -> int:
        """Извлекает ID матча из URL"""
        match = re.search(r'/matches/(\d+)/', url)
        if match:
            return int(match.group(1))
        return None
        
    def _parse_matches_file(self, soup) -> list:
        """Парсит страницу предстоящих матчей, собирая только матчи с определенными командами (не TBD)"""
        matches = []
        
        # Находим основной контент
        main_content = soup.select_one('.mainContent')
        if not main_content:
            logger.error("Не найден основной контент (.mainContent) на странице")
            return matches
            
        # Находим все матчи
        match_elements = main_content.select('.match')
        if not match_elements:
            logger.error("Не найдены элементы матчей (.match) на странице")
            return matches
            
        logger.info(f"Всего найдено {len(match_elements)} элементов матчей")
        
        for match in match_elements:
            try:
                # Получаем ссылку на матч
                match_link = match.select_one('a')
                if not match_link:
                    logger.debug("Пропущен матч: не найдена ссылка на матч")
                    continue
                    
                url = match_link.get('href', '')
                match_id = self._get_match_id(url)
                
                if not match_id:
                    logger.debug(f"Пропущен матч: не удалось извлечь ID из ссылки {url}")
                    continue
                
                # Извлекаем имена команд из URL
                team_names = self._extract_team_names_from_url(url)
                if not team_names or len(team_names) < 2:
                    logger.debug(f"Пропущен матч {match_id}: не удалось извлечь имена команд из URL {url}")
                    continue
                
                # Проверяем, что команды определены (не TBD)
                team1_name = team_names[0].strip()
                team2_name = team_names[1].strip()
                
                if team1_name.lower() == "tbd" or team2_name.lower() == "tbd" or not team1_name or not team2_name:
                    logger.debug(f"Пропущен матч {match_id}: найдены неопределенные команды - {team1_name} vs {team2_name}")
                    continue
                
                # Ищем элемент времени с data-unix атрибутом
                unix_time = None
                for el in match.find_all(lambda tag: tag.has_attr('data-unix')):
                    unix_time = el.get('data-unix', '')
                    break
                
                # Если не нашли через data-unix, ищем через класс времени
                if not unix_time:
                    time_element = match.select_one('.time, .date, .matchTime')
                    if time_element:
                        unix_time = time_element.get('data-unix', '')
                
                if not unix_time:
                    # Если время не найдено, используем текущее время
                    logger.debug(f"Матч {match_id}: время не найдено, используем текущее время")
                    unix_time = str(int(time.time()))
                
                # Формируем данные матча
                match_data = {
                    'id': match_id,
                    'url': url,
                    'date': int(unix_time),
                    'toParse': 1
                }
                
                logger.debug(f"Добавлен матч {match_id}: {team1_name} vs {team2_name}")
                matches.append(match_data)
                
            except Exception as e:
                logger.error(f"Ошибка при парсинге матча: {str(e)}")
                continue
                
        logger.info(f"Найдено предстоящих матчей с определенными командами: {len(matches)}")
        return matches
        
    def _extract_team_names_from_url(self, url: str) -> list:
        """Извлекает имена команд из URL матча"""
        try:
            # Пример URL: /matches/2382314/fnatic-vs-9ine-rainbet-cup-2025
            parts = url.split('/')
            if len(parts) < 3:
                return []
                
            match_part = parts[-1]  # fnatic-vs-9ine-rainbet-cup-2025
            
            # Разделяем по первому вхождению дефиса после "vs"
            team_part = match_part.split('-')
            vs_index = -1
            for i, part in enumerate(team_part):
                if part.lower() == 'vs':
                    vs_index = i
                    break
            
            if vs_index == -1 or vs_index == 0 or vs_index == len(team_part) - 1:
                return []
                
            team1 = '-'.join(team_part[:vs_index])
            team2 = team_part[vs_index+1]
            
            # Находим индекс, где заканчивается название второй команды
            # Обычно после имени второй команды идет название турнира
            for i in range(vs_index+2, len(team_part)):
                if team_part[i] in ['esl', 'blast', 'champions', 'major', 'cup', 'series', 'tournament', 'qualifier']:
                    break
                team2 += '-' + team_part[i]
            
            return [team1, team2]
        except Exception as e:
            logger.error(f"Ошибка при извлечении имен команд из URL: {str(e)}")
            return []

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
                    'toParse': 1  # Для новых записей ставим флаг необходимости парсинга
                }
                matches.append(match_data)
                
            except Exception as e:
                logger.error(f"Ошибка при парсинге результата: {str(e)}")
                continue
                
        return matches

    def _parse_html_file(self, file_path: str) -> list:
        """Парсит HTML файл и возвращает список матчей"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')

            # Определяем тип страницы по имени файла
            if 'matches' in file_path.lower():
                return self._parse_matches_file(soup)
            elif 'results' in file_path.lower():
                return self._parse_results_file(soup)
            return []
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {file_path}: {str(e)}")
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
    
    def _save_upcoming_matches(self, matches: list) -> dict:
        """Сохраняет предстоящие матчи в базу данных"""
        if not matches:
            return {"new": 0, "updated": 0, "deleted": 0}
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_matches = 0
        updated_matches = 0
        deleted_matches = 0
        
        # Получаем список всех ID матчей из текущего HTML
        current_match_ids = [match['id'] for match in matches]
        
        # Получаем список всех ID в базе данных
        cursor.execute('SELECT id FROM url_upcoming')
        db_match_ids = [row[0] for row in cursor.fetchall()]
        
        # Находим ID матчей, которые есть в БД, но отсутствуют в текущем HTML
        obsolete_ids = [match_id for match_id in db_match_ids if match_id not in current_match_ids]
        
        # Удаляем устаревшие матчи
        if obsolete_ids:
            # Используем параметризованный запрос с placeholders для безопасности
            placeholders = ','.join(['?'] * len(obsolete_ids))
            delete_query = f'DELETE FROM url_upcoming WHERE id IN ({placeholders})'
            cursor.execute(delete_query, obsolete_ids)
            deleted_matches = cursor.rowcount
            logger.info(f"Удалено {deleted_matches} устаревших матчей из таблицы url_upcoming")
        
        for match in matches:
            # Проверяем, существует ли матч в таблице предстоящих матчей
            cursor.execute('SELECT 1 FROM url_upcoming WHERE id = ?', (match['id'],))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Матч уже существует, ничего не делаем, просто учитываем в статистике
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
        logger.info(f"Добавлено новых предстоящих матчей: {new_matches}, обновлено: {updated_matches}, удалено устаревших: {deleted_matches}")
        return {"new": new_matches, "updated": updated_matches, "deleted": deleted_matches}
        
    def _save_past_matches(self, matches: list) -> dict:
        """Сохраняет результаты матчей в базу данных"""
        if not matches:
            return {"new": 0, "updated": 0}
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_matches = 0
        updated_matches = 0
        
        for match in matches:
            # Проверяем, существует ли матч в таблице результатов
            cursor.execute('SELECT 1 FROM url_result WHERE id = ?', (match['id'],))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Считаем как обновленный, но не меняем в базе
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
        return {"new": new_matches, "updated": updated_matches}
    
    def collect_matches(self) -> dict:
        """
        Собирает данные из HTML-файла со списком матчей
        
        Returns:
            dict: Статистика обработки
        """
        # Создаем таблицы, если их нет
        self._create_tables()
        
        stats = {
            "total": 0,
            "new": 0,
            "updated": 0,
            "deleted": 0,
            "failed": 0
        }
        
        try:
            file_path = MATCHES_HTML_FILE
            logger.info(f"Обработка файла матчей: {file_path}")
            
            # Проверяем существование файла
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                stats["failed"] = 1
                return stats
                
            # Парсим файл
            matches = self._parse_html_file(file_path)
            stats["total"] = len(matches)
            logger.info(f"Найдено предстоящих матчей: {stats['total']}")
            
            # Сохраняем в базу данных
            if matches:
                result = self._save_upcoming_matches(matches)
                stats["new"] = result["new"]
                stats["updated"] = result["updated"]
                stats["deleted"] = result["deleted"]
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при обработке матчей: {str(e)}")
            stats["failed"] = 1
            return stats
    
    def collect_results(self) -> dict:
        """
        Собирает данные из HTML-файла с результатами матчей
        
        Returns:
            dict: Статистика обработки
        """
        # Создаем таблицы, если их нет
        self._create_tables()
        
        stats = {
            "total": 0,
            "new": 0,
            "updated": 0,
            "failed": 0
        }
        
        try:
            file_path = RESULTS_HTML_FILE
            logger.info(f"Обработка файла результатов: {file_path}")
            
            # Проверяем существование файла
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                stats["failed"] = 1
                return stats
                
            # Парсим файл
            matches = self._parse_html_file(file_path)
            stats["total"] = len(matches)
            logger.info(f"Найдено результатов матчей: {stats['total']}")
            
            # Сохраняем в базу данных
            if matches:
                result = self._save_past_matches(matches)
                stats["new"] = result["new"]
                stats["updated"] = result["updated"]
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при обработке результатов: {str(e)}")
            stats["failed"] = 1
            return stats
        
    def collect(self):
        """Собирает данные из всех HTML файлов в папке"""
        # Для обратной совместимости со старым кодом
        matches_stats = self.collect_matches()
        results_stats = self.collect_results()
        
        return {
            "matches": matches_stats,
            "results": results_stats
        }

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    collector = MatchesCollector()
    result = collector.collect()
    logger.info(f"Collection completed: {result}")
