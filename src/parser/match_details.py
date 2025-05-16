"""
Парсер деталей матчей HLTV

Извлекает из базы данных matches с date = 0 и скачивает их HTML-страницы
"""
import os
import sqlite3
import time
import logging
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

from src.parser.base import BaseParser
from src.config.constants import BASE_URL as HLTV_BASE_URL, HTML_DIR as HTML_STORAGE_DIR, MATCH_UPCOMING_DIR


class MatchDetailsParser(BaseParser):
    """
    Парсер для скачивания страниц деталей матчей
    Использует список URL-адресов из базы данных
    """
    
    def __init__(self, db_path="hltv.db", limit=10, parse_past=True, parse_upcoming=False):
        """
        Инициализация парсера деталей матчей
        
        Args:
            db_path (str): Путь к файлу базы данных
            limit (int): Максимальное количество матчей для обработки за один запуск
            parse_past (bool): Парсить прошедшие матчи
            parse_upcoming (bool): Парсить предстоящие матчи
        """
        super().__init__()
        self.db_path = db_path
        self.limit = limit
        self.parse_past = parse_past
        self.parse_upcoming = parse_upcoming
        self.logger.info(f"MatchDetailsParser инициализирован, лимит: {limit} матчей")
        
    def _get_matches_to_parse(self):
        """
        Получает список матчей для парсинга из базы данных
        
        Returns:
            list: Список словарей с информацией о матчах (id, url)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            matches = []
            remaining_limit = self.limit
            
            # Получаем прошедшие матчи для парсинга
            if self.parse_past and (remaining_limit is None or remaining_limit > 0):
                if self.limit is None:
                    cursor.execute('''
                        SELECT id, url FROM result_urls 
                        WHERE toParse = 1
                    ''')
                else:
                    cursor.execute('''
                        SELECT id, url FROM result_urls 
                        WHERE toParse = 1
                        LIMIT ?
                    ''', (remaining_limit,))
                
                past_matches = [{"id": row[0], "url": row[1], "is_past": True} for row in cursor.fetchall()]
                matches.extend(past_matches)
                if remaining_limit is not None:
                    remaining_limit -= len(past_matches)
                self.logger.info(f"Найдено {len(past_matches)} прошедших матчей для скачивания")
            
            # Получаем предстоящие матчи для парсинга, у которых toParse = 1
            if self.parse_upcoming and (remaining_limit is None or remaining_limit > 0):
                if self.limit is None:
                    cursor.execute('''
                        SELECT id, url FROM upcoming_urls
                        WHERE toParse = 1
                    ''')
                else:
                    cursor.execute('''
                        SELECT id, url FROM upcoming_urls
                        WHERE toParse = 1
                        LIMIT ?
                    ''', (remaining_limit,))
                
                upcoming_matches = [{"id": row[0], "url": row[1], "is_past": False} for row in cursor.fetchall()]
                matches.extend(upcoming_matches)
                self.logger.info(f"Найдено {len(upcoming_matches)} предстоящих матчей для скачивания")
            
            conn.close()
            self.logger.info(f"Всего найдено {len(matches)} матчей для скачивания")
            return matches
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка матчей: {str(e)}")
            return []
    
    def _update_match_status(self, match_id, is_past, status=0):
        """
        Обновляет статус матча в базе данных
        
        Args:
            match_id (int): ID матча
            is_past (bool): Является ли матч прошедшим
            status (int): Новый статус (0 - обработан, 1 - требует обработки)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            table_name = "result_urls" if is_past else "upcoming_urls"
            
            cursor.execute(f'''
                UPDATE {table_name} SET toParse = ? WHERE id = ?
            ''', (status, match_id))
            
            conn.commit()
            conn.close()
            self.logger.info(f"Обновлен статус матча ID {match_id} в таблице {table_name} на {status}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при обновлении статуса матча {match_id}: {str(e)}")
    
    def _get_filename_from_url(self, match_id, url):
        """
        Формирует имя файла из URL матча
        
        Args:
            match_id (int): ID матча
            url (str): URL матча
            
        Returns:
            str: Имя файла
        """
        try:
            # Парсим URL
            parsed_url = urlparse(url)
            path = parsed_url.path
            
            # Проверяем, что URL соответствует формату /matches/ID/команда1-vs-команда2-турнир
            if '/matches/' in path:
                # Убираем начальный "/matches/" и ID
                match_info = path.split('/matches/')[1]
                
                # Удаляем ID (если он есть) и получаем строку с командами и турниром
                if match_info.split('/')[0].isdigit():
                    match_info = '/'.join(match_info.split('/')[1:])
                
                # Если такая информация есть в URL
                if match_info:
                    # Заменяем все специальные символы на дефисы
                    match_info = re.sub(r'[^\w-]', '-', match_info).lower()
                    # Удаляем лишние дефисы
                    match_info = re.sub(r'-+', '-', match_info)
                    # Удаляем дефисы в начале и конце
                    match_info = match_info.strip('-')
                    
                    # Формируем имя файла
                    return f"match_{match_id}-{match_info}.html"
            
            # Если не удалось извлечь информацию из URL, используем старый формат
            return f"match_{match_id}.html"
            
        except Exception as e:
            self.logger.warning(f"Ошибка при формировании имени файла из URL: {str(e)}")
            return f"match_{match_id}.html"
    
    def _parse_match_page(self, match):
        """
        Парсит страницу отдельного матча
        
        Args:
            match (dict): Информация о матче (id, url, is_past)
            
        Returns:
            bool: True если успешно, False если ошибка
        """
        match_id = match["id"]
        url = match["url"]
        is_past = match.get("is_past", True)  # По умолчанию считаем матч прошедшим
        
        # Формируем полный URL, если это относительный путь
        if not url.startswith('http'):
            full_url = urljoin(HLTV_BASE_URL, url)
        else:
            full_url = url
        
        try:
            # Формируем имя файла и путь
            file_name = self._get_filename_from_url(match_id, full_url)
            
            # Определяем директорию для сохранения в зависимости от типа матча
            if is_past:
                # Для прошедших матчей используем директорию result
                save_dir = os.path.join(HTML_STORAGE_DIR, "result")
                self.logger.info(f"Матч ID {match_id} определен как прошедший, сохраняем в {save_dir}")
            else:
                # Для предстоящих матчей используем директорию upcoming
                save_dir = MATCH_UPCOMING_DIR
                self.logger.info(f"Матч ID {match_id} определен как предстоящий, сохраняем в {save_dir}")
            
            # Создаем директорию, если её нет
            os.makedirs(save_dir, exist_ok=True)
            
            file_path = os.path.join(save_dir, file_name)
            
            # Для прошедших матчей и предстоящих матчей с toParse = 0 пропускаем, если файл уже существует
            if os.path.exists(file_path):
                # Проверяем, что у предстоящего матча стоит toParse = 0
                if not is_past:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT toParse FROM upcoming_urls WHERE id = ?", (match_id,))
                    result = cursor.fetchone()
                    conn.close()
                    
                    if result and result[0] == 0:
                        self.logger.info(f"Файл для предстоящего матча ID {match_id} уже существует и toParse = 0. Пропускаем скачивание.")
                        return True
                else:
                    self.logger.info(f"Файл для прошедшего матча ID {match_id} уже существует: {file_path}. Пропускаем скачивание.")
                    return True
                
            self.logger.info(f"Загрузка страницы матча ID {match_id}: {full_url}")
            
            # Загружаем страницу
            self.driver.get(full_url)
            self._wait_for_page_load()
            
            # Получаем HTML-контент
            html = self.driver.page_source
            
            if len(html) < 1000:
                self.logger.warning(f"Получен слишком маленький HTML для матча {match_id} ({len(html)} байт)")
                return False
            
            # Не выводим HTML-контент в лог
            html_size = len(html)
            self.logger.info(f"Получен HTML-контент ({html_size} байт) для матча ID {match_id}")
            
            # Сохраняем HTML в файл
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(html)
                
            self.logger.info(f"Сохранена страница матча ID {match_id} в {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке страницы матча {match_id}: {str(e)}")
            return False
    
    def parse(self):
        """
        Основной метод парсинга
        Получает список матчей из БД и скачивает их страницы
        
        Returns:
            int: Количество успешно скачанных страниц
        """
        self.logger.info("Начало скачивания страниц матчей")
        
        # Получаем список матчей для парсинга
        matches = self._get_matches_to_parse()
        
        if not matches:
            self.logger.info("Нет матчей для скачивания")
            return 0
            
        successful = 0
        
        # Парсим каждый матч
        for match in matches:
            try:
                # Инициализируем новый драйвер для каждого матча
                self._setup_driver()
                
                # Парсим страницу
                if self._parse_match_page(match):
                    # Если успешно и это прошедший матч, обновляем статус
                    # Для предстоящих матчей статус не меняем (это делает метод _update_match_status)
                    self._update_match_status(match["id"], match.get("is_past", True), 0)
                    successful += 1
                
                # Закрываем браузер
                self.close()
                
                # Небольшая задержка между запросами
                time.sleep(2)
                
            except Exception as e:
                self.logger.error(f"Ошибка при обработке матча {match['id']}: {str(e)}")
                self.close()  # Убеждаемся, что браузер закрыт
        
        self.logger.info(f"Завершено скачивание страниц матчей. Успешно: {successful} из {len(matches)}")
        return successful


if __name__ == "__main__":
    # Для автономного запуска
    import sys
    
    # Проверяем, есть ли параметр --test
    test_mode = "--test" in sys.argv
    
    # Устанавливаем лимит в зависимости от режима
    limit = 3 if test_mode else 5
    
    print(f"Запуск парсера деталей матчей в {'тестовом режиме' if test_mode else 'обычном режиме'} (лимит: {limit})")
    parser = MatchDetailsParser(limit=limit)
    parser.parse() 