"""
Модуль для извлечения данных из HTML-файлов матчей
и сохранения их в таблицы match_details и player_stats
"""
import os
import re
import logging
import sqlite3
from bs4 import BeautifulSoup
import glob
from datetime import datetime
import time
from src.config.constants import MATCH_DETAILS_DIR
from src.config.selectors import *

# Настройка логирования
logger = logging.getLogger(__name__)

class MatchDetailsCollector:
    """
    Класс для извлечения данных из HTML-файлов матчей и их сохранения в БД
    """
    def __init__(self, html_dir="storage/html/result", db_path="hltv.db"):
        """
        Инициализация коллектора деталей матчей
        
        Args:
            html_dir (str): Путь к директории с HTML-файлами прошедших матчей
            db_path (str): Путь к файлу базы данных
        """
        self.html_dir = html_dir
        self.db_path = db_path
    
    def collect(self):
        """
        Основной метод для извлечения данных из HTML-файлов и сохранения их в БД
        
        Returns:
            dict: Статистика обработки файлов
        """
        logger.info("Начинаем сбор деталей матчей")
        
        # Статистика обработки
        stats = {
            'total_files': 0,
            'processed_files': 0,
            'successful_match_details': 0,
            'successful_player_stats': 0,
            'errors': 0,
            'already_exists': 0
        }
        
        # Получаем файлы для обработки
        files_to_process = self.get_files_to_process()
        stats['total_files'] = len(files_to_process)
        
        if not files_to_process:
            logger.info("Нет новых файлов для обработки")
            return stats
        
        logger.info(f"Найдено {len(files_to_process)} файлов для обработки")
        
        # Обрабатываем каждый файл
        for file_path in files_to_process:
            try:
                result = self.process_file(file_path)
                stats['processed_files'] += 1
                
                if result == "success":
                    stats['successful_match_details'] += 1
                    stats['successful_player_stats'] += 1
                elif result == "already_exists":
                    stats['already_exists'] += 1
                elif result == "error":
                    stats['errors'] += 1
                
                # Логируем прогресс каждые 10 файлов
                if stats['processed_files'] % 10 == 0:
                    logger.info(f"Обработано {stats['processed_files']} из {stats['total_files']} файлов")
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}")
                stats['errors'] += 1
        
        logger.info(f"Завершен сбор деталей матчей. Обработано {stats['processed_files']} из {stats['total_files']} файлов")
        logger.info(f"Успешно: {stats['successful_match_details']}, Уже существуют: {stats['already_exists']}, Ошибок: {stats['errors']}")
        
        return stats
    
    def get_files_to_process(self):
        """
        Получает список HTML-файлов для обработки
        
        Returns:
            list: Список путей к HTML-файлам
        """
        # Проверяем, что директория существует
        if not os.path.exists(self.html_dir):
            logger.error(f"Директория {self.html_dir} не существует")
            return []
            
        # Получаем все HTML-файлы в директории
        html_files = glob.glob(os.path.join(self.html_dir, "match_*.html"))
        logger.info(f"Найдено {len(html_files)} HTML-файлов с деталями матчей")
        
        # Отфильтровываем только файлы, которые еще не были обработаны
        files_to_process = []
        for file_path in html_files:
            match_id = self._extract_match_id_from_filename(os.path.basename(file_path))
            if match_id and not self._is_match_details_exists(match_id):
                files_to_process.append(file_path)
                
        logger.info(f"Из них {len(files_to_process)} файлов требуют обработки")
        return files_to_process
    
    def process_file(self, file_path):
        """
        Обрабатывает один HTML-файл с деталями матча
        
        Args:
            file_path (str): Путь к HTML-файлу
            
        Returns:
            str: Статус обработки ("success", "already_exists", "error")
        """
        try:
            match_id = self._extract_match_id_from_filename(os.path.basename(file_path))
            
            if not match_id:
                logger.error(f"Не удалось извлечь ID матча из имени файла: {file_path}")
                return "error"
                
            # Проверяем, существуют ли уже детали этого матча в БД
            if self._is_match_details_exists(match_id):
                logger.info(f"Детали матча {match_id} уже существуют в базе данных")
                return "already_exists"
                
            logger.info(f"Обработка файла {file_path} для матча {match_id}")
            
            # Читаем HTML-файл
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            # Парсим HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Извлекаем детали матча
            match_data = self._parse_match_details(soup, match_id)
            
            if not match_data:
                logger.error(f"Не удалось извлечь детали матча из {file_path}")
                return "error"
                
            # Сохраняем детали матча в БД
            self._save_match_details(match_data)
            
            # Извлекаем статистику игроков
            players_data = self._parse_player_stats(soup, match_id)
            
            if players_data:
                # Сохраняем статистику игроков в БД
                self._save_player_stats(players_data)
                
            logger.info(f"Успешно обработан файл {file_path}")
            return "success"
            
        except Exception as e:
            logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}")
            return "error"
    
    def _is_match_details_exists(self, match_id):
        """
        Проверяет, существуют ли детали матча в базе данных
        
        Args:
            match_id (int): ID матча
            
        Returns:
            bool: True, если детали матча уже есть в БД, иначе False
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Проверяем существование таблицы match_details
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='match_details'")
            table_exists = cursor.fetchone() is not None
            
            if not table_exists:
                conn.close()
                return False
                
            # Проверяем наличие данных о матче
            cursor.execute("SELECT 1 FROM match_details WHERE match_id = ?", (match_id,))
            exists = cursor.fetchone() is not None
            
            conn.close()
            return exists
            
        except Exception as e:
            logger.error(f"Ошибка при проверке существования деталей матча {match_id}: {str(e)}")
            return False

    def _extract_match_id_from_filename(self, filename):
        """
        Извлекает ID матча из имени файла
        
        Args:
            filename (str): Имя файла
            
        Returns:
            int or None: ID матча или None, если не удалось извлечь
        """
        match = re.search(r'match_(\d+)', filename)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_id_from_url(self, url):
        """
        Извлекает ID из URL
        
        Args:
            url (str): URL-адрес
            
        Returns:
            int or None: ID или None, если не удалось извлечь
        """
        if not url:
            return None
            
        match = re.search(r'/(\d+)/', url)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_demo_id(self, demo_link):
        """
        Извлекает ID демо из ссылки на демо
        
        Args:
            demo_link (str): Ссылка на демо, например '/download/demo/97086'
            
        Returns:
            int or None: ID демо или None, если не удалось извлечь
        """
        if not demo_link:
            return None
            
        match = re.search(r'/demo/(\d+)', demo_link)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_rank(self, rank_text):
        """
        Извлекает числовое значение рейтинга из текста
        
        Args:
            rank_text (str): Текст с рейтингом, например "#6"
            
        Returns:
            int or None: Числовое значение рейтинга или None
        """
        if not rank_text:
            return None
            
        match = re.search(r'#(\d+)', rank_text)
        if match:
            return int(match.group(1))
        return None
    
    def _parse_match_details(self, soup, match_id):
        """
        Извлекает детали матча из HTML
        
        Args:
            soup (BeautifulSoup): HTML-документ
            match_id (int): ID матча
            
        Returns:
            dict: Словарь с данными матча или None в случае ошибки
        """
        try:
            # По умолчанию статус - upcoming
            match_status = STATUS_UPCOMING
            
            # Проверяем, завершен ли матч по тексту "Match over" в блоке countdown
            match_over_element = soup.select_one(COUNTDOWN)
            if match_over_element and MATCH_OVER_TEXT in match_over_element.text:
                match_status = STATUS_COMPLETED
            
            match_data = {
                'match_id': match_id,
                'datetime': None,
                'team1_id': None,
                'team1_name': None,
                'team1_score': None,
                'team1_rank': None,
                'team2_id': None,
                'team2_name': None,
                'team2_score': None,
                'team2_rank': None,
                'event_id': None,
                'event_name': None,
                'demo_id': None,
                'head_to_head_team1_wins': None,
                'head_to_head_team2_wins': None,
                'status': match_status
            }
            
            # Извлекаем время матча
            time_element = soup.select_one(TIME_EVENT)
            if time_element:
                match_data['datetime'] = int(time_element.get('data-unix', 0)) // 1000  # Переводим из мс в секунды
            
            # Извлекаем данные первой команды
            team1_element = soup.select_one(TEAM1_GRADIENT)
            if team1_element:
                team1_link = team1_element.select_one(PLAYER_LINK)
                if team1_link:
                    match_data['team1_id'] = self._extract_id_from_url(team1_link.get('href'))
                
                team1_name_element = team1_element.select_one(TEAM_NAME)
                if team1_name_element:
                    match_data['team1_name'] = team1_name_element.text.strip()
            
            # Извлекаем данные второй команды
            team2_element = soup.select_one(TEAM2_GRADIENT)
            if team2_element:
                team2_link = team2_element.select_one(PLAYER_LINK)
                if team2_link:
                    match_data['team2_id'] = self._extract_id_from_url(team2_link.get('href'))
                
                team2_name_element = team2_element.select_one(TEAM_NAME)
                if team2_name_element:
                    match_data['team2_name'] = team2_name_element.text.strip()
            
            # Извлекаем счет команд
            team1_score_element = soup.select_one(TEAM1_SCORE)
            if team1_score_element:
                match_data['team1_score'] = int(team1_score_element.text.strip())
                logger.info(f"Счет первой команды: {match_data['team1_score']}")
            
            team2_score_element = soup.select_one(TEAM2_SCORE)
            if team2_score_element:
                match_data['team2_score'] = int(team2_score_element.text.strip())
                logger.info(f"Счет второй команды: {match_data['team2_score']}")
            
            # Извлекаем рейтинги команд
            team1_rank_element = soup.select_one(TEAM1_RANK)
            if team1_rank_element:
                match_data['team1_rank'] = self._extract_rank(team1_rank_element.text.strip())
            
            team2_rank_element = soup.select_one(TEAM2_RANK)
            if team2_rank_element:
                match_data['team2_rank'] = self._extract_rank(team2_rank_element.text.strip())
            
            # Извлекаем данные о событии
            event_element = soup.select_one(EVENT)
            if event_element:
                match_data['event_id'] = self._extract_id_from_url(event_element.get('href'))
                match_data['event_name'] = event_element.text.strip()
            
            # Извлекаем ID демо из атрибута data-demo-link
            demo_element = soup.select_one(DEMO_URL)
            if demo_element:
                demo_link = demo_element.get('data-demo-link')
                match_data['demo_id'] = self._extract_demo_id(demo_link)
                logger.info(f"Найдена ссылка на демо: {demo_link} (ID: {match_data['demo_id']})")
            
            # Извлекаем данные об очных встречах
            h2h_team1_element = soup.select_one(H2H_TEAM1_WINS)
            if h2h_team1_element:
                try:
                    match_data['head_to_head_team1_wins'] = int(h2h_team1_element.text.strip())
                except ValueError:
                    match_data['head_to_head_team1_wins'] = 0
            
            h2h_team2_element = soup.select_one(H2H_TEAM2_WINS)
            if h2h_team2_element:
                try:
                    match_data['head_to_head_team2_wins'] = int(h2h_team2_element.text.strip())
                except ValueError:
                    match_data['head_to_head_team2_wins'] = 0
            
            # Логирование информации о матче
            if match_data['status'] == STATUS_COMPLETED:
                if match_data['team1_score'] is None or match_data['team2_score'] is None:
                    logger.warning(f"Матч {match_id} помечен как завершенный, но не имеет счета")
            else:
                logger.info(f"Матч {match_id} определен как предстоящий")
            
            return match_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге деталей матча {match_id}: {str(e)}")
            return None
    
    def _parse_player_stats(self, soup, match_id):
        """
        Извлекает статистику игроков из HTML
        
        Args:
            soup (BeautifulSoup): HTML-документ
            match_id (int): ID матча
            
        Returns:
            list: Список словарей с данными игроков или пустой список в случае ошибки
        """
        players_data = []
        
        try:
            # Получаем ID команд
            team1_id = None
            team2_id = None
            
            team1_element = soup.select_one(f"{TEAM1_GRADIENT} {PLAYER_LINK}")
            if team1_element:
                team1_id = self._extract_id_from_url(team1_element.get('href'))
            
            team2_element = soup.select_one(f"{TEAM2_GRADIENT} {PLAYER_LINK}")
            if team2_element:
                team2_id = self._extract_id_from_url(team2_element.get('href'))
            
            # Ищем контейнер stats-content, содержащий таблицы статистики
            stats_content = soup.select_one('.stats-content')
            if not stats_content:
                logger.warning(f"Контейнер .stats-content не найден для матча {match_id}")
                
                # Пробуем найти хотя бы одну таблицу статистики (старый метод)
                stats_table = soup.select_one(STATS_TABLE)
                if not stats_table:
                    logger.warning(f"Таблица статистики не найдена для матча {match_id}")
                    return players_data
                
                # Проверяем, есть ли заголовки в таблице
                headers = stats_table.select('th')
                
                if headers:
                    # Стандартная таблица с заголовками - используем стандартный метод
                    return self._parse_player_stats_with_headers(soup, match_id, stats_table, team1_id, team2_id)
                else:
                    # Новая структура таблицы без заголовков - используем новый метод
                    return self._parse_player_stats_without_headers(soup, match_id, stats_table, team1_id, team2_id)
            
            # Находим таблицы внутри .stats-content
            stats_tables = stats_content.select('.table.totalstats, table.totalstats')
            
            if not stats_tables:
                # Если таблицы с классом totalstats не найдены, ищем любые таблицы
                stats_tables = stats_content.select('table')
                
            if not stats_tables:
                logger.warning(f"Таблицы статистики не найдены в .stats-content для матча {match_id}")
                return players_data
            
            logger.info(f"Найдено {len(stats_tables)} таблиц статистики")
            
            # Если найдено несколько таблиц (обычно две - по одной на команду)
            if len(stats_tables) >= 2:
                # Получаем названия команд
                team1_name = None
                team2_name = None
                
                team1_element = soup.select_one(f"{TEAM1_GRADIENT} .teamName")
                if team1_element:
                    team1_name = team1_element.text.strip()
                    logger.info(f"Название первой команды: {team1_name}")
                
                team2_element = soup.select_one(f"{TEAM2_GRADIENT} .teamName")
                if team2_element:
                    team2_name = team2_element.text.strip()
                    logger.info(f"Название второй команды: {team2_name}")
                
                # Обрабатываем первую таблицу (первая команда)
                team1_rows = stats_tables[0].select('tbody tr')
                for row in team1_rows:
                    player_data = self._extract_player_stats_from_new_format(row, match_id, team1_id)
                    if player_data:
                        players_data.append(player_data)
                
                # Обрабатываем вторую таблицу (вторая команда)
                team2_rows = stats_tables[1].select('tbody tr')
                for row in team2_rows:
                    player_data = self._extract_player_stats_from_new_format(row, match_id, team2_id)
                    if player_data:
                        players_data.append(player_data)
                
                logger.info(f"Извлечена статистика для {len(players_data)} игроков из обеих команд")
                return players_data
            
            # Если найдена только одна таблица, используем старую логику
            stats_table = stats_tables[0]
            
            # Проверяем, есть ли заголовки в таблице
            headers = stats_table.select('th')
            
            if headers:
                # Стандартная таблица с заголовками - используем стандартный метод
                return self._parse_player_stats_with_headers(soup, match_id, stats_table, team1_id, team2_id)
            else:
                # Новая структура таблицы без заголовков - используем новый метод
                return self._parse_player_stats_without_headers(soup, match_id, stats_table, team1_id, team2_id)
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге статистики игроков матча {match_id}: {str(e)}")
            return []
    
    def _parse_player_stats_without_headers(self, soup, match_id, stats_table, team1_id, team2_id):
        """
        Извлекает статистику игроков из таблицы без заголовков (новый формат HLTV)
        
        Args:
            soup (BeautifulSoup): HTML-документ
            match_id (int): ID матча
            stats_table (Tag): Таблица статистики
            team1_id (int): ID первой команды
            team2_id (int): ID второй команды
            
        Returns:
            list: Список словарей с данными игроков
        """
        players_data = []
        
        try:
            # Получаем названия команд
            team1_name = None
            team2_name = None
            
            team1_element = soup.select_one(f"{TEAM1_GRADIENT} .teamName")
            if team1_element:
                team1_name = team1_element.text.strip()
                logger.info(f"Название первой команды: {team1_name}")
            
            team2_element = soup.select_one(f"{TEAM2_GRADIENT} .teamName")
            if team2_element:
                team2_name = team2_element.text.strip()
                logger.info(f"Название второй команды: {team2_name}")
            
            # Получаем все строки таблицы
            rows = stats_table.select('tr')
            logger.info(f"Всего строк в таблице: {len(rows)}")
            
            # Определяем строки для каждой команды
            team1_rows = []
            team2_rows = []
            
            # Переменная для отслеживания текущей команды
            current_team_id = None
            current_team_name = None
            
            # Проходим по строкам таблицы и группируем их по командам
            for row in rows:
                # Получаем классы строки
                row_classes = row.get('class', [])
                
                # Получаем ячейки
                cells = row.select('td')
                if not cells:
                    continue
                    
                # Если это строка заголовка команды (имеет класс 'header-row')
                if 'header-row' in row_classes:
                    # Получаем ссылку на команду в первой ячейке
                    team_link = cells[0].select_one('a')
                    if team_link and '/team/' in team_link.get('href', ''):
                        team_href = team_link.get('href', '')
                        team_id = self._extract_id_from_url(team_href)
                        
                        team_name = team_link.text.strip()
                        logger.info(f"Найдена строка заголовка команды: {team_name} (ID: {team_id})")
                        
                        # Запоминаем текущую команду
                        current_team_id = team_id
                        current_team_name = team_name
                        
                        if team_id == team1_id:
                            logger.info(f"Эта таблица содержит статистику первой команды: {team_name}")
                        elif team_id == team2_id:
                            logger.info(f"Эта таблица содержит статистику второй команды: {team_name}")
                        else:
                            logger.warning(f"Найдена ссылка на неизвестную команду: {team_name} (ID: {team_id})")
                    
                    # Эту строку не добавляем в список игроков
                    continue
                
                # Проверяем, содержит ли строка статистику игрока
                player_link = cells[0].select_one('a')
                if player_link and ('/player/' in player_link.get('href', '') or '/players/' in player_link.get('href', '')):
                    # Это строка с игроком
                    if current_team_id == team1_id:
                        team1_rows.append(row)
                    elif current_team_id == team2_id:
                        team2_rows.append(row)
                    else:
                        # Если ID команды не совпадает ни с одной из известных
                        if current_team_name and team1_name and current_team_name == team1_name:
                            team1_rows.append(row)
                        elif current_team_name and team2_name and current_team_name == team2_name:
                            team2_rows.append(row)
                        else:
                            # Если не смогли определить, добавляем в соответствующую команду по порядку
                            if not team1_rows:
                                team1_rows.append(row)
                            else:
                                team2_rows.append(row)
            
            # Логируем результаты
            logger.info(f"Найдено игроков в первой команде: {len(team1_rows)}")
            logger.info(f"Найдено игроков во второй команде: {len(team2_rows)}")
            
            # На данный момент, похоже, что страница матча содержит статистику только для одной команды
            # Если мы не нашли данные второй команды, отобразим предупреждение
            if team1_rows and not team2_rows:
                logger.info(f"Найдена статистика только для первой команды: {team1_name}")
            elif team2_rows and not team1_rows:
                logger.info(f"Найдена статистика только для второй команды: {team2_name}")
            elif not team1_rows and not team2_rows:
                logger.warning("Не найдена статистика ни для одной команды")
            
            # Обрабатываем статистику игроков первой команды
            if team1_rows:
                for row in team1_rows:
                    player_data = self._extract_player_stats_from_new_format(row, match_id, team1_id)
                    if player_data:
                        players_data.append(player_data)
            
            # Обрабатываем статистику игроков второй команды
            if team2_rows:
                for row in team2_rows:
                    player_data = self._extract_player_stats_from_new_format(row, match_id, team2_id)
                    if player_data:
                        players_data.append(player_data)
            
            return players_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге статистики игроков (новый формат) матча {match_id}: {str(e)}")
            return []
    
    def _extract_name_and_nickname(self, player_text):
        """
        Извлекает полное имя и ник игрока из текста вида "Henrique 'rikz' Waku rikz"
        
        Args:
            player_text (str): Текст с именем и ником игрока
            
        Returns:
            tuple: (full_name, nickname) - полное имя и ник игрока
        """
        if not player_text:
            return None, None
            
        # Убираем лишние пробелы
        player_text = re.sub(r'\s+', ' ', player_text.strip())
        
        # Ищем ник в одинарных кавычках
        nickname_match = re.search(r"'([^']+)'", player_text)
        
        if nickname_match:
            nickname = nickname_match.group(1)
            # Полное имя - это весь текст до повторения ника в конце (если такое есть)
            full_name = player_text
            
            # Проверяем, повторяется ли ник в конце строки
            if player_text.endswith(nickname):
                # Обрезаем повторяющийся ник в конце
                full_name = player_text[:player_text.rfind(nickname)].strip()
            
            return full_name, nickname
        else:
            # Если ник не найден в кавычках, возвращаем весь текст как ник
            return player_text, player_text
    
    def _extract_player_stats_from_new_format(self, row, match_id, team_id):
        """
        Извлекает статистику одного игрока из строки таблицы нового формата
        
        Args:
            row (Tag): Строка таблицы
            match_id (int): ID матча
            team_id (int): ID команды
            
        Returns:
            dict: Словарь с данными игрока или None в случае ошибки
        """
        try:
            # Проверяем, является ли строка заголовком команды или строкой с заголовками колонок
            row_classes = row.get('class', [])
            if 'header-row' in row_classes or 'header' in row_classes or row.select_one('th'):
                logger.info(f"Найдена строка заголовка в матче {match_id}, пропускаем")
                return None
                
            player_data = {
                'match_id': match_id,
                'team_id': team_id,
                'player_id': None,
                'player_nickname': None,
                'fullName': None,
                'nickName': None,
                'kills': None,
                'deaths': None,
                'kd_ratio': None,
                'plus_minus': None,
                'adr': None,
                'kast': None,
                'rating': None
            }
            
            # Извлекаем данные игрока
            cells = row.select(PLAYER_STATS_CELLS)
            
            # Проверяем количество ячеек
            if len(cells) < 2:
                logger.warning(f"Недостаточно ячеек в строке ({len(cells)})")
                return None
            
            # Индексы колонок по умолчанию
            column_map = STATS_TABLE_COLUMNS
            
            # Имя и ID игрока - из первой ячейки
            player_cell = cells[column_map['player']]
            
            # Дополнительная проверка: если ячейка содержит заголовок или имя команды
            if player_cell.select_one('th') or (player_cell.get('class') and ('team-name' in player_cell.get('class') or 'header' in player_cell.get('class'))):
                logger.info(f"Найдена ячейка заголовка в матче {match_id}, пропускаем")
                return None
                
            # Проверяем, содержит ли ячейка только имя команды без ссылки на игрока
            player_link = player_cell.select_one(PLAYER_LINK)
            if not player_link and player_cell.text.strip() in [
                "Iberian Soul", "Monte", "9INE", "CYBERSHOKE", "ENCE", "KOMNATA", 
                "MIGHT", "Moneyball", "Tsunami", "TYLOO", "Rare Atom", "DogEvil", 
                "Leça", "Rhyno", "G2 Ares", "Preasy", "RUSH B", "Zero Tenacity",
                "G2", "Astralis", "Virtus.pro", "BIG", "Natus Vincere", "Aurora",
                "MIBR", "paiN", "FlyQuest", "JiJieHao", "Lynn Vision", "Astrum",
                "Fire Flux", "Nexus", "Partizan", "Spirit Academy", "SINNERS",
                "FAVBET", "Passion UA", "HEROIC Academy", "WOPA", "ESC", "Kubix",
                "CPH Wolves", "Steel Helmet", "GATERON", "Change The Game", "E9",
                "Young Ninjas", "ex-Astralis Talent", "BRUTE", "Sashi", "kONO",
                "SPARTA", "PARIVISION", "AMKAL", "Delta", "ECLOT", "FORZE Reload",
                "Illuminar", "500", "Sangal", "Inner Circle", "Mousquetaires",
                "ENCE Academy", "Volt"
            ]:
                logger.info(f"Найдено имя команды без ссылки на игрока в матче {match_id}: {player_cell.text.strip()}, пропускаем")
                return None

            # Пытаемся найти ссылку на профиль игрока с более точным селектором
            player_profile = player_cell.select_one(PLAYER_PROFILE_LINK)
            if player_profile:
                href = player_profile.get('href', '')
                # Проверяем, что это ссылка на игрока, а не на команду
                if '/player/' in href or '/players/' in href:
                    player_data['player_id'] = self._extract_id_from_url(href)
                    player_nickname_text = re.sub(r'\s+', ' ', player_profile.text.strip().replace('\n', ' ')).strip()
                    player_data['player_nickname'] = player_nickname_text
                    
                    # Извлекаем полное имя и ник игрока
                    full_name, nickname = self._extract_name_and_nickname(player_nickname_text)
                    player_data['fullName'] = full_name
                    player_data['nickName'] = nickname
                    
                    logger.debug(f"Найден игрок: {player_data['player_nickname']} (ID: {player_data['player_id']})")
                elif '/team/' in href:
                    # Если это ссылка на команду, а не на игрока, пропускаем строку
                    logger.info(f"Найдена ссылка на команду, а не на игрока в матче {match_id}: {href}, пропускаем")
                    return None
                else:
                    logger.warning(f"Найдена ссылка не на игрока: {href}")
            else:
                # Пробуем стандартный селектор, если уточненный не сработал
                player_link = player_cell.select_one(PLAYER_LINK)
                if player_link and ('/player/' in player_link.get('href', '') or '/players/' in player_link.get('href', '')):
                    player_data['player_id'] = self._extract_id_from_url(player_link.get('href'))
                    player_nickname_text = re.sub(r'\s+', ' ', player_link.text.strip().replace('\n', ' ')).strip()
                    player_data['player_nickname'] = player_nickname_text
                    
                    # Извлекаем полное имя и ник игрока
                    full_name, nickname = self._extract_name_and_nickname(player_nickname_text)
                    player_data['fullName'] = full_name
                    player_data['nickName'] = nickname
                elif player_link and '/team/' in player_link.get('href', ''):
                    # Если это ссылка на команду, а не на игрока, пропускаем строку
                    logger.info(f"Найдена ссылка на команду, а не на игрока в матче {match_id}: {player_link.get('href')}, пропускаем")
                    return None
                else:
                    # Если нет ссылки на игрока, пытаемся получить хотя бы текст
                    player_name = player_cell.text.strip()
                    if player_name:
                        player_nickname_text = re.sub(r'\s+', ' ', player_name.replace('\n', ' ')).strip()
                        player_data['player_nickname'] = player_nickname_text
                        
                        # Извлекаем полное имя и ник игрока
                        full_name, nickname = self._extract_name_and_nickname(player_nickname_text)
                        player_data['fullName'] = full_name
                        player_data['nickName'] = nickname
                    else:
                        logger.warning(f"Не найдено имя игрока в матче {match_id}")
                        return None
            
            # Извлекаем K-D (убийства-смерти)
            if column_map['kd'] < len(cells):
                kd_text = cells[column_map['kd']].text.strip()
                if kd_text and '-' in kd_text:
                    kd_parts = kd_text.split('-')
                    if len(kd_parts) == 2:
                        try:
                            player_data['kills'] = int(kd_parts[0].strip())
                            player_data['deaths'] = int(kd_parts[1].strip())
                            # Рассчитываем K/D соотношение
                            if player_data['deaths'] > 0:
                                player_data['kd_ratio'] = round(player_data['kills'] / player_data['deaths'], 2)
                        except ValueError:
                            logger.debug(f"Не удалось извлечь убийства/смерти из K-D: {kd_text}")
            
            # Извлекаем +/-
            if column_map['plus_minus'] < len(cells):
                pm_text = cells[column_map['plus_minus']].text.strip()
                if pm_text:
                    try:
                        if pm_text.startswith('+'):
                            player_data['plus_minus'] = int(pm_text[1:])
                        elif pm_text.startswith('-'):
                            player_data['plus_minus'] = -int(pm_text[1:])
                        else:
                            player_data['plus_minus'] = int(pm_text)
                    except ValueError:
                        logger.debug(f"Не удалось извлечь +/- значение: {pm_text}")
            
            # Извлекаем ADR
            if column_map['adr'] < len(cells):
                adr_text = cells[column_map['adr']].text.strip()
                if adr_text:
                    try:
                        player_data['adr'] = float(adr_text)
                    except ValueError:
                        logger.debug(f"Не удалось извлечь ADR: {adr_text}")
            
            # Извлекаем KAST
            if column_map['kast'] < len(cells):
                kast_text = cells[column_map['kast']].text.strip()
                if kast_text:
                    try:
                        # KAST может быть в процентах
                        kast_value = kast_text.replace('%', '')
                        player_data['kast'] = float(kast_value) / 100 if '%' in kast_text else float(kast_value)
                    except ValueError:
                        logger.debug(f"Не удалось извлечь KAST: {kast_text}")
            
            # Извлекаем Rating
            if 'rating' in column_map and column_map['rating'] < len(cells):
                rating_text = cells[column_map['rating']].text.strip()
                if rating_text:
                    try:
                        player_data['rating'] = float(rating_text)
                    except ValueError:
                        logger.debug(f"Не удалось извлечь рейтинг: {rating_text}")
            
            # Проверка на минимально необходимые данные
            if player_data['player_nickname'] is not None and player_data['kills'] is not None:
                return player_data
            else:
                logger.warning(f"Недостаточно данных об игроке в матче {match_id}: {player_data['player_nickname']}")
                return None
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении статистики игрока в матче {match_id}: {str(e)}")
            return None
    
    def _parse_player_stats_with_headers(self, soup, match_id, stats_table, team1_id, team2_id):
        """
        Извлекает статистику игроков из таблицы с заголовками (старый формат HLTV)
        
        Args:
            soup (BeautifulSoup): HTML-документ
            match_id (int): ID матча
            stats_table (Tag): Таблица статистики
            team1_id (int): ID первой команды
            team2_id (int): ID второй команды
            
        Returns:
            list: Список словарей с данными игроков
        """
        players_data = []
        
        try:
            # Анализируем структуру таблицы для определения колонок
            column_map = self._analyze_table_structure(stats_table)
            if not column_map:
                logger.warning(f"Не удалось определить структуру таблицы для матча {match_id}")
                return players_data
                
            logger.info(f"Определена структура таблицы: {column_map}")
            
            # Определяем строки для каждой команды
            team1_rows = []
            team2_rows = []
            
            # Флаг для определения, читаем ли мы строки первой или второй команды
            current_team = 1
            
            # Проходим по строкам таблицы
            for row in stats_table.select('tr'):
                # Если нашли разделитель команд (строку с .totalstats), меняем текущую команду
                if row.select_one(TOTAL_STATS_ROW) or 'totalstats' in row.get('class', []):
                    current_team = 2
                    continue
                
                # Пропускаем заголовки
                if row.select_one('th'):
                    continue
                
                # Добавляем строку в соответствующий список
                if current_team == 1:
                    team1_rows.append(row)
                else:
                    team2_rows.append(row)
            
            # Обрабатываем статистику игроков первой команды
            for row in team1_rows:
                player_data = self._extract_player_stats(row, match_id, team1_id, column_map)
                if player_data:
                    players_data.append(player_data)
            
            # Обрабатываем статистику игроков второй команды
            for row in team2_rows:
                player_data = self._extract_player_stats(row, match_id, team2_id, column_map)
                if player_data:
                    players_data.append(player_data)
            
            return players_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге статистики игроков матча {match_id}: {str(e)}")
            return []
    
    def _analyze_table_structure(self, stats_table):
        """
        Анализирует структуру таблицы статистики для определения порядка колонок
        
        Args:
            stats_table: Таблица со статистикой
            
        Returns:
            dict: Маппинг колонок на их индексы
        """
        # Ищем заголовки таблицы
        headers = stats_table.select('th')
        if not headers:
            logger.warning("Заголовки таблицы не найдены")
            return None
        
        # Определяем индексы колонок по заголовкам
        column_map = {}
        for i, header in enumerate(headers):
            header_text = header.text.strip().lower()
            
            if header_text == 'player' or not header_text:
                column_map['player'] = i
            elif header_text == 'k' or header_text == 'kills':
                column_map['kills'] = i
            elif header_text == 'd' or header_text == 'deaths':
                column_map['deaths'] = i
            elif header_text == '+/-' or header_text == 'plus_minus':
                column_map['plus_minus'] = i
            elif header_text == 'adr':
                column_map['adr'] = i
            elif header_text == 'kast':
                column_map['kast'] = i
            elif header_text == 'rating' or header_text == 'rating 2.0':
                column_map['rating'] = i
            elif header_text == 'k/d' or header_text == 'kd':
                column_map['kd_ratio'] = i
        
        # Если не нашли некоторые колонки, используем предполагаемые индексы
        if 'player' not in column_map:
            column_map['player'] = 0
        if 'kills' not in column_map and len(headers) > 1:
            column_map['kills'] = 1
        if 'deaths' not in column_map and len(headers) > 2:
            column_map['deaths'] = 2
        if 'kd_ratio' not in column_map and len(headers) > 3:
            column_map['kd_ratio'] = 3
        if 'plus_minus' not in column_map and len(headers) > 4:
            column_map['plus_minus'] = 4
        if 'adr' not in column_map and len(headers) > 5:
            column_map['adr'] = 5
        if 'kast' not in column_map and len(headers) > 6:
            column_map['kast'] = 6
        if 'rating' not in column_map and len(headers) > 7:
            column_map['rating'] = 7
            
        return column_map
    
    def _extract_player_stats(self, row, match_id, team_id, column_map):
        """
        Извлекает статистику одного игрока из строки таблицы с учетом карты колонок (для старого формата)
        
        Args:
            row (Tag): Строка таблицы
            match_id (int): ID матча
            team_id (int): ID команды
            column_map (dict): Маппинг колонок на их индексы
            
        Returns:
            dict: Словарь с данными игрока или None в случае ошибки
        """
        try:
            player_data = {
                'match_id': match_id,
                'team_id': team_id,
                'player_id': None,
                'player_nickname': None,
                'fullName': None,
                'nickName': None,
                'kills': None,
                'deaths': None,
                'kd_ratio': None,
                'plus_minus': None,
                'adr': None,
                'kast': None,
                'rating': None
            }
            
            # Извлекаем данные игрока
            cells = row.select(PLAYER_STATS_CELLS)
            
            # Проверяем количество ячеек
            if len(cells) < 2:
                logger.warning(f"Недостаточно ячеек в строке ({len(cells)})")
                return None
            
            # Индекс ячейки с именем игрока
            player_index = column_map.get('player', 0)
            if player_index >= len(cells):
                player_index = 0
            
            # Получаем ячейку с именем игрока
            player_cell = cells[player_index]
            
            # Проверяем наличие специального класса для ячейки игрока
            if player_cell.select_one(PLAYER_CELL):
                player_cell = player_cell.select_one(PLAYER_CELL)
            
            # Дополнительная проверка: если ячейка содержит заголовок или имя команды
            if player_cell.select_one('th') or (player_cell.get('class') and ('team-name' in player_cell.get('class') or 'header' in player_cell.get('class'))):
                logger.info(f"Найдена ячейка заголовка в матче {match_id}, пропускаем")
                return None
                
            # Проверяем, содержит ли ячейка только имя команды без ссылки на игрока
            player_link = player_cell.select_one(PLAYER_LINK)
            if not player_link and player_cell.text.strip() in [
                "Iberian Soul", "Monte", "9INE", "CYBERSHOKE", "ENCE", "KOMNATA", 
                "MIGHT", "Moneyball", "Tsunami", "TYLOO", "Rare Atom", "DogEvil", 
                "Leça", "Rhyno", "G2 Ares", "Preasy", "RUSH B", "Zero Tenacity",
                "G2", "Astralis", "Virtus.pro", "BIG", "Natus Vincere", "Aurora",
                "MIBR", "paiN", "FlyQuest", "JiJieHao", "Lynn Vision", "Astrum",
                "Fire Flux", "Nexus", "Partizan", "Spirit Academy", "SINNERS",
                "FAVBET", "Passion UA", "HEROIC Academy", "WOPA", "ESC", "Kubix",
                "CPH Wolves", "Steel Helmet", "GATERON", "Change The Game", "E9",
                "Young Ninjas", "ex-Astralis Talent", "BRUTE", "Sashi", "kONO",
                "SPARTA", "PARIVISION", "AMKAL", "Delta", "ECLOT", "FORZE Reload",
                "Illuminar", "500", "Sangal", "Inner Circle", "Mousquetaires",
                "ENCE Academy", "Volt"
            ]:
                logger.info(f"Найдено имя команды без ссылки на игрока в матче {match_id}: {player_cell.text.strip()}, пропускаем")
                return None

            # Пытаемся найти ссылку на профиль игрока с более точным селектором
            player_profile = player_cell.select_one(PLAYER_PROFILE_LINK)
            if player_profile:
                href = player_profile.get('href', '')
                # Проверяем, что это ссылка на игрока, а не на команду
                if '/player/' in href or '/players/' in href:
                    player_data['player_id'] = self._extract_id_from_url(href)
                    player_nickname_text = re.sub(r'\s+', ' ', player_profile.text.strip().replace('\n', ' ')).strip()
                    player_data['player_nickname'] = player_nickname_text
                    
                    # Извлекаем полное имя и ник игрока
                    full_name, nickname = self._extract_name_and_nickname(player_nickname_text)
                    player_data['fullName'] = full_name
                    player_data['nickName'] = nickname
                    
                    logger.debug(f"Найден игрок: {player_data['player_nickname']} (ID: {player_data['player_id']})")
                elif '/team/' in href:
                    # Если это ссылка на команду, а не на игрока, пропускаем строку
                    logger.info(f"Найдена ссылка на команду, а не на игрока в матче {match_id}: {href}, пропускаем")
                    return None
                else:
                    logger.warning(f"Найдена ссылка не на игрока: {href}")
            else:
                # Пробуем стандартный селектор, если уточненный не сработал
                player_link = player_cell.select_one(PLAYER_LINK)
                if player_link and ('/player/' in player_link.get('href', '') or '/players/' in player_link.get('href', '')):
                    player_data['player_id'] = self._extract_id_from_url(player_link.get('href'))
                    player_nickname_text = re.sub(r'\s+', ' ', player_link.text.strip().replace('\n', ' ')).strip()
                    player_data['player_nickname'] = player_nickname_text
                    
                    # Извлекаем полное имя и ник игрока
                    full_name, nickname = self._extract_name_and_nickname(player_nickname_text)
                    player_data['fullName'] = full_name
                    player_data['nickName'] = nickname
                elif player_link and '/team/' in player_link.get('href', ''):
                    # Если это ссылка на команду, а не на игрока, пропускаем строку
                    logger.info(f"Найдена ссылка на команду, а не на игрока в матче {match_id}: {player_link.get('href')}, пропускаем")
                    return None
                else:
                    # Если нет ссылки на игрока, пытаемся получить хотя бы текст
                    player_name = player_cell.text.strip()
                    if player_name:
                        player_nickname_text = re.sub(r'\s+', ' ', player_name.replace('\n', ' ')).strip()
                        player_data['player_nickname'] = player_nickname_text
                        
                        # Извлекаем полное имя и ник игрока
                        full_name, nickname = self._extract_name_and_nickname(player_nickname_text)
                        player_data['fullName'] = full_name
                        player_data['nickName'] = nickname
                    else:
                        logger.warning(f"Не найдено имя игрока в матче {match_id}")
                        return None
            
            # Извлекаем статистику по карте колонок
            for stat, index in column_map.items():
                if stat == 'player':
                    continue  # Уже обработали имя игрока
                
                if index < len(cells):
                    try:
                        cell_value = cells[index].text.strip()
                        
                        if not cell_value:
                            continue
                        
                        if stat == 'kills':
                            player_data['kills'] = int(cell_value)
                        elif stat == 'deaths':
                            player_data['deaths'] = int(cell_value)
                        elif stat == 'kd_ratio':
                            player_data['kd_ratio'] = float(cell_value)
                        elif stat == 'plus_minus':
                            # Обрабатываем различные форматы +/-
                            if cell_value.startswith('+'):
                                player_data['plus_minus'] = int(cell_value[1:])
                            elif cell_value.startswith('-'):
                                player_data['plus_minus'] = -int(cell_value[1:])
                            else:
                                player_data['plus_minus'] = int(cell_value)
                        elif stat == 'adr':
                            player_data['adr'] = float(cell_value)
                        elif stat == 'kast':
                            # KAST может быть в формате процентов
                            kast_value = cell_value.replace('%', '')
                            player_data['kast'] = float(kast_value) / 100 if '%' in cell_value else float(kast_value)
                        elif stat == 'rating':
                            player_data['rating'] = float(cell_value)
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Не удалось извлечь {stat} для игрока {player_data['player_nickname']}: {str(e)}")
            
            # Проверка на минимально необходимые данные
            if player_data['player_nickname'] is not None:
                return player_data
            else:
                logger.warning(f"Недостаточно данных об игроке в матче {match_id}")
                return None
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении статистики игрока в матче {match_id}: {str(e)}")
            return None
    
    def _save_match_details(self, match_data):
        """
        Сохраняет детали матча в базу данных
        
        Args:
            match_data (dict): Данные матча
            
        Returns:
            bool: True если успешно, False если ошибка
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Проверяем, существует ли уже запись для этого матча
            cursor.execute('SELECT 1 FROM match_details WHERE match_id = ?', (match_data['match_id'],))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Обновляем существующую запись
                cursor.execute('''
                    UPDATE match_details SET 
                    datetime = ?,
                    team1_id = ?,
                    team1_name = ?,
                    team1_score = ?,
                    team1_rank = ?,
                    team2_id = ?,
                    team2_name = ?,
                    team2_score = ?,
                    team2_rank = ?,
                    event_id = ?,
                    event_name = ?,
                    demo_id = ?,
                    head_to_head_team1_wins = ?,
                    head_to_head_team2_wins = ?,
                    status = ?,
                    parsed_at = CURRENT_TIMESTAMP
                    WHERE match_id = ?
                ''', (
                    match_data['datetime'],
                    match_data['team1_id'],
                    match_data['team1_name'],
                    match_data['team1_score'],
                    match_data['team1_rank'],
                    match_data['team2_id'],
                    match_data['team2_name'],
                    match_data['team2_score'],
                    match_data['team2_rank'],
                    match_data['event_id'],
                    match_data['event_name'],
                    match_data['demo_id'],
                    match_data['head_to_head_team1_wins'],
                    match_data['head_to_head_team2_wins'],
                    match_data['status'],
                    match_data['match_id']
                ))
                logger.info(f"Обновлены детали матча {match_data['match_id']}")
            else:
                # Добавляем новую запись
                cursor.execute('''
                    INSERT INTO match_details (
                    match_id, datetime, team1_id, team1_name, team1_score, team1_rank,
                    team2_id, team2_name, team2_score, team2_rank, event_id, event_name,
                    demo_id, head_to_head_team1_wins, head_to_head_team2_wins, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_data['match_id'],
                    match_data['datetime'],
                    match_data['team1_id'],
                    match_data['team1_name'],
                    match_data['team1_score'],
                    match_data['team1_rank'],
                    match_data['team2_id'],
                    match_data['team2_name'],
                    match_data['team2_score'],
                    match_data['team2_rank'],
                    match_data['event_id'],
                    match_data['event_name'],
                    match_data['demo_id'],
                    match_data['head_to_head_team1_wins'],
                    match_data['head_to_head_team2_wins'],
                    match_data['status']
                ))
                logger.info(f"Добавлены детали матча {match_data['match_id']}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении деталей матча {match_data['match_id']}: {str(e)}")
            return False
    
    def _save_player_stats(self, players_data):
        """
        Сохраняет статистику игроков в базу данных
        
        Args:
            players_data (list): Список с данными игроков
            
        Returns:
            bool: True если успешно, False если ошибка
        """
        if not players_data:
            logger.warning("Нет данных игроков для сохранения")
            return False
            
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Удаляем существующие записи для этого матча
            match_id = players_data[0]['match_id']
            cursor.execute('DELETE FROM player_stats WHERE match_id = ?', (match_id,))
            
            # Добавляем новые записи
            for player_data in players_data:
                cursor.execute('''
                    INSERT INTO player_stats (
                    match_id, team_id, player_id, player_nickname,
                    fullName, nickName,
                    kills, deaths, kd_ratio, plus_minus,
                    adr, kast, rating
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    player_data['match_id'],
                    player_data['team_id'],
                    player_data['player_id'],
                    player_data['player_nickname'],
                    player_data['fullName'],
                    player_data['nickName'],
                    player_data['kills'],
                    player_data['deaths'],
                    player_data['kd_ratio'],
                    player_data['plus_minus'],
                    player_data['adr'],
                    player_data['kast'],
                    player_data['rating']
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Сохранена статистика {len(players_data)} игроков для матча {match_id}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении статистики игроков: {str(e)}")
            return False

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Проверяем, передан ли аргумент с ID матча для дебага
    import sys
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        match_id = int(sys.argv[1])
        logger.info(f"Запуск дебага для матча ID {match_id}")
        
        # Создаем экземпляр коллектора
        collector = MatchDetailsCollector()
        
        # Ищем файл матча
        import glob
        import os
        
        match_files = glob.glob(os.path.join(collector.html_dir, f'match_{match_id}*.html'))
        
        if not match_files:
            logger.error(f"Файл матча ID {match_id} не найден")
            sys.exit(1)
        
        file_path = match_files[0]
        logger.info(f"Файл матча найден: {os.path.basename(file_path)}")
        
        # Читаем HTML-файл
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Парсим HTML
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Проверяем наличие таблицы статистики
        stats_table = soup.select_one('.stats-content table')
        if not stats_table:
            logger.error("Таблица статистики не найдена в HTML")
            
            # Проверяем наличие других элементов
            logger.info("Проверка других элементов страницы...")
            
            # Проверка статуса матча
            match_status = soup.select_one('.match-page .team1-gradient, .match-page .team2-gradient')
            if match_status:
                logger.info(f"Найден блок команд: {match_status.get_text().strip()[:100]}...")
            else:
                logger.info("Блок команд не найден")
            
            # Проверяем, есть ли сообщение, что матч еще не начался или не завершен
            status_text = soup.select_one('.match-page .padding')
            if status_text:
                logger.info(f"Текст статуса: {status_text.get_text().strip()[:100]}...")
            
            sys.exit(1)
        
        # Анализируем структуру таблицы
        logger.info("Анализ структуры таблицы статистики:")
        rows = stats_table.select('tr')
        logger.info(f"Всего строк в таблице: {len(rows)}")
        
        # Проверяем заголовки
        header_row = stats_table.select_one('tr')
        if header_row:
            headers = header_row.select('th')
            logger.info(f"Заголовки таблицы ({len(headers)}): {[h.get_text().strip() for h in headers]}")
        
        # Получаем команды
        team1_rows = []
        team2_rows = []
        current_team = 1
        
        for row in rows:
            # Если нашли разделитель команд, меняем текущую команду
            if row.select_one('.totalstats'):
                current_team = 2
                logger.info(f"Найден разделитель команд: {row.get_text().strip()[:50]}...")
                continue
            
            # Пропускаем заголовки
            if row.select_one('th'):
                continue
            
            # Добавляем строку в соответствующий список
            if current_team == 1:
                team1_rows.append(row)
            else:
                team2_rows.append(row)
        
        logger.info(f"Найдено игроков в первой команде: {len(team1_rows)}")
        logger.info(f"Найдено игроков во второй команде: {len(team2_rows)}")
        
        # Анализируем строки игроков
        for team_idx, team_rows in enumerate([team1_rows, team2_rows], 1):
            logger.info(f"Анализ игроков команды {team_idx}:")
            
            for player_idx, row in enumerate(team_rows, 1):
                cells = row.select('td')
                logger.info(f"  Игрок {player_idx}: {len(cells)} ячеек")
                
                player_link = row.select_one('a')
                if player_link:
                    player_name = player_link.get_text().strip()
                    player_url = player_link.get('href', '')
                    logger.info(f"    Имя: {player_name}, URL: {player_url}")
                else:
                    logger.info("    Ссылка на игрока не найдена")
                
                # Выводим содержимое всех ячеек
                for cell_idx, cell in enumerate(cells):
                    try:
                        cell_text = cell.get_text().strip()
                        logger.info(f"    Ячейка {cell_idx}: '{cell_text}'")
                    except Exception as e:
                        logger.error(f"    Ошибка при получении текста ячейки {cell_idx}: {str(e)}")
        
        # Тестовый парсинг
        logger.info("Попытка извлечения данных...")
        
        # Извлекаем данные матча
        match_data = collector._parse_match_details(soup, match_id)
        if match_data:
            logger.info("Данные матча успешно извлечены:")
            for key, value in match_data.items():
                logger.info(f"  {key}: {value}")
        else:
            logger.error("Не удалось извлечь данные матча")
        
        # Извлекаем статистику игроков
        players_data = collector._parse_player_stats(soup, match_id)
        if players_data:
            logger.info(f"Успешно извлечена статистика {len(players_data)} игроков:")
            for player_idx, player in enumerate(players_data, 1):
                logger.info(f"  Игрок {player_idx}: {player['player_nickname']}")
                for key, value in player.items():
                    if key != 'player_nickname' and key != 'match_id':
                        logger.info(f"    {key}: {value}")
        else:
            logger.error("Не удалось извлечь статистику игроков")
            
    else:
        # Запуск сбора данных
        collector = MatchDetailsCollector()
        stats = collector.collect()
        
        # Вывод статистики
        logger.info(f"Обработано {stats['processed_files']} файлов из {stats['total_files']}")
        logger.info(f"Успешно извлечено данных матчей: {stats['successful_match_details']}")
        logger.info(f"Успешно извлечено данных игроков: {stats['successful_player_stats']}")
        logger.info(f"Ошибок: {stats['errors']}") 