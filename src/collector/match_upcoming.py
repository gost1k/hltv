"""
Модуль для извлечения данных из HTML-файлов предстоящих матчей
и сохранения их в таблицы match_upcoming и match_upcoming_players
"""
import os
import re
import logging
import sqlite3
from bs4 import BeautifulSoup
import glob
from datetime import datetime
import time
from src.config.constants import MATCH_UPCOMING_DIR
from src.config.selectors import *

# Настройка логирования
logger = logging.getLogger(__name__)

class MatchUpcomingCollector:
    """
    Класс для извлечения данных из HTML-файлов предстоящих матчей и их сохранения в БД
    """
    def __init__(self, html_dir=MATCH_UPCOMING_DIR, db_path="hltv.db"):
        """
        Инициализация коллектора предстоящих матчей
        
        Args:
            html_dir (str): Путь к директории с HTML-файлами предстоящих матчей
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
        logger.info("Начинаем сбор данных предстоящих матчей")
        
        # Статистика обработки
        stats = {
            'total_files': 0,
            'processed_files': 0,
            'successful_match_data': 0,
            'successful_player_data': 0,
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
                    stats['successful_match_data'] += 1
                    stats['successful_player_data'] += 1
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
        
        logger.info(f"Завершен сбор данных предстоящих матчей. Обработано {stats['processed_files']} из {stats['total_files']} файлов")
        logger.info(f"Успешно: {stats['successful_match_data']}, Уже существуют: {stats['already_exists']}, Ошибок: {stats['errors']}")
        
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
        logger.info(f"Найдено {len(html_files)} HTML-файлов с предстоящими матчами")
        
        # В отличие от прошедших матчей, для предстоящих мы всегда обрабатываем все файлы
        # так как информация может меняться
        return html_files
    
    def process_file(self, file_path):
        """
        Обрабатывает один HTML-файл с деталями предстоящего матча
        
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
            
            logger.info(f"Обработка файла {file_path} для матча {match_id}")
            
            # Читаем HTML-файл
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            # Парсим HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Извлекаем данные матча
            match_data = self._parse_match_details(soup, match_id)
            
            if not match_data:
                logger.error(f"Не удалось извлечь данные матча из {file_path}")
                return "error"
                
            # Сохраняем данные матча в БД
            self._save_match_details(match_data)
            
            # Извлекаем данные игроков
            players_data = self._parse_player_data(soup, match_id)
            
            if players_data:
                # Сохраняем данные игроков в БД
                self._save_player_data(players_data)
                
            logger.info(f"Успешно обработан файл {file_path}")
            return "success"
            
        except Exception as e:
            logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}")
            return "error"
    
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
        Извлекает данные о предстоящем матче из HTML
        
        Args:
            soup (BeautifulSoup): HTML-документ
            match_id (int): ID матча
            
        Returns:
            dict: Словарь с данными матча или None в случае ошибки
        """
        try:
            # Статус по умолчанию - upcoming
            match_status = STATUS_UPCOMING
            
            # Проверяем, начался ли матч по тексту в блоке countdown
            match_live_element = soup.select_one(COUNTDOWN)
            if match_live_element and "LIVE" in match_live_element.text:
                match_status = "live"
            
            match_data = {
                'match_id': match_id,
                'datetime': None,
                'team1_id': None,
                'team1_name': None,
                'team1_rank': None,
                'team2_id': None,
                'team2_name': None,
                'team2_rank': None,
                'event_id': None,
                'event_name': None,
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
            logger.info(f"Матч {match_id} определен как {match_status}")
            
            return match_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге данных матча {match_id}: {str(e)}")
            return None
    
    def _parse_player_data(self, soup, match_id):
        """
        Извлекает данные об игроках из HTML
        
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
            team1_name = None
            team2_name = None
            
            team1_element = soup.select_one(f"{TEAM1_GRADIENT} {PLAYER_LINK}")
            if team1_element:
                team1_id = self._extract_id_from_url(team1_element.get('href'))
            
            team2_element = soup.select_one(f"{TEAM2_GRADIENT} {PLAYER_LINK}")
            if team2_element:
                team2_id = self._extract_id_from_url(team2_element.get('href'))
                
            team1_name_element = soup.select_one(f"{TEAM1_GRADIENT} {TEAM_NAME}")
            if team1_name_element:
                team1_name = team1_name_element.text.strip()
                
            team2_name_element = soup.select_one(f"{TEAM2_GRADIENT} {TEAM_NAME}")
            if team2_name_element:
                team2_name = team2_name_element.text.strip()
            
            # Найдем линейки (lineups) команд
            lineup_container = soup.select_one('.lineups')
            if not lineup_container:
                logger.warning(f"Контейнер с линейками не найден для матча {match_id}")
                return players_data
            
            # Найдем все блоки с командами
            team_containers = lineup_container.select('.lineup')
            
            if len(team_containers) < 2:
                logger.warning(f"Недостаточно блоков с линейками команд для матча {match_id}")
                return players_data
            
            # Первая команда
            team1_container = team_containers[0]
            team1_players = team1_container.select('.player-container')
            
            for player in team1_players:
                player_data = self._extract_player_data(player, match_id, team1_id)
                if player_data:
                    players_data.append(player_data)
            
            # Вторая команда
            team2_container = team_containers[1]
            team2_players = team2_container.select('.player-container')
            
            for player in team2_players:
                player_data = self._extract_player_data(player, match_id, team2_id)
                if player_data:
                    players_data.append(player_data)
            
            logger.info(f"Извлечены данные {len(players_data)} игроков для матча {match_id}")
            return players_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге данных игроков матча {match_id}: {str(e)}")
            return []
    
    def _extract_player_data(self, player_element, match_id, team_id):
        """
        Извлекает данные одного игрока из HTML элемента
        
        Args:
            player_element: HTML элемент с данными игрока
            match_id (int): ID матча
            team_id (int): ID команды
            
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
                'nickName': None
            }
            
            # Получаем ссылку на игрока
            player_link = player_element.select_one('a')
            if player_link:
                player_data['player_id'] = self._extract_id_from_url(player_link.get('href'))
                
                # Получаем ник игрока
                player_nickname = player_link.select_one('.nick')
                if player_nickname:
                    player_data['nickName'] = player_nickname.text.strip()
                    player_data['player_nickname'] = player_nickname.text.strip()
                
                # Получаем полное имя игрока
                player_name = player_link.select_one('.name')
                if player_name:
                    player_data['fullName'] = player_name.text.strip()
            
            # Проверяем, что у нас есть хотя бы ник игрока
            if player_data['player_nickname']:
                return player_data
            else:
                logger.warning(f"Недостаточно данных об игроке в матче {match_id}")
                return None
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении данных игрока в матче {match_id}: {str(e)}")
            return None
    
    def _save_match_details(self, match_data):
        """
        Сохраняет данные предстоящего матча в базу данных
        
        Args:
            match_data (dict): Данные матча
            
        Returns:
            bool: True если успешно, False если ошибка
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Проверяем, существует ли уже запись для этого матча
            cursor.execute('SELECT 1 FROM match_upcoming WHERE match_id = ?', (match_data['match_id'],))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Обновляем существующую запись
                cursor.execute('''
                    UPDATE match_upcoming SET 
                    datetime = ?,
                    team1_id = ?,
                    team1_name = ?,
                    team1_rank = ?,
                    team2_id = ?,
                    team2_name = ?,
                    team2_rank = ?,
                    event_id = ?,
                    event_name = ?,
                    head_to_head_team1_wins = ?,
                    head_to_head_team2_wins = ?,
                    status = ?,
                    parsed_at = CURRENT_TIMESTAMP
                    WHERE match_id = ?
                ''', (
                    match_data['datetime'],
                    match_data['team1_id'],
                    match_data['team1_name'],
                    match_data['team1_rank'],
                    match_data['team2_id'],
                    match_data['team2_name'],
                    match_data['team2_rank'],
                    match_data['event_id'],
                    match_data['event_name'],
                    match_data['head_to_head_team1_wins'],
                    match_data['head_to_head_team2_wins'],
                    match_data['status'],
                    match_data['match_id']
                ))
                logger.info(f"Обновлены данные предстоящего матча {match_data['match_id']}")
            else:
                # Добавляем новую запись
                cursor.execute('''
                    INSERT INTO match_upcoming (
                    match_id, datetime, team1_id, team1_name, team1_rank,
                    team2_id, team2_name, team2_rank, event_id, event_name,
                    head_to_head_team1_wins, head_to_head_team2_wins, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_data['match_id'],
                    match_data['datetime'],
                    match_data['team1_id'],
                    match_data['team1_name'],
                    match_data['team1_rank'],
                    match_data['team2_id'],
                    match_data['team2_name'],
                    match_data['team2_rank'],
                    match_data['event_id'],
                    match_data['event_name'],
                    match_data['head_to_head_team1_wins'],
                    match_data['head_to_head_team2_wins'],
                    match_data['status']
                ))
                logger.info(f"Добавлены данные предстоящего матча {match_data['match_id']}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных матча {match_data['match_id']}: {str(e)}")
            return False
    
    def _save_player_data(self, players_data):
        """
        Сохраняет данные игроков в базу данных
        
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
            cursor.execute('DELETE FROM match_upcoming_players WHERE match_id = ?', (match_id,))
            
            # Добавляем новые записи
            for player_data in players_data:
                cursor.execute('''
                    INSERT INTO match_upcoming_players (
                    match_id, team_id, player_id, player_nickname,
                    fullName, nickName
                    ) VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    player_data['match_id'],
                    player_data['team_id'],
                    player_data['player_id'],
                    player_data['player_nickname'],
                    player_data['fullName'],
                    player_data['nickName']
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Сохранены данные {len(players_data)} игроков для матча {match_id}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении данных игроков: {str(e)}")
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
    
    # Создаем экземпляр коллектора и запускаем сбор данных
    collector = MatchUpcomingCollector()
    stats = collector.collect()
    
    # Вывод статистики
    logger.info(f"Обработано {stats['processed_files']} файлов из {stats['total_files']}")
    logger.info(f"Успешно извлечено данных матчей: {stats['successful_match_data']}")
    logger.info(f"Успешно извлечено данных игроков: {stats['successful_player_data']}")
    logger.info(f"Ошибок: {stats['errors']}") 