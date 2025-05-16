"""
Модуль для извлечения данных из HTML-файлов предстоящих матчей
и сохранения их в таблицы upcoming_match и upcoming_match_players
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
import json

# Настройка логирования
logger = logging.getLogger(__name__)

# Директории для JSON файлов
JSON_OUTPUT_DIR = "storage/json"
UPCOMING_MATCH_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "upcoming_match")
UPCOMING_PLAYERS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "upcoming_players")
UPCOMING_STREAMERS_JSON_DIR = os.path.join(JSON_OUTPUT_DIR, "upcoming_streams")
os.makedirs(UPCOMING_MATCH_JSON_DIR, exist_ok=True)
os.makedirs(UPCOMING_PLAYERS_JSON_DIR, exist_ok=True)
os.makedirs(UPCOMING_STREAMERS_JSON_DIR, exist_ok=True)

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
                
            # Добавляем дату обработки
            match_data['parsed_at'] = datetime.now().isoformat()
            
            # Сохраняем детали матча в JSON
            self._save_match_details_to_json(match_data)
            
            # Проверяем, есть ли определенные команды в матче
            # Если обе команды TBD, игроков точно нет, пропускаем сбор
            if match_data['team1_name'] == "TBD" and match_data['team2_name'] == "TBD":
                logger.info(f"Матч {match_id} не имеет определенных команд, пропускаем сбор данных игроков")
                # Удаляем файл после успешной обработки
                try:
                    os.remove(file_path)
                    logger.info(f"Файл {file_path} успешно удален")
                except Exception as e:
                    logger.warning(f"Не удалось удалить файл {file_path}: {str(e)}")
                return "success"
            
            # Извлекаем данные игроков
            players_data = self._parse_player_data(soup, match_id)
            
            if players_data:
                # Сохраняем данные игроков в JSON
                self._save_players_to_json(match_id, players_data)
                
            # Извлекаем данные стримеров
            streamers_data = self._parse_streamers_data(soup, match_id)
            if streamers_data:
                self._save_streamers_to_json(match_id, streamers_data)
            
            logger.info(f"Успешно обработан файл {file_path}")
            
            # Удаляем файл после успешной обработки
            try:
                os.remove(file_path)
                logger.info(f"Файл {file_path} успешно удален")
            except Exception as e:
                logger.warning(f"Не удалось удалить файл {file_path}: {str(e)}")
                
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
                match_status = STATUS_LIVE
            
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
            
            # Проверяем команду 1 на наличие класса .noteam
            team1_element = soup.select_one(TEAM1_GRADIENT)
            team1_is_noteam = team1_element and NOTEAM.lstrip('.') in team1_element.get('class', [])
            
            if team1_element and not team1_is_noteam:
                team1_link = team1_element.select_one(PLAYER_LINK)
                if team1_link:
                    match_data['team1_id'] = self._extract_id_from_url(team1_link.get('href'))
                
                team1_name_element = team1_element.select_one(TEAM_NAME)
                if team1_name_element:
                    match_data['team1_name'] = team1_name_element.text.strip()
            elif team1_is_noteam:
                match_data['team1_name'] = "TBD"  # To Be Determined
            
            # Проверяем команду 2 на наличие класса .noteam
            team2_element = soup.select_one(TEAM2_GRADIENT)
            team2_is_noteam = team2_element and NOTEAM.lstrip('.') in team2_element.get('class', [])
            
            if team2_element and not team2_is_noteam:
                team2_link = team2_element.select_one(PLAYER_LINK)
                if team2_link:
                    match_data['team2_id'] = self._extract_id_from_url(team2_link.get('href'))
                
                team2_name_element = team2_element.select_one(TEAM_NAME)
                if team2_name_element:
                    match_data['team2_name'] = team2_name_element.text.strip()
            elif team2_is_noteam:
                match_data['team2_name'] = "TBD"  # To Be Determined
            
            # Извлекаем рейтинги команд (только если команды определены)
            if not team1_is_noteam:
                team1_rank_element = soup.select_one(TEAM1_RANK)
                if team1_rank_element:
                    match_data['team1_rank'] = self._extract_rank(team1_rank_element.text.strip())
            
            if not team2_is_noteam:
                team2_rank_element = soup.select_one(TEAM2_RANK)
                if team2_rank_element:
                    match_data['team2_rank'] = self._extract_rank(team2_rank_element.text.strip())
            
            # Извлекаем данные о событии
            event_element = soup.select_one(EVENT)
            if event_element:
                match_data['event_id'] = self._extract_id_from_url(event_element.get('href'))
                match_data['event_name'] = event_element.text.strip()
            
            # Извлекаем данные об очных встречах (только если обе команды определены)
            if not team1_is_noteam and not team2_is_noteam:
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
            # Проверяем наличие блока с линейками команд
            lineups_container = soup.select_one(LINEUPS_CONTAINER)
            if not lineups_container:
                logger.info(f"Блок с линейками команд не найден для матча {match_id}")
                return players_data
            
            # Получаем ID команд и проверяем, определены ли команды
            team1_element = soup.select_one(TEAM1_GRADIENT)
            team2_element = soup.select_one(TEAM2_GRADIENT)
            
            team1_is_noteam = team1_element and NOTEAM.lstrip('.') in team1_element.get('class', [])
            team2_is_noteam = team2_element and NOTEAM.lstrip('.') in team2_element.get('class', [])
            
            # Если обе команды не определены, вернем пустой список
            if team1_is_noteam and team2_is_noteam:
                logger.info(f"Обе команды для матча {match_id} еще не определены, пропускаем парсинг игроков")
                return players_data
            
            team1_id = None
            team2_id = None
            
            # Получаем ID команд, если они определены
            if not team1_is_noteam and team1_element:
                team1_link = team1_element.select_one(PLAYER_LINK)
                if team1_link:
                    team1_id = self._extract_id_from_url(team1_link.get('href'))
                    logger.debug(f"ID первой команды: {team1_id}")
            
            if not team2_is_noteam and team2_element:
                team2_link = team2_element.select_one(PLAYER_LINK)
                if team2_link:
                    team2_id = self._extract_id_from_url(team2_link.get('href'))
                    logger.debug(f"ID второй команды: {team2_id}")
            
            # Ищем все блоки .players
            players_divs = lineups_container.select('.players')
            if not players_divs or len(players_divs) == 0:
                logger.info(f"Блоки с игроками не найдены для матча {match_id}")
                return players_data
            
            logger.debug(f"Найдено {len(players_divs)} блоков с игроками")
            
            # Обрабатываем все блоки с игроками
            total_players = 0
            
            for player_div in players_divs:
                # Ищем таблицу в текущем блоке .players
                players_table = player_div.select_one('table')
                if not players_table:
                    continue
                
                # Ищем строки таблицы
                rows = players_table.select('tr')
                if len(rows) <= 1:
                    continue
                
                # Обычно игроки находятся во второй строке таблицы
                player_row = rows[1]
                
                # Получаем все ячейки с игроками
                player_cells = player_row.select('td.player')
                logger.debug(f"Найдено {len(player_cells)} ячеек с игроками в текущем блоке")
                
                # Анализируем каждую ячейку с игроком
                for cell in player_cells:
                    # Получаем информацию о команде игрока из атрибута data-team-ordinal
                    player_compare = cell.select_one('.player-compare')
                    if not player_compare:
                        continue
                    
                    team_ordinal = player_compare.get('data-team-ordinal')
                    # Определяем, к какой команде относится игрок (1 - первая команда, 2 - вторая команда)
                    current_team_id = team1_id if team_ordinal == "1" else team2_id
                    
                    # Извлекаем данные игрока
                    player_data = self._extract_player_data_from_cell(cell, match_id, current_team_id)
                    if player_data:
                        players_data.append(player_data)
                        total_players += 1
            
            logger.info(f"Извлечены данные {total_players} игроков для матча {match_id}")
            return players_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге данных игроков матча {match_id}: {str(e)}")
            return []
    
    def _extract_player_data_from_cell(self, cell, match_id, team_id):
        """
        Извлекает данные одного игрока из ячейки таблицы
        
        Args:
            cell: HTML элемент с данными игрока
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
            
            # Получаем элемент с данными игрока
            player_compare = cell.select_one('.player-compare')
            if not player_compare:
                return None
            
            # Получаем ID игрока из атрибута data-player-id
            player_id_attr = player_compare.get('data-player-id')
            if player_id_attr:
                try:
                    player_data['player_id'] = int(player_id_attr)
                except ValueError:
                    pass
            
            # Получаем ник игрока
            player_nickname = player_compare.select_one(PLAYER_NICKNAME)
            if player_nickname:
                nickname_text = player_nickname.text.strip()
                player_data['player_nickname'] = nickname_text
                player_data['nickName'] = nickname_text
                
                # В этой структуре полное имя обычно отсутствует, только ник
                player_data['fullName'] = nickname_text
            
            # Проверяем, что у нас есть хотя бы ник игрока
            if player_data['player_nickname']:
                return player_data
            else:
                return None
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении данных игрока в матче {match_id}: {str(e)}")
            return None
    
    def _save_match_details_to_json(self, match_data):
        """
        Сохраняет детали матча в JSON файл
        """
        try:
            match_id = match_data['match_id']
            json_file_path = os.path.join(UPCOMING_MATCH_JSON_DIR, f"{match_id}.json")
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(match_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Сохранены детали матча {match_id} в файл {json_file_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении деталей матча {match_data['match_id']} в JSON: {str(e)}")
            return False
    
    def _save_players_to_json(self, match_id, players_data):
        """
        Сохраняет игроков матча в JSON файл
        """
        try:
            json_file_path = os.path.join(UPCOMING_PLAYERS_JSON_DIR, f"{match_id}.json")
            data = {
                'match_id': match_id,
                'players': players_data
            }
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Сохранены игроки для матча {match_id} в файл {json_file_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении игроков для матча {match_id} в JSON: {str(e)}")
            return False

    def _parse_streamers_data(self, soup, match_id):
        """
        Извлекает данные о стримерах из HTML
        Args:
            soup (BeautifulSoup): HTML-документ
            match_id (int): ID матча
        Returns:
            list: Список словарей с данными стримеров
        """
        streamers = []
        try:
            streams_block = soup.select_one('.streams')
            if not streams_block:
                logger.info(f"Блок .streams не найден для матча {match_id}")
                return streamers
            # Игнорируем .hltv-live
            for stream_box in streams_block.select('.stream-box'):
                if stream_box.select_one('.hltv-live'):
                    continue
                embed = stream_box.select_one('.stream-box-embed')
                if not embed:
                    continue
                # Язык
                flag_img = embed.select_one('img.flag')
                lang = flag_img['title'] if flag_img and flag_img.has_attr('title') else None
                # Имя
                name = embed.get_text(strip=True)
                # URL
                ext = stream_box.select_one('.external-stream a')
                url = ext['href'] if ext and ext.has_attr('href') else None
                if url:
                    streamers.append({
                        'match_id': match_id,
                        'name': name,
                        'lang': lang,
                        'url': url
                    })
            logger.info(f"Извлечено {len(streamers)} стримеров для матча {match_id}")
            return streamers
        except Exception as e:
            logger.error(f"Ошибка при парсинге стримеров для матча {match_id}: {str(e)}")
            return []

    def _save_streamers_to_json(self, match_id, streamers_data):
        """
        Сохраняет стримеров матча в JSON файл
        """
        try:
            json_file_path = os.path.join(UPCOMING_STREAMERS_JSON_DIR, f"{match_id}.json")
            data = {
                'match_id': match_id,
                'streams': streamers_data
            }
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Сохранены стримеры для матча {match_id} в файл {json_file_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении стримеров для матча {match_id} в JSON: {str(e)}")
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