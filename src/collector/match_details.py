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
            
            # Получаем таблицу статистики
            stats_table = soup.select_one(STATS_TABLE)
            if not stats_table:
                logger.warning(f"Таблица статистики не найдена для матча {match_id}")
                return players_data
            
            # Определяем строки для каждой команды
            team1_rows = []
            team2_rows = []
            
            # Флаг для определения, читаем ли мы строки первой или второй команды
            current_team = 1
            
            # Проходим по строкам таблицы
            for row in stats_table.select('tr'):
                # Если нашли разделитель команд (строку с .totalstats), меняем текущую команду
                if row.select_one(TOTAL_STATS_ROW):
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
                player_data = self._extract_player_stats(row, match_id, team1_id)
                if player_data:
                    players_data.append(player_data)
            
            # Обрабатываем статистику игроков второй команды
            for row in team2_rows:
                player_data = self._extract_player_stats(row, match_id, team2_id)
                if player_data:
                    players_data.append(player_data)
            
            return players_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге статистики игроков матча {match_id}: {str(e)}")
            return []
    
    def _extract_player_stats(self, row, match_id, team_id):
        """
        Извлекает статистику одного игрока из строки таблицы
        
        Args:
            row (Tag): Строка таблицы
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
            if len(cells) < 8:
                # Проверяем, есть ли хотя бы информация об игроке
                if len(cells) > 0 and cells[0].select_one(PLAYER_LINK):
                    # Если есть имя игрока, сохраняем хотя бы его
                    player_link = cells[0].select_one(PLAYER_LINK)
                    player_data['player_id'] = self._extract_id_from_url(player_link.get('href'))
                    # Очищаем имя игрока от переносов строк
                    player_data['player_nickname'] = re.sub(r'\s+', ' ', player_link.text.strip().replace('\n', ' ')).strip()
                    
                    logger.warning(f"Недостаточно данных для полной статистики игрока {player_data['player_nickname']} (матч {match_id})")
                    
                    # Заполняем доступные данные
                    for i, cell in enumerate(cells[1:], 1):
                        try:
                            if i == 1 and cell.text.strip():  # kills
                                player_data['kills'] = int(cell.text.strip())
                            elif i == 2 and cell.text.strip():  # deaths
                                player_data['deaths'] = int(cell.text.strip())
                        except (ValueError, TypeError):
                            pass
                    
                    return player_data
                else:
                    logger.warning(f"Недостаточно ячеек в строке статистики игрока для матча {match_id} ({len(cells)} вместо 8)")
                    return None
            
            # Имя и ID игрока
            player_link = cells[0].select_one(PLAYER_LINK)
            if player_link:
                player_data['player_id'] = self._extract_id_from_url(player_link.get('href'))
                # Очищаем имя игрока от переносов строк
                player_data['player_nickname'] = re.sub(r'\s+', ' ', player_link.text.strip().replace('\n', ' ')).strip()
            else:
                # Если нет ссылки на игрока, пытаемся получить хотя бы текст
                player_name = cells[0].text.strip()
                if player_name:
                    # Очищаем имя игрока от переносов строк
                    player_data['player_nickname'] = re.sub(r'\s+', ' ', player_name.replace('\n', ' ')).strip()
                    logger.warning(f"Не найдена ссылка на профиль игрока '{player_data['player_nickname']}' (матч {match_id})")
                else:
                    logger.warning(f"Не найдено имя игрока в матче {match_id}")
                    return None
            
            # Статистика - с обработкой ошибок для каждого поля
            try:
                if cells[1].text.strip():
                    player_data['kills'] = int(cells[1].text.strip())
            except (ValueError, IndexError, TypeError):
                logger.debug(f"Не удалось извлечь количество убийств для игрока {player_data['player_nickname']}")
                
            try:
                if cells[2].text.strip():
                    player_data['deaths'] = int(cells[2].text.strip())
            except (ValueError, IndexError, TypeError):
                logger.debug(f"Не удалось извлечь количество смертей для игрока {player_data['player_nickname']}")
                
            try:
                if cells[3].text.strip():
                    player_data['kd_ratio'] = float(cells[3].text.strip())
            except (ValueError, IndexError, TypeError):
                logger.debug(f"Не удалось извлечь K/D для игрока {player_data['player_nickname']}")
                
            try:
                if cells[4].text.strip():
                    player_data['plus_minus'] = int(cells[4].text.strip())
            except (ValueError, IndexError, TypeError):
                logger.debug(f"Не удалось извлечь +/- для игрока {player_data['player_nickname']}")
                
            try:
                if cells[5].text.strip():
                    player_data['adr'] = float(cells[5].text.strip())
            except (ValueError, IndexError, TypeError):
                logger.debug(f"Не удалось извлечь ADR для игрока {player_data['player_nickname']}")
                
            try:
                if len(cells) > 6 and cells[6].text.strip():
                    # KAST (может быть в процентах)
                    kast_text = cells[6].text.strip().replace('%', '')
                    player_data['kast'] = float(kast_text) / 100 if '%' in cells[6].text else float(kast_text)
            except (ValueError, IndexError, TypeError):
                logger.debug(f"Не удалось извлечь KAST для игрока {player_data['player_nickname']}")
                
            try:
                if len(cells) > 7 and cells[7].text.strip():
                    player_data['rating'] = float(cells[7].text.strip())
            except (ValueError, IndexError, TypeError):
                logger.debug(f"Не удалось извлечь рейтинг для игрока {player_data['player_nickname']}")
            
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
                    kills, deaths, kd_ratio, plus_minus,
                    adr, kast, rating
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    player_data['match_id'],
                    player_data['team_id'],
                    player_data['player_id'],
                    player_data['player_nickname'],
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
    
    def collect(self, limit=None):
        """
        Основной метод сбора данных из HTML-файлов матчей
        
        Args:
            limit (int): Максимальное количество файлов для обработки
            
        Returns:
            dict: Статистика сбора данных
        """
        # Статистика
        stats = {
            'total_files': 0,
            'processed_files': 0,
            'successful_match_details': 0,
            'successful_player_stats': 0,
            'errors': 0
        }
        
        # Проверяем существование директории
        if not os.path.exists(self.html_dir):
            logger.error(f"Директория {self.html_dir} не существует")
            return stats
            
        # Получаем список файлов матчей
        match_files = glob.glob(os.path.join(self.html_dir, 'match_*.html'))
        stats['total_files'] = len(match_files)
        
        # Ограничиваем количество файлов, если указан лимит
        if limit and limit > 0:
            match_files = match_files[:limit]
        
        logger.info(f"Найдено {stats['total_files']} файлов матчей в директории {self.html_dir}, обработка {len(match_files)}")
        
        # Обрабатываем каждый файл
        for file_path in match_files:
            try:
                # Извлекаем ID матча из имени файла
                match_id = self._extract_match_id_from_filename(os.path.basename(file_path))
                if not match_id:
                    logger.warning(f"Не удалось извлечь ID матча из имени файла: {file_path}")
                    continue
                
                logger.info(f"Обработка матча ID {match_id} из файла {os.path.basename(file_path)}")
                
                # Читаем HTML-файл
                with open(file_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                
                # Парсим HTML
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Извлекаем данные матча
                match_data = self._parse_match_details(soup, match_id)
                if match_data:
                    # Сохраняем данные матча
                    if self._save_match_details(match_data):
                        stats['successful_match_details'] += 1
                
                # Извлекаем статистику игроков
                players_data = self._parse_player_stats(soup, match_id)
                if players_data:
                    # Сохраняем статистику игроков
                    if self._save_player_stats(players_data):
                        stats['successful_player_stats'] += 1
                
                stats['processed_files'] += 1
                
            except Exception as e:
                logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}")
                stats['errors'] += 1
        
        logger.info(f"Обработка завершена. Статистика: {stats}")
        return stats

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