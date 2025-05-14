"""
Parser Manager - handles all parsing operations
"""
import logging
from src.parser.results import ResultsParser
from src.parser.matches import MatchesParser
from src.parser.match_details import MatchDetailsParser
from src.db.database import DatabaseService

logger = logging.getLogger(__name__)

class ParserManager:
    """Manages all parsers and their operations"""
    
    def __init__(self):
        self.db = DatabaseService()
        
    def parse_results(self):
        """Parse results page and save HTML"""
        with ResultsParser() as parser:
            results_file = parser.parse()
            return results_file
            
    def parse_matches(self):
        """Parse matches page and save HTML"""
        with MatchesParser() as parser:
            matches_file = parser.parse()
            return matches_file
            
    def parse_match_details(self, limit=None, parse_past=True, parse_upcoming=False):
        """
        Parse match details pages
        
        Args:
            limit (int, optional): Maximum number of matches to parse
            parse_past (bool): Whether to parse past matches
            parse_upcoming (bool): Whether to parse upcoming matches
            
        Returns:
            int: Number of successfully parsed matches
        """
        # Получаем match_ids для обработки
        past_match_ids = []
        upcoming_match_ids = []
        
        if parse_past:
            past_match_ids = self._get_match_ids_for_parsing(is_past=True, limit=limit)
            logger.info(f"Found {len(past_match_ids)} past matches to parse")
            
        if parse_upcoming:
            upcoming_match_ids = self._get_match_ids_for_parsing(is_past=False, limit=limit)
            logger.info(f"Found {len(upcoming_match_ids)} upcoming matches to parse")
            
        match_ids = past_match_ids + upcoming_match_ids
        
        if limit and len(match_ids) > limit:
            match_ids = match_ids[:limit]
            
        logger.info(f"Starting to parse {len(match_ids)} match details")
        
        # Создаем парсер с соответствующими параметрами
        match_details_parser = MatchDetailsParser(
            limit=limit,
            parse_past=parse_past,
            parse_upcoming=parse_upcoming
        )
        
        try:
            # Запускаем парсинг
            with match_details_parser:
                success_count = match_details_parser.parse()
                
            # Обновляем статусы всех обработанных матчей
            for match_id in match_ids:
                self._update_match_parsed_status(match_id, is_past=True)
                
            return success_count
        except Exception as e:
            logger.error(f"Error in parse_match_details: {e}")
            return 0
    
    def _get_match_ids_for_parsing(self, is_past=True, limit=None):
        """
        Получает ID матчей для парсинга, проверяя сначала новую структуру,
        а затем старую структуру таблиц.
        
        Args:
            is_past (bool): True для прошедших матчей, False для предстоящих
            limit (int, optional): Максимальное количество матчей
            
        Returns:
            list: Список ID матчей для парсинга
        """
        try:
            # Сначала пробуем получить из новой структуры
            match_ids = self.db.get_match_ids_for_parsing(is_past=is_past, limit=limit)
            if match_ids:
                return match_ids
                
            # Если пусто, или ошибка, пробуем старую структуру
            conn = self.db.connect()
            if not conn:
                return []
                
            cursor = self.db.cursor
                
            # Определяем таблицу на основе типа матча
            table = "url_result" if is_past else "url_upcoming"
            
            # Запрос для получения ID матчей с флагом toParse = 1
            query = f"""
                SELECT id FROM {table} 
                WHERE toParse = 1
                ORDER BY id DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
                
            cursor.execute(query)
            match_ids = [row[0] for row in cursor.fetchall()]
            
            self.db.close()
            return match_ids
            
        except Exception as e:
            logger.error(f"Error in _get_match_ids_for_parsing: {e}")
            return []
    
    def _update_match_parsed_status(self, match_id, is_past=True):
        """
        Обновляет статус матча как обработанный
        
        Args:
            match_id (int): ID матча
            is_past (bool): True для прошедших матчей, False для предстоящих
        """
        try:
            # Сначала пробуем обновить в новой структуре
            result = self.db.update_match_parsed_status(match_id, parsed=True)
            
            # Также обновляем в старой структуре
            conn = self.db.connect()
            if not conn:
                return
                
            cursor = self.db.cursor
                
            # Определяем таблицу на основе типа матча
            table = "url_result" if is_past else "url_upcoming"
            
            # Обновляем флаг toParse = 0
            query = f"UPDATE {table} SET toParse = 0 WHERE id = ?"
            cursor.execute(query, (match_id,))
            self.db.conn.commit()
            
            self.db.close()
            
        except Exception as e:
            logger.error(f"Error in _update_match_parsed_status: {e}") 