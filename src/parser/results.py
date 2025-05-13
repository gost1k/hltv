"""
Парсер страницы результатов HLTV
"""
from src.parser.base import BaseParser
from src.config import RESULTS_URL, RESULTS_HTML_FILE

class ResultsParser(BaseParser):
    def parse(self):
        """
        Парсинг страницы результатов
        Returns:
            str: путь к сохраненному HTML файлу
        """
        self.logger.info("Starting results page parsing")
        
        def _parse_page():
            self.driver.get(RESULTS_URL)
            self._wait_for_page_load()  # Ждем полной загрузки страницы
            return self.driver.page_source

        # Получаем страницу с повторными попытками
        content = self._retry_with_delay(_parse_page)
        
        # Сохраняем в файл
        self._ensure_storage_dir(RESULTS_HTML_FILE)
        with open(RESULTS_HTML_FILE, "w", encoding="utf-8") as f:
            f.write(content)
            
        self.logger.info(f"Results page saved to: {RESULTS_HTML_FILE}")
        return RESULTS_HTML_FILE
