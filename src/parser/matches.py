"""
Парсер страницы матчей HLTV
"""
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from src.parser.base import BaseParser
from src.config import MATCHES_URL, MATCHES_HTML_FILE
from src.config.selectors import MATCHES, COMMON

class MatchesParser(BaseParser):
    def parse(self):
        """
        Парсинг страницы матчей
        Returns:
            str: путь к сохраненному HTML файлу
        """
        self.logger.info("Starting matches page parsing")
        
        def _parse_page():
            self.driver.get(MATCHES_URL)
            self.logger.info("Page loaded, waiting for sort button...")
            
            try:
                # Проверяем наличие окна cookie и принимаем их
                try:
                    cookie_dialog = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.ID, COMMON['COOKIE_DIALOG']))
                    )
                    self.logger.info("Cookie dialog found, accepting cookies...")
                    
                    accept_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.ID, COMMON['COOKIE_ACCEPT_ALL']))
                    )
                    accept_button.click()
                    self.logger.info("Cookies accepted")
                except TimeoutException:
                    self.logger.info("No cookie dialog found or already accepted")
                
                # Ждем появления элемента сортировки
                sort_button = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, MATCHES['SORT_BY_TIME']))
                )
                self.logger.info("Sort button found")
                
                # Прокручиваем к элементу и кликаем
                self.driver.execute_script("arguments[0].scrollIntoView(true);", sort_button)
                self._human_delay(0.5, 1)  # Минимальная задержка
                
                try:
                    sort_button.click()
                except ElementClickInterceptedException:
                    self.driver.execute_script("arguments[0].click();", sort_button)
                
                self.logger.info("Sort button clicked")
                
                # Даем время на обновление страницы
                self._human_delay(2, 3)
                
                # Получаем HTML
                html = self.driver.page_source
                self.logger.info(f"HTML length: {len(html)}")
                self.logger.info(f"HTML preview: {html[:500].replace(chr(10), ' ').replace(chr(13), ' ')}")
                return html
                
            except Exception as e:
                self.logger.error(f"Error during parsing: {str(e)}")
                return None

        # Получаем страницу с повторными попытками
        content = self._retry_with_delay(_parse_page)
        
        if not content:
            self.logger.warning("No HTML content was retrieved, saving empty file.")
            content = ''
        
        # Сохраняем в файл
        self._ensure_storage_dir(MATCHES_HTML_FILE)
        with open(MATCHES_HTML_FILE, "w", encoding="utf-8") as f:
            f.write(content)
            
        self.logger.info(f"Matches page saved to: {MATCHES_HTML_FILE}")
        return MATCHES_HTML_FILE
