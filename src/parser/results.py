"""
Парсер страницы результатов HLTV
"""
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from src.parser.base import BaseParser
from src.config import RESULTS_URL, RESULTS_HTML_FILE, SCREENSHOTS_DIR
from src.config.selectors import COMMON
import os
from datetime import datetime

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
            self.logger.info("Page loaded, waiting for content...")
            
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
                
                # Ждем загрузки основного контента
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "results-all"))
                )
                self.logger.info("Main content loaded")
                
                # Ждем загрузки страницы
                self._wait_for_page_load()
                
                # Получаем HTML
                html = self.driver.page_source
                self.logger.info(f"HTML length: {len(html)}")
                self.logger.info(f"HTML preview: {html[:500].replace(chr(10), ' ').replace(chr(13), ' ')}")
                
                # Если HTML слишком маленький или пустой, считаем сбор неудачным и создаем скриншот
                if html is None or len(html) < 50000:
                    self.logger.warning("HTML content is too small or empty, creating debug screenshot")
                    # Создаем директорию для скриншотов, если её нет
                    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
                    
                    # Создаем имя файла с типом парсинга и датой
                    current_date = datetime.now().strftime("%Y-%m-%d")
                    screenshot_filename = f"results_failure_{current_date}.png"
                    screenshot_path = os.path.join(os.path.abspath(SCREENSHOTS_DIR), screenshot_filename)
                    
                    try:
                        self.driver.save_screenshot(screenshot_path)
                        self.logger.info(f'Debug screenshot saved: {screenshot_path}')
                    except Exception as e:
                        self.logger.error(f'Failed to save debug screenshot: {e}')
                
                return html
                
            except Exception as e:
                self.logger.error(f"Error during parsing: {str(e)}")
                return None

        # Получаем страницу с повторными попытками
        content = self._retry_with_delay(_parse_page)
        
        if not content:
            self.logger.warning("No HTML content was retrieved, saving empty file.")
            content = ''
        
        # Удаляем старый файл, если он есть
        if os.path.exists(RESULTS_HTML_FILE):
            os.remove(RESULTS_HTML_FILE)
            self.logger.info(f"Удалён старый файл: {RESULTS_HTML_FILE}")
        # Теперь сохраняем новый файл
        with open(RESULTS_HTML_FILE, "w", encoding="utf-8") as f:
            f.write(content)
        self.logger.info(f"Results page saved to: {RESULTS_HTML_FILE}")
        return RESULTS_HTML_FILE
