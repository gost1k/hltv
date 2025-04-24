from .base import BaseParser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep

class MatchesParser(BaseParser):
    def __init__(self):
        super().__init__(base_url="https://www.hltv.org/matches")
    
    def parse(self):
        """Parse upcoming matches page"""
        # Загружаем страницу
        self.browser.get(self.base_url)
        
        try:
            # Ждем загрузки страницы
            sleep(5)
            
            # Пробуем найти кнопку сортировки разными способами
            selectors = [
                (By.CLASS_NAME, "matches-sort-by-toggle-time"),
                (By.CSS_SELECTOR, "[class*='matches-sort-by-toggle']"),
                (By.CSS_SELECTOR, "[class*='sort-by-toggle']"),
                (By.XPATH, "//div[contains(@class, 'sort-by-toggle')]")
            ]
            
            sort_button = None
            for by, selector in selectors:
                try:
                    sort_button = WebDriverWait(self.browser, 3).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    print(f"Найдена кнопка сортировки по селектору: {selector}")
                    break
                except:
                    continue
            
            if sort_button:
                try:
                    # Прокручиваем к кнопке
                    self.browser.execute_script("arguments[0].scrollIntoView(true);", sort_button)
                    sleep(1)
                    
                    # Кликаем по кнопке через JavaScript
                    self.browser.execute_script("arguments[0].click();", sort_button)
                    print("Выполнен клик по кнопке сортировки")
                    
                    # Ждем 5 секунд после клика
                    sleep(5)
                except Exception as e:
                    print(f"Ошибка при клике на кнопку сортировки: {str(e)}")
            else:
                print("Не удалось найти кнопку сортировки")
            
            # Сохраняем страницу
            content = self.browser.page_source
            return self.save_page(content, "matches")
            
        except Exception as e:
            print(f"Ошибка при парсинге страницы matches: {str(e)}")
            return None
