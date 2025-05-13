"""
Обработчик Cloudflare защиты
"""
import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from src.config import (
    CLOUDFLARE_WAIT_TIME,
    CLOUDFLARE_INDICATORS,
    HUMAN_DELAY_MIN,
    HUMAN_DELAY_MAX,
    MOUSE_MOVEMENT_STEPS,
    SCROLL_STEPS,
    SCROLL_DELAY
)

class CloudflareHandler:
    def __init__(self, driver, logger):
        """
        Инициализация обработчика Cloudflare
        
        Args:
            driver: Selenium WebDriver
            logger: Logger для записи событий
        """
        self.driver = driver
        self.logger = logger

    def _human_delay(self, min_delay: float = HUMAN_DELAY_MIN, max_delay: float = HUMAN_DELAY_MAX):
        """Случайная задержка для эмуляции человеческого поведения"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def _move_mouse_randomly(self):
        """Случайное движение мышью по странице"""
        try:
            action = ActionChains(self.driver)
            for _ in range(MOUSE_MOVEMENT_STEPS):
                x = random.randint(0, 1000)
                y = random.randint(0, 1000)
                action.move_by_offset(x, y)
                action.pause(random.uniform(0.1, 0.3))
            action.perform()
        except Exception as e:
            self.logger.warning(f"Mouse movement failed: {str(e)}")

    def _scroll_page(self):
        """Плавная прокрутка страницы"""
        try:
            # Получаем высоту страницы
            page_height = self.driver.execute_script("return document.body.scrollHeight")
            viewport_height = self.driver.execute_script("return window.innerHeight")
            
            # Вычисляем шаг прокрутки
            scroll_step = page_height / SCROLL_STEPS
            
            # Выполняем плавную прокрутку
            for i in range(SCROLL_STEPS):
                self.driver.execute_script(f"window.scrollTo(0, {scroll_step * i});")
                time.sleep(SCROLL_DELAY)
            
            # Прокручиваем обратно вверх
            self.driver.execute_script("window.scrollTo(0, 0);")
            
        except Exception as e:
            self.logger.warning(f"Page scroll failed: {str(e)}")

    def is_cloudflare_page(self, html: str) -> bool:
        """
        Проверка наличия Cloudflare защиты на странице
        
        Args:
            html: HTML содержимое страницы
            
        Returns:
            bool: True если обнаружена защита Cloudflare
        """
        return any(indicator in html for indicator in CLOUDFLARE_INDICATORS)

    def wait_for_cloudflare(self, max_wait_time: int = CLOUDFLARE_WAIT_TIME) -> bool:
        """
        Ожидание прохождения Cloudflare проверки
        
        Args:
            max_wait_time: Максимальное время ожидания в секундах
            
        Returns:
            bool: True если проверка пройдена успешно
        """
        try:
            # Эмулируем человеческое поведение
            self._human_delay()
            self._move_mouse_randomly()
            self._scroll_page()
            
            # Ждем немного дольше для Cloudflare
            self._human_delay(max_wait_time, max_wait_time * 2)
            
            # Проверяем еще раз после ожидания
            content = self.driver.page_source
            if self.is_cloudflare_page(content):
                self.logger.warning("Cloudflare protection still present after waiting")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error waiting for Cloudflare: {str(e)}")
            return False

    def handle_cloudflare(self, html: str) -> bool:
        """
        Обработка Cloudflare защиты
        
        Args:
            html: HTML содержимое страницы
            
        Returns:
            bool: True если защита успешно обработана
        """
        if not self.is_cloudflare_page(html):
            return True
            
        self.logger.warning("Cloudflare protection detected")
        return self.wait_for_cloudflare() 