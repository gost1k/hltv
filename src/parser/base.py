"""
Базовый класс для парсеров HLTV
"""
import os
import json
import time
import random
import logging
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from src.config import (
    SELENIUM_TIMEOUT,
    SELENIUM_HEADLESS,
    SELENIUM_WINDOW_SIZE,
    SELENIUM_PAGE_LOAD_TIMEOUT,
    SELENIUM_IMPLICIT_WAIT,
    PARSER_DELAY,
    MAX_RETRIES,
    MIN_PAGE_SIZE,
    COOKIES_FILE,
    COOKIES_EXPIRY_DAYS,
    LOG_LEVEL,
    LOG_FORMAT,
    LOG_FILE
)
from src.parser.cloudflare import CloudflareHandler

class BaseParser(ABC):
    def __init__(self):
        self._setup_logging()
        self.driver = None
        self._setup_driver()
        self._ensure_storage_dir(COOKIES_FILE)
        self.cloudflare = CloudflareHandler(self.driver, self.logger)

    def _setup_logging(self):
        """Настройка логирования"""
        logging.basicConfig(
            level=getattr(logging, LOG_LEVEL),
            format=LOG_FORMAT,
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _setup_driver(self):
        """Настройка Selenium WebDriver"""
        try:
            options = Options()
            if SELENIUM_HEADLESS:
                options.add_argument('--headless')
            
            # Добавляем настройки для обхода Cloudflare
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-infobars')
            # options.add_argument('--start-maximized')  # Удалено для избежания конфликта с set_window_size
            options.add_argument('--disable-notifications')
            options.add_argument('--disable-popup-blocking')
            
            # Добавляем user-agent
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36')
            
            # Добавляем дополнительные настройки для предотвращения обнаружения
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-web-security')
            options.add_argument('--ignore-certificate-errors')
            
            # Добавляем экспериментальные опции
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Устанавливаем путь к бинарному файлу, если он указан
            options.binary_location = 'C:/Program Files/Google/Chrome/Application/chrome.exe'
            
            # Устанавливаем размер окна
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            
            # Устанавливаем размер окна
            self.driver.set_window_size(*SELENIUM_WINDOW_SIZE)
            
            # Устанавливаем таймауты
            self.driver.set_page_load_timeout(SELENIUM_PAGE_LOAD_TIMEOUT)
            self.driver.implicitly_wait(SELENIUM_IMPLICIT_WAIT)
            
            # Выполняем CDP команды для предотвращения обнаружения
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
            })
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en']
                    });
                    window.chrome = {
                        runtime: {}
                    };
                '''
            })
            
            self.logger.info("Chrome driver initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {str(e)}")
            raise

    def _save_cookies(self):
        """Сохранение cookies в файл"""
        try:
            cookies = self.driver.get_cookies()
            if not cookies:
                return
            
            # Создаем директорию, если она не существует
            os.makedirs(os.path.dirname(COOKIES_FILE), exist_ok=True)
            
            # Сохраняем cookies с истечением срока
            cookie_data = {
                'cookies': cookies,
                'expiry': (datetime.now() + timedelta(days=COOKIES_EXPIRY_DAYS)).isoformat()
            }
            
            with open(COOKIES_FILE, 'w') as f:
                json.dump(cookie_data, f)
                
            self.logger.info("Cookies saved successfully")
            
        except Exception as e:
            self.logger.warning(f"Failed to save cookies: {str(e)}")

    def _load_cookies(self) -> bool:
        """Загрузка cookies из файла, если они существуют и не истекли"""
        try:
            if not os.path.exists(COOKIES_FILE):
                return False
            
            with open(COOKIES_FILE, 'r') as f:
                cookie_data = json.load(f)
            
            # Проверяем, не истекли ли cookies
            expiry = datetime.fromisoformat(cookie_data['expiry'])
            if datetime.now() > expiry:
                os.remove(COOKIES_FILE)
                return False
            
            # Добавляем cookies к драйверу
            for cookie in cookie_data['cookies']:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    self.logger.warning(f"Failed to add cookie: {str(e)}")
                    
            self.logger.info("Cookies loaded successfully")
            return True
            
        except Exception as e:
            self.logger.warning(f"Failed to load cookies: {str(e)}")
            return False

    def _ensure_storage_dir(self, filepath):
        """Создание директории для сохранения файлов если она не существует"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

    def _is_valid_page(self, content):
        """Проверка валидности страницы"""
        # Проверяем размер страницы
        if len(content) < MIN_PAGE_SIZE:
            self.logger.warning(f"Page size too small: {len(content)} bytes")
            return False
            
        # Временно отключаем проверку Cloudflare
        # if self.cloudflare.is_cloudflare_page(content):
        #     self.logger.warning("Cloudflare protection detected")
        #     return False
            
        return True

    def _wait_for_page_load(self):
        """Ожидание полной загрузки страницы"""
        try:
            WebDriverWait(self.driver, SELENIUM_PAGE_LOAD_TIMEOUT).until(
                lambda driver: driver.execute_script('return document.readyState') == 'complete'
            )
            
            # Временно отключаем проверку Cloudflare
            # content = self.driver.page_source
            # if not self.cloudflare.handle_cloudflare(content):
            #     raise TimeoutException("Cloudflare protection still present after waiting")
            
            # Проверяем валидность страницы
            content = self.driver.page_source
            if not self._is_valid_page(content):
                raise TimeoutException("Invalid page content")
                
        except TimeoutException as e:
            self.logger.error(f"Page load timeout: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during page load: {str(e)}")
            raise

    def _retry_with_delay(self, func, *args, **kwargs):
        """Повторные попытки выполнения функции с задержкой"""
        last_error = None
        
        for attempt in range(MAX_RETRIES):
            try:
                if attempt == 0:
                    self._load_cookies()
                
                result = func(*args, **kwargs)
                
                # Проверяем результат
                if isinstance(result, str) and not self._is_valid_page(result):
                    raise Exception("Invalid page content")
                
                self._save_cookies()
                return result
                
            except Exception as e:
                last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < MAX_RETRIES - 1:
                    time.sleep(PARSER_DELAY)
                    
                    # Очищаем cookies и кэш при повторной попытке
                    self.driver.delete_all_cookies()
                    self.driver.execute_script("window.localStorage.clear();")
                    self.driver.execute_script("window.sessionStorage.clear();")
                    
                    # Перезагружаем страницу
                    self.driver.refresh()
                    
        # Если все попытки не удались, закрываем браузер
        self.close()
        raise last_error

    def close(self):
        """Закрытие браузера и очистка ресурсов"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                self.logger.error(f"Error closing driver: {str(e)}")
            finally:
                self.driver = None

    def __enter__(self):
        """Поддержка контекстного менеджера"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Гарантированное закрытие браузера при выходе из контекста"""
        self.close()

    @abstractmethod
    def parse(self):
        """Абстрактный метод для парсинга, должен быть реализован в дочерних классах"""
        pass
