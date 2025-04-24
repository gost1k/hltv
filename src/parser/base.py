from abc import ABC, abstractmethod
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class BaseParser(ABC):
    def __init__(self, base_url: str, html_dir: str = "html"):
        self.base_url = base_url
        self.html_dir = html_dir
        self._setup_browser()
        self._ensure_html_dir()
    
    def _setup_browser(self):
        """Setup Chrome browser"""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Add user-agent and other headers
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
        
        self.browser = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        
        # Скрываем факт использования WebDriver
        self.browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    def _ensure_html_dir(self):
        """Ensure HTML directory exists"""
        os.makedirs(self.html_dir, exist_ok=True)
    
    def save_page(self, content: str, prefix: str):
        """Save page content to HTML file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.html"
        filepath = os.path.join(self.html_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        return filepath
    
    def get_page(self, url: str, prefix: str) -> str:
        """Get page content and save it"""
        self.browser.get(url)
        # Добавляем задержку для полной загрузки страницы
        from time import sleep
        sleep(5)  # ждем 5 секунд
        content = self.browser.page_source
        return self.save_page(content, prefix)
    
    def close(self):
        """Close browser"""
        if hasattr(self, "browser"):
            self.browser.quit()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    @abstractmethod
    def parse(self):
        """Abstract method that must be implemented by specific parsers"""
        pass
