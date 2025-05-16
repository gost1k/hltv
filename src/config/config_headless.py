"""
Конфигурационный файл проекта HLTV Parser (headless)
"""

# URLs
HLTV_BASE_URL = "https://www.hltv.org"
MATCHES_URL = f"{HLTV_BASE_URL}/matches"
RESULTS_URL = f"{HLTV_BASE_URL}/results"

# Database
DATABASE_NAME = "hltv.db"

# Selenium settings
SELENIUM_TIMEOUT = 30  # seconds
SELENIUM_HEADLESS = True  # Включаем headless режим
SELENIUM_WINDOW_SIZE = (1920, 1080)  # Размер окна браузера
SELENIUM_PAGE_LOAD_TIMEOUT = 30  # Таймаут загрузки страницы
SELENIUM_IMPLICIT_WAIT = 10  # Неявное ожидание элементов

# Human behavior simulation
HUMAN_DELAY_MIN = 1  # Минимальная задержка между действиями
HUMAN_DELAY_MAX = 3  # Максимальная задержка между действиями
MOUSE_MOVEMENT_STEPS = 10  # Количество шагов для движения мыши
SCROLL_STEPS = 5  # Количество шагов для прокрутки
SCROLL_DELAY = 0.5  # Задержка между прокрутками

# Cookies
COOKIES_FILE = "storage/cookies.json"
COOKIES_EXPIRY_DAYS = 7  # Срок хранения cookies в днях

# Parser settings
PARSER_DELAY = 2  # seconds between requests
MAX_RETRIES = 3
CLOUDFLARE_WAIT_TIME = 15  # Время ожидания после загрузки страницы
MIN_PAGE_SIZE = 50000  # Минимальный размер страницы в байтах

# Cloudflare detection
CLOUDFLARE_INDICATORS = [
    "cf-browser-verification",
    "cf_chl_",
    "cf_clearance",
    "challenge-platform",
    "cf-please-wait",
    "cf-error-details",
    "cf-browser-verification",
    "cf_chl_",
    "cf_clearance",
    "challenge-platform",
    "cf-please-wait",
    "cf-error-details",
    "Just a moment",
    "Checking your browser",
    "Please wait while we verify",
    "DDoS protection by",
    "Cloudflare"
]

# File paths
HTML_STORAGE_DIR = "storage/html"
SCREENSHOTS_DIR = "storage/screenshots"  # Директория для скриншотов
MATCHES_HTML_FILE = f"{HTML_STORAGE_DIR}/matches.html"
RESULTS_HTML_FILE = f"{HTML_STORAGE_DIR}/results.html"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = "hltv_parser.log" 