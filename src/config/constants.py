"""
HLTV Parser Constants
"""
# URLs
BASE_URL = "https://www.hltv.org"
RESULTS_URL = f"{BASE_URL}/results"
MATCHES_URL = f"{BASE_URL}/matches"
MATCH_URL = f"{BASE_URL}/matches/"

# File paths
STORAGE_DIR = "storage"
HTML_DIR = f"{STORAGE_DIR}/html"
RESULTS_HTML_FILE = f"{HTML_DIR}/results.html"
MATCHES_HTML_FILE = f"{HTML_DIR}/matches.html"
MATCH_DETAILS_DIR = f"{HTML_DIR}/match_details"
MATCH_UPCOMING_DIR = f"{HTML_DIR}/upcoming"

# Logging
LOG_DIR = "logs"
LOG_FILE = "hltv_parser.log"
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Browser settings
SELENIUM_TIMEOUT = 30
SELENIUM_HEADLESS = True
SELENIUM_WINDOW_SIZE = "1920,1080"
SELENIUM_PAGE_LOAD_TIMEOUT = 60
SELENIUM_IMPLICIT_WAIT = 10
PARSER_DELAY = (1, 3)  # Random delay range in seconds
MAX_RETRIES = 3
MIN_PAGE_SIZE = 1000  # Minimum page size in bytes to consider valid

# Cookies
COOKIES_FILE = f"{STORAGE_DIR}/cookies.json"
COOKIES_EXPIRY_DAYS = 7

# Database
DATABASE_FILE = "hltv.db" 