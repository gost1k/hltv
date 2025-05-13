"""
Селекторы CSS для парсинга HTML-страниц HLTV
"""

# Общие селекторы
TIME_EVENT = '.timeAndEvent [data-unix]'
COUNTDOWN = '.countdown'

# Селекторы для команд
TEAM1_GRADIENT = '.team1-gradient'
TEAM2_GRADIENT = '.team2-gradient'
TEAM_NAME = '.teamName'

# Селекторы для счета
TEAM1_SCORE = '.team1-gradient .won, .team1-gradient .lost'
TEAM2_SCORE = '.team2-gradient .won, .team2-gradient .lost'

# Селекторы для рейтингов
TEAM1_RANK = '.lineups .lineup:nth-child(1) .teamRanking a'
TEAM2_RANK = '.lineups .lineup:nth-child(3) .teamRanking a'

# Селекторы для события
EVENT = '.timeAndEvent .event a'

# Селекторы для демо
DEMO_URL = 'a.stream-box[data-demo-link]'

# Селекторы для Head-to-Head
H2H_TEAM1_WINS = '.head-to-head .right-border .bold'
H2H_TEAM2_WINS = '.head-to-head .left-border .bold'

# Селекторы для статистики игроков
STATS_TABLE = '.stats-content table'
TOTAL_STATS_ROW = '.totalstats'
PLAYER_STATS_CELLS = 'td'
PLAYER_LINK = 'a'

# Текстовые метки
MATCH_OVER_TEXT = "Match over"

# Статусы матчей
STATUS_UPCOMING = 'upcoming'
STATUS_COMPLETED = 'completed'
STATUS_UNKNOWN = 'unknown'

# Селекторы HTML элементов для парсеров HLTV
MATCHES = {
    # Кнопка сортировки по времени
    'SORT_BY_TIME': "matches-sort-by-toggle-time",
    
    # Контейнер с матчами
    'MATCHES_CONTAINER': "matches-container",
    
    # Индикаторы загрузки
    'LOADING_INDICATORS': [
        "loading-indicator",
        "spinner",
        "loading-spinner"
    ],
    
    # Индикаторы Cloudflare
    'CLOUDFLARE_INDICATORS': [
        "cf-browser-verification",
        "cf_chl_",
        "cf_clearance",
        "challenge-platform",
        "cf-please-wait",
        "cf-error-details"
    ]
}

# Селекторы страницы результатов
RESULTS = {
    # Добавить при необходимости
}

# Общие селекторы
COMMON = {
    # Окно принятия cookie
    'COOKIE_DIALOG': "CybotCookiebotDialog",
    'COOKIE_ACCEPT_ALL': "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"
} 