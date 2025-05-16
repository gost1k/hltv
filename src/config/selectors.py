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
NOTEAM = '.noteam'  # Селектор для неопределенной команды

# Селекторы для счета
TEAM1_SCORE = '.team1-gradient .won, .team1-gradient .lost'
TEAM2_SCORE = '.team2-gradient .won, .team2-gradient .lost'

# Селекторы для рейтингов
TEAM1_RANK = '.lineups .lineup:nth-child(1) .teamRanking a'
TEAM2_RANK = '.lineups .lineup:nth-child(3) .teamRanking a'

# Селекторы для линеек игроков
LINEUPS_CONTAINER = '.lineups'
TEAM1_LINEUP = '.lineups .lineup:nth-child(1)'
TEAM2_LINEUP = '.lineups .lineup:nth-child(3)'
PLAYERS_TABLE = '.players > table'
PLAYER_ROW = 'tr'
PLAYER_CELL = 'td.player'
PLAYER_DATA_ID = '[data-player-id]'
PLAYER_COMPARE = '.player-compare'
PLAYER_TEAM_ORDINAL = '[data-team-ordinal]'
PLAYER_NICKNAME = 'div.text-ellipsis'

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

# Уточненные селекторы для игроков
PLAYER_CELL = 'td.player-cell'  # Ячейка с именем игрока
PLAYER_PROFILE_LINK = '.flagAlign a'  # Ссылка на профиль игрока внутри ячейки

# Селекторы для новой структуры таблицы статистики
STATS_PLAYER_ROW = '.stats-content tr:not(:first-child)'  # Строки с данными игроков (кроме заголовка)
STATS_TEAM_HEADER = '.stats-content tr:first-child td'    # Строка с названием команды
STATS_TABLE_COLUMNS = {
    'player': 0,    # Индекс колонки с игроком
    'kd': 1,        # Индекс колонки с K-D (убийства-смерти)
    'plus_minus': 2, # Индекс колонки с +/-
    'adr': 3,       # Индекс колонки с ADR
    'kast': 4,      # Индекс колонки с KAST
    'rating': 5     # Индекс колонки с рейтингом
}

# Текстовые метки
MATCH_OVER_TEXT = "Match over"

# Статусы матчей
STATUS_UPCOMING = 'upcoming'
STATUS_COMPLETED = 'completed'
STATUS_UNKNOWN = 'unknown'
STATUS_LIVE = 'live'  # Статус для матчей, которые в данный момент идут

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

# Селекторы для стримов
STREAMS_BLOCK = '.streams'
STREAM_BOX = '.stream-box'
STREAM_BOX_EMBED = '.stream-box-embed'
STREAM_FLAG_IMG = 'img.flag'
STREAM_EXTERNAL_LINK = '.external-stream a'
STREAM_HLTV_LIVE = '.hltv-live' 