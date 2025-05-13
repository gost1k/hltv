"""
Селекторы HTML элементов для парсеров HLTV
"""

# Селекторы страницы матчей
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