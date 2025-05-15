#!/usr/bin/env python
"""
Общие константы для ботов HLTV
"""

from datetime import timezone, timedelta

# Определяем московское время (UTC+3)
MOSCOW_TIMEZONE = timezone(timedelta(hours=3))

# Кнопки меню
MENU_UPCOMING_MATCHES = "Будущие матчи"
MENU_COMPLETED_MATCHES = "Прошедшие матчи"

# Форматы для даты и времени
DATE_FORMAT = "%d.%m.%Y"
TIME_FORMAT = "%H:%M"
DATETIME_FORMAT = "%d.%m.%Y %H:%M" 