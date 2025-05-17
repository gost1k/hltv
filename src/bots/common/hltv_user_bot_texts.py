# Все текстовые сообщения и шаблоны для HLTVUserBot

BOT_TEXTS = {
    'start': (
        "Привет, {first_name}! 👋\n\n"
        "Я бот для отображения статистики матчей HLTV.\n\n"
        "Доступные команды:\n"
        "/yesterday - Показать статистику матчей за вчерашний день\n"
        "/today - Показать статистику матчей за сегодня\n"
        "/upcoming - Показать предстоящие матчи на сегодня\n"
        "/menu - Показать меню\n"
        "/help - Показать справку\n\n"
        "Также вы можете ввести точное название команды (например, 'NAVI' или 'Astralis'), чтобы найти её последние и предстоящие матчи."
    ),
    'help': (
        "Справка по командам бота:\n\n"
        "/yesterday - Показать статистику матчей за вчерашний день\n"
        "/today - Показать статистику матчей за сегодня\n"
        "/upcoming - Показать предстоящие матчи на сегодня\n"
        "/menu - Показать меню\n"
        "/help - Показать эту справку\n\n"
        "Для поиска матчей определенной команды, введите её точное название в чат (например, 'NAVI' или 'Astralis')."
    ),
    'menu': "Выберите действие:",
    'choose_period_completed': "Выберите период для просмотра прошедших матчей:",
    'choose_period_upcoming': "Выберите период для просмотра предстоящих матчей:",
    'input_team': "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи команды.",
    'no_matches_period': "Нет данных о матчах за указанный период.",
    'no_matches_upcoming': "Нет данных о предстоящих матчах за указанный период.",
    'no_events_week': "Нет данных о событиях за последнюю неделю.",
    'no_events_14days': "Нет данных о событиях на ближайшие 14 дней.",
    'choose_event_completed': "Выберите событие для просмотра прошедших матчей:",
    'choose_event_upcoming': "Выберите событие для просмотра предстоящих матчей:",
    'event_not_found': "Событие не найдено.",
    'no_matches_event_completed': "Нет данных о завершенных матчах события {event_name}.",
    'no_matches_event_upcoming': "Нет данных о предстоящих матчах события {event_name}.",
    'matches_team_header': "<b>Матчи команды '{team_name}':</b>\n\n",
    'matches_team_upcoming': "<b>📅 Предстоящие матчи:</b>\n",
    'matches_team_completed': "<b>📊 Прошедшие матчи:</b>\n",
    'choose_match': "Выберите матч для просмотра подробной информации:",
    'no_matches_team': "Не найдено матчей для команды '{team_name}'.",
    'match_not_found': "Матч с ID {match_id} не найден.",
    'error_getting_match': "Произошла ошибка при получении данных о матче.",
    'error_getting_event': "Произошла ошибка при получении данных о матчах.",
    'error_getting_events': "Произошла ошибка при получении списка событий.",
    'error_search_team': "Произошла ошибка при поиске матчей команды.",
    'no_lineups': "<i>Нет данных о составах команд для этого матча.</i>",
    'where_to_watch': "\n<b>Где посмотреть:</b>\n",
    'subscribe_success': "Вы подписались на live-матч {match_id}",
    'unsubscribe_success': "Вы отписались от live-матча {match_id}",
    'not_subscribed': "Вы не были подписаны на этот матч.",
    'subscribe_error': "Ошибка: не удалось подписаться на live-матч.",
    'unsubscribe_error': "Ошибка: не удалось отписаться от live-матча.",
    'live_no_matches': "Сейчас нет live-матчей.",
    'live_matches_header': "<b>Live матчи:</b>\n\n",
    'subscribed_matches': "Вы подписаны на матчи:\n{matches}",
    'not_subscribed_any': "Вы не подписаны ни на один матч.",
    'back': "Назад",
    'log': {
        'init': "Инициализация HLTVUserBot ({config_name})",
        'start_command': "{user_info} - Запуск команды /start",
        'help_command': "{user_info} - Запуск команды /help",
        'main_menu': "{user_info} - Вызов основного меню",
        'message': "{user_info} - Сообщение: '{message_text}'",
        'completed_matches_request': "{user_info} - Запрос прошедших матчей",
        'upcoming_matches_request': "{user_info} - Запрос будущих матчей",
        'today_stats_request': "{user_info} - Запрос статистики за сегодня",
        'yesterday_stats_request': "{user_info} - Запрос статистики за вчера через команду",
        'period_matches_request': "{user_info} - Запрос матчей за период с {start} по {end}",
        'found_matches_period': "{user_info} - Найдено {count} матчей за указанный период",
        'found_matches_today': "{user_info} - Найдено {count} матчей за сегодня",
        'events_list_request': "{user_info} - Запрос списка событий",
        'back_to_menu': "{user_info} - Возврат в главное меню",
        'match_details_request': "{user_info} - Запрос статистики матча ID {match_id}",
        'matches_for_event_request': "{user_info} - Запрос матчей события ID {event_id}",
        # ... другие шаблоны логов ...
    },
    'match_details_header': "<b>⏰ {datetime}</b>\n<b>🏆 {event_name}</b>\n\n",
    'match_score': "{team1} {score1} : {score2} {team2}\n\n",
    'maps_stats_header': "<b>Статистика по картам:</b>\n",
    'maps_stats_line': "{map_name}: {team1_rounds}{rounds} {team2_rounds}\n",
    'lineups_header': "<b>👥 Ожидаемые составы:</b>\n\n",
    'lineups_team': "<b>{team_name}:</b>\n{players}\n",
    'team_rating': "Рейтинг команд:\n{team1_rank} - {team1_name}\n{team2_rank} - {team2_name}\n\n",
    'h2h_header': "<b>История встреч:</b>\n",
    'h2h_line': "{team}: {wins} побед\n",
    'demo_link': "<b>📥 <a href='{url}'>Скачать Demo игры</a></b>\n\n",
    'player_stats_header': "<b>📈 Статистика игроков:</b>\n\n",
    'player_stats_team': "<b>{team_name}:</b>\n<pre>\nИгрок        K-D   K/D  ADR KAST Rating\n----------------------------------------\n{players}</pre>\n\n",
    'player_stats_line': "{nick} {kd} {kd_ratio} {adr} {kast} {rating}\n",
    'match_vs': "<b>{team1} vs {team2}</b>\n\n",
    'upcoming_event_matches_header': "📅 <b>Предстоящие матчи события {event_name}</b>\n\n",
    'completed_event_matches_header': "📊 <b>Результаты матчей события {event_name}</b>\n\n",
    'error_streamers': "Ошибка при получении стримеров для матча {match_id}: {error}",
    'error_getting_matches_period': "Ошибка при получении матчей за период: {error}",
    # ... остальные шаблоны ...
} 