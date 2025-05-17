#!/usr/bin/env python
"""
Телеграм-бот для отправки статистики матчей HLTV (пользовательская версия)
"""

import os
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import sys
import tempfile
import traceback

from src.bots.config import load_config
from src.scripts.live_matches_parser import handle_new_subscription

# Загружаем конфигурацию
config = load_config('user')

# Настройка логгера
logger = logging.getLogger(__name__)

# Получаем параметры из конфигурации
TOKEN = config['token']
DB_PATH = config['hltv_db_path']

# Кнопки меню
MENU_UPCOMING_MATCHES = "Будущие матчи"
MENU_COMPLETED_MATCHES = "Прошедшие матчи"
MENU_LIVE_MATCHES = "Live матчи"

# Определяем московское время (UTC+3)
MOSCOW_TIMEZONE = timezone(timedelta(hours=3))

class HLTVStatsBot:
    """
    Телеграм-бот для отображения статистики HLTV
    """
    
    def __init__(self, token, db_path):
        """
        Инициализация бота
        
        Args:
            token (str): Токен для Telegram API
            db_path (str): Путь к БД со статистикой HLTV
        """
        self.token = token
        self.db_path = db_path
        
        # Настройка логирования для этого класса
        self.logger = logging.getLogger(__name__)
        # Проверяем, есть ли уже обработчики у логгера
        if not self.logger.handlers:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            
        # Используем локальный логгер вместо глобального
        self.logger.info("Инициализация бота HLTV")
        
        # Создаем клавиатуру с кнопками меню
        self.menu_keyboard = [
            [KeyboardButton(MENU_COMPLETED_MATCHES)],
            [KeyboardButton(MENU_UPCOMING_MATCHES)],
            [KeyboardButton(MENU_LIVE_MATCHES)]
        ]
        self.markup = ReplyKeyboardMarkup(self.menu_keyboard, resize_keyboard=True)
        
    def _get_safe_user_info(self, user):
        """
        Создает безопасную строку с информацией о пользователе для логирования,
        избегая проблем с кодировкой Unicode
        
        Args:
            user: Пользователь Telegram
            
        Returns:
            str: Безопасная строка с информацией о пользователе
        """
        try:
            first_name = user.first_name if user.first_name else ""
            last_name = user.last_name if user.last_name else ""
            username = user.username if user.username else "no_username"
            
            # Заменяем проблемные символы на их безопасные версии или удаляем их
            first_name = ''.join(c if ord(c) < 128 else '_' for c in first_name)
            last_name = ''.join(c if ord(c) < 128 else '_' for c in last_name)
            
            return f"User: {first_name} {last_name} (@{username}) [ID: {user.id}]"
        except:
            # В случае любой ошибки возвращаем только ID пользователя
            return f"User ID: {user.id}"
    
    async def error(self, update, context):
        """
        Обработчик ошибок
        """
        self.logger.error(f"Ошибка: {context.error} при обработке запроса {update}")
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик команды /start
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запуск команды /start")
        
        message = (
            f"Привет, {user.first_name}! 👋\n\n"
            f"Я бот для отображения статистики матчей HLTV.\n\n"
            f"Доступные команды:\n"
            f"/yesterday - Показать статистику матчей за вчерашний день\n"
            f"/today - Показать статистику матчей за сегодня\n"
            f"/upcoming - Показать предстоящие матчи на сегодня\n"
            f"/menu - Показать меню\n"
            f"/help - Показать справку\n\n"
            f"Также вы можете ввести точное название команды (например, 'NAVI' или 'Astralis'), чтобы найти её последние и предстоящие матчи."
        )
        await update.message.reply_text(message, reply_markup=self.markup)
    
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик команды /help
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запуск команды /help")
        
        message = (
            "Справка по командам бота:\n\n"
            "/yesterday - Показать статистику матчей за вчерашний день\n"
            "/today - Показать статистику матчей за сегодня\n"
            "/upcoming - Показать предстоящие матчи на сегодня\n"
            "/menu - Показать меню\n"
            "/help - Показать эту справку\n\n"
            "Для поиска матчей определенной команды, введите её точное название в чат (например, 'NAVI' или 'Astralis')."
        )
        await update.message.reply_text(message, reply_markup=self.markup)
    
    async def show_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает меню с кнопками
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Вызов основного меню")
        
        await update.message.reply_text(
            "Выберите действие:",
            reply_markup=self.markup
        )
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик текстовых сообщений и нажатий на кнопки
        """
        message_text = update.message.text
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Сообщение: '{message_text}'")
        
        # Сохраняем текст нажатой кнопки для последующего определения типа действий
        context.user_data['last_button'] = message_text
        
        # Подписка на live-матч
        if message_text.startswith("/subscribe_live "):
            try:
                match_id = int(message_text.split(" ")[1])
                handle_new_subscription(match_id, user.id)
                await update.message.reply_text(f"Вы подписались на live-матч {match_id}")
            except Exception as e:
                await update.message.reply_text("Ошибка: не удалось подписаться на live-матч.")
            return
        # Отписка от live-матча
        if message_text.startswith("/unsubscribe_live "):
            try:
                match_id = int(message_text.split(" ")[1])
                from src.scripts.live_matches_parser import load_json, save_json, SUBS_JSON
                subs = load_json(SUBS_JSON, default={})
                match_id_str = str(match_id)
                if match_id_str in subs and user.id in subs[match_id_str]:
                    subs[match_id_str].remove(user.id)
                    if not subs[match_id_str]:
                        subs.pop(match_id_str)
                    save_json(SUBS_JSON, subs)
                    await update.message.reply_text(f"Вы отписались от live-матча {match_id}")
                else:
                    await update.message.reply_text("Вы не были подписаны на этот матч.")
            except Exception as e:
                await update.message.reply_text("Ошибка: не удалось отписаться от live-матча.")
            return
        
        if message_text == MENU_COMPLETED_MATCHES:
            self.logger.info(f"{user_info} - Запрос прошедших матчей")
            context.user_data['showing_menu'] = MENU_COMPLETED_MATCHES
            await self.show_completed_matches(update, context)
        elif message_text == MENU_UPCOMING_MATCHES:
            self.logger.info(f"{user_info} - Запрос будущих матчей")
            context.user_data['showing_menu'] = MENU_UPCOMING_MATCHES
            await self.show_upcoming_matches(update, context)
        elif message_text == MENU_LIVE_MATCHES:
            await self.show_live_matches(update, context)
        elif message_text == "За сегодня":
            self.logger.info(f"{user_info} - Запрос матчей за сегодня")
            await self.send_today_stats(update, context)
        elif message_text == "За вчера":
            self.logger.info(f"{user_info} - Запрос матчей за вчера")
            await self.show_matches_for_period(update, context, 1)
        elif message_text == "За 3 дня":
            self.logger.info(f"{user_info} - Запрос матчей за 3 дня")
            await self.show_matches_for_period(update, context, 3)
        elif message_text == "На сегодня":
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на сегодня")
            await self.show_upcoming_matches_for_period(update, context, 0)
        elif message_text == "На завтра":
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на завтра")
            await self.show_upcoming_matches_for_period(update, context, 1)
        elif message_text == "На 3 дня":
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на 3 дня")
            await self.show_upcoming_matches_for_period(update, context, 3)
        elif message_text == "По событию":
            self.logger.info(f"{user_info} - Запрос списка событий")
            await self.show_events_list(update, context)
        elif message_text == "Назад":
            self.logger.info(f"{user_info} - Возврат в главное меню")
            await self.show_menu(update, context)
        elif 'match_mapping' in context.user_data and message_text in context.user_data['match_mapping']:
            # Если текст сообщения совпадает с названием матча в нашем словаре
            match_id = context.user_data['match_mapping'][message_text]
            self.logger.info(f"{user_info} - Запрос статистики матча ID {match_id}")
            await self.show_match_details(update, context, match_id)
        elif 'event_mapping' in context.user_data and message_text in context.user_data['event_mapping']:
            # Если текст сообщения совпадает с названием события в нашем словаре
            event_id = context.user_data['event_mapping'][message_text]
            self.logger.info(f"{user_info} - Запрос матчей события ID {event_id}")
            await self.show_matches_for_event(update, context, event_id)
        elif "(" in message_text and ")" in message_text:
            # Обработка запроса статистики с ID в скобках (оставляем для обратной совместимости)
            try:
                # Извлекаем ID матча из скобок
                match_id_text = message_text.split("(")[-1].split(")")[0].strip()
                match_id = int(''.join(filter(str.isdigit, match_id_text)))
                await self.show_match_details(update, context, match_id)
            except (ValueError, IndexError):
                await update.message.reply_text(
                    "Не удалось определить ID матча. Пожалуйста, используйте меню для выбора матча.",
                    reply_markup=self.markup
                )
        else:
            # Пробуем найти команду по названию
            await self.find_matches_by_team(update, context, message_text)
    
    async def show_completed_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает меню для выбора прошедших матчей
        """
        keyboard = [
            [KeyboardButton("За сегодня")],
            [KeyboardButton("За вчера")],
            [KeyboardButton("За 3 дня")],
            [KeyboardButton("По событию")],
            [KeyboardButton("Назад")]
        ]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            "Выберите период для просмотра прошедших матчей:",
            reply_markup=markup
        )
        await update.message.reply_text(
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи команды."
        )
    
    async def show_upcoming_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает меню для выбора предстоящих матчей
        """
        keyboard = [
            [KeyboardButton("На сегодня")],
            [KeyboardButton("На завтра")],
            [KeyboardButton("На 3 дня")],
            [KeyboardButton("По событию")],
            [KeyboardButton("Назад")]
        ]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            "Выберите период для просмотра предстоящих матчей:",
            reply_markup=markup
        )
        await update.message.reply_text(
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи команды."
        )
    
    async def send_yesterday_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Отправляет статистику матчей за вчерашний день
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запрос статистики за вчера через команду")
        await self.show_matches_for_period(update, context, 1)
    
    async def send_today_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Отправляет статистику матчей за сегодняшний день
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запрос статистики за сегодня")
        
        # Получаем временные метки начала и конца текущего дня по московскому времени
        today = datetime.now(MOSCOW_TIMEZONE)
        start_of_today = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=MOSCOW_TIMEZONE).timestamp()
        end_of_today = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=MOSCOW_TIMEZONE).timestamp()
        
        # Получаем матчи за текущий день
        events = self.get_matches_by_date(start_of_today, end_of_today)
        
        # Форматируем сообщение
        message = f"📊 <b>Результаты матчей за {today.strftime('%d.%m.%Y')}</b>\n\n"
        message += self.format_matches_message(events)
        
        # Логируем количество найденных матчей
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        self.logger.info(f"{user_info} - Найдено {match_count} матчей за сегодня")
        
        # Отправляем сообщение
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
        await update.message.reply_text(
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи команды."
        )
    
    async def show_matches_for_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days=1):
        """
        Показывает прошедшие матчи за указанный период
        
        Args:
            update: Объект обновления Telegram
            context: Контекст обработчика
            days (int): Количество дней для выборки (по умолчанию 1 день)
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        
        today = datetime.now(MOSCOW_TIMEZONE)
        
        # Вычисляем начало и конец периода
        end_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=MOSCOW_TIMEZONE) - timedelta(days=1)
        end_timestamp = end_date.timestamp() + 86399  # Конец дня (23:59:59)
        start_date = end_date - timedelta(days=days-1)
        start_timestamp = start_date.timestamp()
        
        self.logger.info(f"{user_info} - Запрос матчей за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}")
        
        # Получаем матчи за период
        events = self.get_matches_by_date(start_timestamp, end_timestamp)
        
        # Логируем количество найденных матчей
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        self.logger.info(f"{user_info} - Найдено {match_count} матчей за указанный период")
        
        if days == 1:
            period_text = f"за {end_date.strftime('%d.%m.%Y')}"
        else:
            period_text = f"за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}"
        
        # Форматируем сообщение
        message = f"📊 <b>Результаты матчей {period_text}</b>\n\n"
        message += self.format_matches_message(events)
        
        # Отправляем сообщение
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
        await update.message.reply_text(
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи команды."
        )
    
    async def show_events_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает список событий за последнюю неделю
        """
        # Определяем, какой тип событий запрашивается (прошедшие или предстоящие)
        if 'showing_menu' not in context.user_data:
            # Проверяем последние шаги пользователя по нажатым кнопкам меню
            if context.user_data.get('last_button') in ["На сегодня", "На завтра", "На 3 дня", MENU_UPCOMING_MATCHES]:
                # Если последнее действие было связано с предстоящими матчами
                context.user_data['showing_menu'] = MENU_UPCOMING_MATCHES
            else:
                # По умолчанию показываем прошедшие события
                context.user_data['showing_menu'] = MENU_COMPLETED_MATCHES
        
        event_type = context.user_data['showing_menu']
        
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запрос списка событий типа {event_type}")
        
        today = datetime.now(MOSCOW_TIMEZONE)
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if event_type == MENU_COMPLETED_MATCHES:
                # Для прошедших событий - смотрим назад на 7 дней
                start_date = today - timedelta(days=7)
                start_timestamp = start_date.timestamp()
                end_timestamp = today.timestamp()
                
                # Получаем список уникальных событий за указанный период для прошедших матчей
                cursor.execute('''
                    SELECT DISTINCT event_id, event_name 
                    FROM result_match
                    WHERE datetime BETWEEN ? AND ?
                    AND event_id IS NOT NULL
                    AND event_name IS NOT NULL
                    ORDER BY event_name
                ''', (start_timestamp, end_timestamp))
            else:  # MENU_UPCOMING_MATCHES
                # Для предстоящих событий - смотрим вперед на 14 дней
                start_timestamp = today.timestamp()
                end_date = today + timedelta(days=14)
                end_timestamp = end_date.timestamp()
                
                # Получаем список уникальных событий для предстоящих матчей
                cursor.execute('''
                    SELECT DISTINCT event_id, event_name 
                    FROM upcoming_match
                    WHERE datetime BETWEEN ? AND ?
                    AND event_id IS NOT NULL
                    AND event_name IS NOT NULL
                    ORDER BY event_name
                ''', (start_timestamp, end_timestamp))
            
            events = cursor.fetchall()
            conn.close()
            
            if not events:
                period_str = "за последнюю неделю" if event_type == MENU_COMPLETED_MATCHES else "на ближайшие 14 дней"
                await update.message.reply_text(
                    f"Нет данных о событиях {period_str}.",
                    reply_markup=self.markup
                )
                return
            
            # Сохраняем соответствие между названием события и его ID в контексте пользователя
            if 'event_mapping' not in context.user_data:
                context.user_data['event_mapping'] = {}
                
            # Создаем клавиатуру с кнопками событий
            keyboard = []
            for event in events:
                event_name = event['event_name']
                event_id = event['event_id']
                # Сохраняем соответствие
                context.user_data['event_mapping'][event_name] = event_id
                # Добавляем кнопку только с названием события
                keyboard.append([KeyboardButton(event_name)])
            
            # Добавляем кнопку "Назад"
            keyboard.append([KeyboardButton("Назад")])
            markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            event_type_str = "прошедших" if event_type == MENU_COMPLETED_MATCHES else "предстоящих"
            await update.message.reply_text(
                f"Выберите событие для просмотра {event_type_str} матчей:",
                reply_markup=markup
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка событий: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при получении списка событий.",
                reply_markup=self.markup
            )
    
    async def show_matches_for_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id):
        """
        Показывает матчи конкретного события
        
        Args:
            update: Объект обновления Telegram
            context: Контекст обработчика
            event_id (int): ID события
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запрос матчей события ID {event_id}")
        
        # Проверяем тип события (прошедшие или предстоящие)
        event_type = context.user_data.get('showing_menu', MENU_COMPLETED_MATCHES)
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Проверяем, существует ли событие
            if event_type == MENU_COMPLETED_MATCHES:
                # Для прошедших матчей
                cursor.execute('SELECT event_name FROM result_match WHERE event_id = ? LIMIT 1', (event_id,))
                event_result = cursor.fetchone()
                
                if not event_result:
                    await update.message.reply_text(
                        "Событие не найдено.",
                        reply_markup=self.markup
                    )
                    conn.close()
                    return
                
                event_name = event_result['event_name']
                
                # Получаем матчи события
                cursor.execute('''
                    SELECT 
                        match_id, datetime, 
                        team1_id, team1_name, team1_score, 
                        team2_id, team2_name, team2_score
                    FROM result_match
                    WHERE event_id = ?
                    ORDER BY datetime
                ''', (event_id,))
                
                matches = cursor.fetchall()
                conn.close()
                
                if not matches:
                    await update.message.reply_text(
                        f"Нет данных о завершенных матчах события {event_name}.",
                        reply_markup=self.markup
                    )
                    return
                
                # Создаем событие для форматирования
                events = {
                    event_id: {
                        'name': event_name,
                        'matches': [dict(match) for match in matches]
                    }
                }
                
                # Форматируем сообщение
                message = f"📊 <b>Результаты матчей события {event_name}</b>\n\n"
                message += self.format_matches_message(events)
            else:  # MENU_UPCOMING_MATCHES
                # Для предстоящих матчей
                cursor.execute('SELECT event_name FROM upcoming_match WHERE event_id = ? LIMIT 1', (event_id,))
                event_result = cursor.fetchone()
                
                if not event_result:
                    await update.message.reply_text(
                        "Событие не найдено.",
                        reply_markup=self.markup
                    )
                    conn.close()
                    return
                
                event_name = event_result['event_name']
                
                # Получаем предстоящие матчи события
                self.logger.info(f"Запрос предстоящих матчей события ID {event_id}")
                
                cursor.execute('''
                    SELECT 
                        match_id, datetime, 
                        team1_id, team1_name, team1_rank,
                        team2_id, team2_name, team2_rank
                    FROM upcoming_match
                    WHERE event_id = ?
                    ORDER BY datetime
                ''', (event_id,))
                
                matches = cursor.fetchall()
                
                self.logger.info(f"Найдено {len(matches)} предстоящих матчей для события ID {event_id}")
                
                conn.close()
                
                if not matches:
                    await update.message.reply_text(
                        f"Нет данных о предстоящих матчах события {event_name}.",
                        reply_markup=self.markup
                    )
                    return
                
                # Создаем событие для форматирования
                events = {
                    event_id: {
                        'name': event_name,
                        'matches': [dict(match) for match in matches]
                    }
                }
                
                # Форматируем сообщение
                message = f"📅 <b>Предстоящие матчи события {event_name}</b>\n\n"
                message += self.format_upcoming_matches_message(events)
            
            # Добавляем информацию о стримах для предстоящих матчей
            ics_button_markup = None
            ics_file_path = None
            if event_type == 'upcoming':
                try:
                    conn2 = sqlite3.connect(self.db_path)
                    conn2.row_factory = sqlite3.Row
                    cursor2 = conn2.cursor()
                    cursor2.execute('''
                        SELECT name, lang, url FROM upcoming_match_streamers WHERE match_id = ?
                    ''', (event_id,))
                    streams = cursor2.fetchall()
                    conn2.close()
                    if streams:
                        message += '\n<b>Где посмотреть:</b>\n'
                        for s in streams:
                            lang = f" ({s['lang']})" if s['lang'] else ''
                            message += f"• <a href=\"{s['url']}\">{s['name']}{lang}</a>\n"
                except Exception as e:
                    self.logger.error(f"Ошибка при получении стримеров для матча {event_id}: {str(e)}")
            # Отправляем сообщение
            await update.message.reply_text(message, parse_mode="HTML", reply_markup=ics_button_markup if ics_button_markup else self.markup)
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении матчей события {event_id}: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при получении данных о матчах.",
                reply_markup=self.markup
            )
    
    async def show_match_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE, match_id):
        """
        Показывает подробную информацию о матче и статистику игроков
        
        Args:
            update: Объект обновления Telegram
            context: Контекст обработчика
            match_id (int): ID матча
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запрос детальной информации о матче ID {match_id}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Сначала проверяем, есть ли матч в таблице прошедших матчей
            cursor.execute('''
                SELECT 
                    m.match_id, m.datetime, 
                    m.team1_id, m.team1_name, m.team1_score, m.team1_rank,
                    m.team2_id, m.team2_name, m.team2_score, m.team2_rank,
                    m.event_id, m.event_name, m.demo_id, 'completed' as match_type
                FROM result_match m
                WHERE m.match_id = ?
            ''', (match_id,))
            
            match = cursor.fetchone()
            
            # Если не найден, ищем в таблице предстоящих матчей
            if not match:
                cursor.execute('''
                    SELECT 
                        m.match_id, m.datetime, 
                        m.team1_id, m.team1_name, 0 as team1_score, m.team1_rank,
                        m.team2_id, m.team2_name, 0 as team2_score, m.team2_rank,
                        m.event_id, m.event_name, NULL as demo_id, 'upcoming' as match_type
                    FROM upcoming_match m
                    WHERE m.match_id = ?
                ''', (match_id,))
                
                match = cursor.fetchone()
            
            if not match:
                await update.message.reply_text(
                    f"Матч с ID {match_id} не найден.",
                    reply_markup=self.markup
                )
                conn.close()
                return
                
            # Получаем статистику игроков для этого матча, если это прошедший матч
            player_stats = []
            match_type = match['match_type']
            
            if match_type == 'completed':
                # Для прошедших матчей получаем статистику игроков
                cursor.execute('''
                    SELECT 
                        p.nickname, p.team_id, p.kills, p.deaths, 
                        p.kd_ratio, p.adr, p.kast, p.rating
                    FROM player_stats p
                    WHERE p.match_id = ?
                    ORDER BY p.team_id, p.rating DESC
                ''', (match_id,))
                
                player_stats = cursor.fetchall()
            elif match_type == 'upcoming':
                # Для предстоящих матчей получаем составы команд
                cursor.execute('''
                    SELECT 
                        p.player_nickname as nickname, p.team_id
                    FROM upcoming_match_players p
                    WHERE p.match_id = ?
                    ORDER BY p.team_id
                ''', (match_id,))
                
                player_stats = cursor.fetchall()
            
            conn.close()
            
            # Форматируем информацию о матче
            match_datetime = datetime.fromtimestamp(match['datetime'], tz=MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M')
            
            # Используем полные названия команд
            team1_name = match['team1_name']
            team2_name = match['team2_name']
            
            message = f"<b>⏰ {match_datetime}</b>\n"
            message += f"<b>🏆 {match['event_name']}</b>\n\n"
            
            if match_type == 'completed':
                team1_score = match['team1_score']
                team2_score = match['team2_score']
                
                # Выделяем победителя
                if team1_score > team2_score:
                    team1_name = f"🏆 <b>{team1_name}</b>"
                    team2_name = f"{team2_name}"
                elif team2_score > team1_score:
                    team1_name = f"{team1_name}"
                    team2_name = f"<b>{team2_name}</b>"
                    
                message += f"{team1_name} {team1_score} : {team2_score} {team2_name}\n\n"

                # --- Новый блок: Статистика по картам ---
                try:
                    conn2 = sqlite3.connect(self.db_path)
                    conn2.row_factory = sqlite3.Row
                    cursor2 = conn2.cursor()
                    cursor2.execute('''
                        SELECT map_name, team1_rounds, team2_rounds, rounds
                        FROM result_match_maps
                        WHERE match_id = ?
                        ORDER BY id
                    ''', (match_id,))
                    maps = cursor2.fetchall()
                    conn2.close()
                    if maps:
                        message += '<b>Статистика по картам:</b>\n'
                        for m in maps:
                            map_line = f"{m['map_name']}: {m['team1_rounds']}"
                            if m['rounds']:
                                map_line += f" {m['rounds']}"
                            map_line += f" {m['team2_rounds']}"
                            message += map_line + '\n'
                        message += '\n'
                except Exception as e:
                    self.logger.error(f"Ошибка при получении сыгранных карт для матча {match_id}: {str(e)}")
                # --- Конец блока ---
            else:  # upcoming
                message += f"<b>{team1_name} vs {team2_name}</b>\n\n"
            
            # Если есть информация о рейтинге команд
            if match['team1_rank'] or match['team2_rank']:
                team1_rank = f"#{match['team1_rank']}" if match['team1_rank'] else "нет данных"
                team2_rank = f"#{match['team2_rank']}" if match['team2_rank'] else "нет данных"
                message += f"Рейтинг команд:\n{team1_rank} - {match['team1_name']}\n{team2_rank} - {match['team2_name']}\n\n"
            
            # Получаем head-to-head статистику, если она есть
            if match_type == 'upcoming' and hasattr(match, 'head_to_head_team1_wins') and hasattr(match, 'head_to_head_team2_wins'):
                if match['head_to_head_team1_wins'] is not None and match['head_to_head_team2_wins'] is not None:
                    message += f"<b>История встреч:</b>\n"
                    message += f"{match['team1_name']}: {match['head_to_head_team1_wins']} побед\n"
                    message += f"{match['team2_name']}: {match['head_to_head_team2_wins']} побед\n\n"
            
            # Добавляем ссылку на демо, если она доступна
            if match_type == 'completed' and match['demo_id']:
                demo_url = f"https://www.hltv.org/download/demo/{match['demo_id']}"
                message += f"<b>📥 <a href='{demo_url}'>Скачать Demo игры</a></b>\n\n"
            
            # Группируем статистику по командам
            team1_players = [p for p in player_stats if p['team_id'] == match['team1_id']]
            team2_players = [p for p in player_stats if p['team_id'] == match['team2_id']]
            
            if match_type == 'completed' and (team1_players or team2_players):
                message += "<b>📈 Статистика игроков:</b>\n\n"
                
                if team1_players:
                    message += f"<b>{match['team1_name']}:</b>\n"
                    message += "<pre>\n"
                    message += "Игрок        K-D   K/D  ADR KAST Rating\n"
                    message += "----------------------------------------\n"
                    
                    for player in team1_players:
                        nick = player['nickname']
                        if len(nick) > 12:
                            nick = nick[:9] + "..."
                            
                        kd = f"{player['kills'] or 0}-{player['deaths'] or 0}"
                        kd_ratio = f"{player['kd_ratio']:.2f}" if player['kd_ratio'] else "0.00"
                        adr = f"{player['adr']:.1f}" if player['adr'] else "0.0"
                        kast = f"{player['kast']*100:.0f}%" if player['kast'] else "0%"
                        rating = f"{player['rating']:.2f}" if player['rating'] else "0.00"
                        
                        message += f"{nick.ljust(12)} {kd.ljust(5)} {kd_ratio.ljust(4)} {adr.ljust(3)} {kast.ljust(4)} {rating}\n"
                    
                    message += "</pre>\n\n"
                
                if team2_players:
                    message += f"<b>{match['team2_name']}:</b>\n"
                    message += "<pre>\n"
                    message += "Игрок        K-D   K/D  ADR KAST Rating\n"
                    message += "----------------------------------------\n"
                    
                    for player in team2_players:
                        nick = player['nickname']
                        if len(nick) > 12:
                            nick = nick[:9] + "..."
                            
                        kd = f"{player['kills'] or 0}-{player['deaths'] or 0}"
                        kd_ratio = f"{player['kd_ratio']:.2f}" if player['kd_ratio'] else "0.00"
                        adr = f"{player['adr']:.1f}" if player['adr'] else "0.0"
                        kast = f"{player['kast']*100:.0f}%" if player['kast'] else "0%"
                        rating = f"{player['rating']:.2f}" if player['rating'] else "0.00"
                        
                        message += f"{nick.ljust(12)} {kd.ljust(5)} {kd_ratio.ljust(4)} {adr.ljust(3)} {kast.ljust(4)} {rating}\n"
                    
                    message += "</pre>\n"
            elif match_type == 'upcoming' and (team1_players or team2_players):
                message += "<b>👥 Ожидаемые составы:</b>\n\n"
                
                if team1_players:
                    message += f"<b>{match['team1_name']}:</b>\n"
                    for player in team1_players:
                        message += f"• {player['nickname']}\n"
                    message += "\n"
                
                if team2_players:
                    message += f"<b>{match['team2_name']}:</b>\n"
                    for player in team2_players:
                        message += f"• {player['nickname']}\n"
            else:
                message += "<i>Нет данных о составах команд для этого матча.</i>"
            
            # Отправляем сообщение
            await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении данных о матче {match_id}: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при получении данных о матче.",
                reply_markup=self.markup
            )
    
    async def find_matches_by_team(self, update: Update, context: ContextTypes.DEFAULT_TYPE, team_name):
        """
        Поиск и отображение последних 10 прошедших и предстоящих матчей команды
        
        Args:
            update: Объект обновления Telegram
            context: Контекст обработчика
            team_name (str): Название команды для поиска
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Поиск матчей команды: {team_name}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Поиск прошедших матчей команды
            cursor.execute('''
                SELECT 
                    match_id, datetime, 
                    team1_id, team1_name, team1_score, 
                    team2_id, team2_name, team2_score,
                    event_name, 'completed' as match_type
                FROM result_match
                WHERE (LOWER(team1_name) = LOWER(?) OR LOWER(team2_name) = LOWER(?))
                ORDER BY datetime DESC
                LIMIT 10
            ''', (team_name, team_name))
            
            completed_matches = cursor.fetchall()
            
            # Поиск предстоящих матчей команды
            cursor.execute('''
                SELECT 
                    match_id, datetime, 
                    team1_id, team1_name, 0 as team1_score, 
                    team2_id, team2_name, 0 as team2_score,
                    event_name, 'upcoming' as status, 'upcoming' as match_type
                FROM upcoming_match
                WHERE (LOWER(team1_name) = LOWER(?) OR LOWER(team2_name) = LOWER(?))
                ORDER BY datetime ASC
                LIMIT 10
            ''', (team_name, team_name))
            
            upcoming_matches = cursor.fetchall()
            conn.close()
            
            # Объединяем результаты: сначала предстоящие, потом прошедшие
            all_matches = list(upcoming_matches) + list(completed_matches)
            
            if not all_matches:
                await update.message.reply_text(
                    f"Не найдено матчей для команды '{team_name}'.",
                    reply_markup=self.markup
                )
                return
            
            # Создаем кнопки для каждого матча
            keyboard = []
            
            # Сохраняем соответствие между названием матча и его ID в контексте пользователя
            if 'match_mapping' not in context.user_data:
                context.user_data['match_mapping'] = {}
            
            # Формируем текстовый список матчей с датами
            matches_list = f"<b>Матчи команды '{team_name}':</b>\n\n"
            
            # Сначала добавляем предстоящие матчи
            if upcoming_matches:
                matches_list += "<b>📅 Предстоящие матчи:</b>\n"
                for i, match in enumerate(upcoming_matches, 1):
                    team1_name = match['team1_name']
                    team2_name = match['team2_name']
                    match_id = match['match_id']
                    match_date = datetime.fromtimestamp(match['datetime'], tz=MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M')
                    
                    # Добавляем матч в текстовый список
                    matches_list += f"{i}. <b>{match_date}</b>: {team1_name} vs {team2_name}\n"
                    
                    # Создаем текст кнопки без даты
                    match_text = f"{team1_name} vs {team2_name}"
                    
                    # Сохраняем соответствие
                    context.user_data['match_mapping'][match_text] = match_id
                    
                    # Добавляем кнопку
                    keyboard.append([KeyboardButton(match_text)])
                
                matches_list += "\n"
            
            # Затем добавляем прошедшие матчи
            if completed_matches:
                matches_list += "<b>📊 Прошедшие матчи:</b>\n"
                for i, match in enumerate(completed_matches, 1):
                    team1_name = match['team1_name']
                    team2_name = match['team2_name']
                    team1_score = match['team1_score']
                    team2_score = match['team2_score']
                    match_id = match['match_id']
                    match_date = datetime.fromtimestamp(match['datetime'], tz=MOSCOW_TIMEZONE).strftime('%d.%m.%Y')
                    
                    # Добавляем матч в текстовый список
                    matches_list += f"{i}. <b>{match_date}</b>: {team1_name} {team1_score}:{team2_score} {team2_name}\n"
                    
                    # Создаем текст кнопки без даты
                    match_text = f"{team1_name} {team1_score}:{team2_score} {team2_name}"
                    
                    # Сохраняем соответствие
                    context.user_data['match_mapping'][match_text] = match_id
                    
                    # Добавляем кнопку
                    keyboard.append([KeyboardButton(match_text)])
            
            # Добавляем кнопку "Назад"
            keyboard.append([KeyboardButton("Назад")])
            
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            # Отправляем список матчей и клавиатуру для выбора
            await update.message.reply_text(
                matches_list,
                parse_mode="HTML"
            )
            
            await update.message.reply_text(
                "Выберите матч для просмотра подробной информации:",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка при поиске матчей команды: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при поиске матчей команды.",
                reply_markup=self.markup
            )
    
    def get_matches_by_date(self, date_start, date_end):
        """
        Получает завершенные матчи из базы данных за указанный период
        
        Args:
            date_start (int): Начало периода (UNIX timestamp)
            date_end (int): Конец периода (UNIX timestamp)
            
        Returns:
            dict: Словарь с событиями и матчами
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Возвращать результаты в виде словарей
            cursor = conn.cursor()
            
            # Получаем матчи за указанный период
            cursor.execute('''
                SELECT 
                    m.match_id, m.datetime, 
                    m.team1_id, m.team1_name, m.team1_score, m.team1_rank,
                    m.team2_id, m.team2_name, m.team2_score, m.team2_rank,
                    m.event_id, m.event_name
                FROM result_match m
                WHERE m.datetime BETWEEN ? AND ?
                ORDER BY m.event_id, m.datetime
            ''', (date_start, date_end))
            
            matches = cursor.fetchall()
            
            # Группируем матчи по событиям
            events = {}
            for match in matches:
                event_id = match['event_id']
                event_name = match['event_name']
                
                if event_id not in events:
                    events[event_id] = {
                        'name': event_name,
                        'matches': []
                    }
                
                events[event_id]['matches'].append({
                    'match_id': match['match_id'],
                    'datetime': match['datetime'],
                    'team1_id': match['team1_id'],
                    'team1_name': match['team1_name'],
                    'team1_score': match['team1_score'],
                    'team2_id': match['team2_id'],
                    'team2_name': match['team2_name'],
                    'team2_score': match['team2_score']
                })
            
            conn.close()
            return events
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении матчей за период: {str(e)}")
            return {}
            
    def format_matches_message(self, events):
        """
        Форматирует сообщение с матчами
        
        Args:
            events (dict): Словарь с событиями и матчами
            
        Returns:
            str: Отформатированное сообщение
        """
        if not events:
            return "Нет данных о матчах за указанный период."
        
        message = ""
        
        for event_id, event_data in events.items():
            event_name = event_data['name'] or "Без названия"
            matches = event_data['matches']
            
            # Добавляем название события
            message += f"🏆 <b>{event_name}</b>\n\n"
            
            for match in matches:
                # Получаем короткие имена команд (никнеймы)
                team1_name = match['team1_name']
                team2_name = match['team2_name']
                team1_score = match['team1_score']
                team2_score = match['team2_score']
                
                # Выделяем победителя
                if team1_score > team2_score:
                    team1_name = f"<b>{team1_name}</b>"
                elif team2_score > team1_score:
                    team2_name = f"<b>{team2_name}</b>"
                
                # Формируем строку результата без ID
                message += f"• <code>{team1_name}</code> {team1_score} : {team2_score} <code>{team2_name}</code>\n"
            
            # Добавляем разделитель между событиями
            message += "\n"
        
        return message
    
    async def show_upcoming_matches_for_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days=0):
        """
        Показывает предстоящие матчи за указанный период
        
        Args:
            update: Объект обновления Telegram
            context: Контекст обработчика
            days (int): Через сколько дней (0 - сегодня, 1 - завтра, и т.д.)
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        
        today = datetime.now(MOSCOW_TIMEZONE)
        current_timestamp = today.timestamp()
        
        # Вычисляем начало и конец периода
        if days == 0:  # Сегодня - берем с текущего момента до конца дня
            start_timestamp = current_timestamp
            end_date = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=MOSCOW_TIMEZONE)
            end_timestamp = end_date.timestamp()
            period_text = "на сегодня"
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на сегодня ({start_timestamp} - {end_timestamp})")
        elif days == 1:  # Завтра
            tomorrow = today + timedelta(days=1)
            start_date = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0, tzinfo=MOSCOW_TIMEZONE)
            end_date = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 23, 59, 59, tzinfo=MOSCOW_TIMEZONE)
            start_timestamp = start_date.timestamp()
            end_timestamp = end_date.timestamp()
            period_text = f"на завтра ({start_date.strftime('%d.%m.%Y')})"
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на завтра ({start_timestamp} - {end_timestamp})")
        else:  # Несколько дней вперед
            start_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=MOSCOW_TIMEZONE)
            end_date = start_date + timedelta(days=days)
            start_timestamp = current_timestamp  # С текущего момента
            end_timestamp = end_date.timestamp()
            period_text = f"на ближайшие {days} дней"
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на {days} дней ({start_timestamp} - {end_timestamp})")
            
        # Получаем матчи за период
        events = self.get_upcoming_matches_by_date(start_timestamp, end_timestamp)
        
        # Логируем количество найденных матчей
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        self.logger.info(f"{user_info} - Найдено {match_count} предстоящих матчей за указанный период")
        
        # Форматируем сообщение
        message = f"📅 <b>Предстоящие матчи {period_text}</b>\n\n"
        message += self.format_upcoming_matches_message(events)
        
        # Отправляем сообщение
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
        await update.message.reply_text(
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи команды."
        )
    
    async def send_upcoming_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Отправляет информацию о предстоящих матчах на сегодня
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запрос предстоящих матчей через команду")
        await self.show_upcoming_matches_for_period(update, context, 0)
    
    def get_upcoming_matches_by_date(self, date_start, date_end):
        """
        Получает предстоящие матчи из базы данных за указанный период
        
        Args:
            date_start (int): Начало периода (UNIX timestamp)
            date_end (int): Конец периода (UNIX timestamp)
            
        Returns:
            dict: Словарь с предстоящими матчами, сгруппированными по событиям
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Возвращать результаты в виде словарей
            cursor = conn.cursor()
            
            # Логируем запрос для отладки
            self.logger.info(f"Запрос предстоящих матчей за период от {date_start} до {date_end}")
            
            # Получаем матчи за указанный период
            cursor.execute('''
                SELECT 
                    m.match_id, m.datetime, 
                    m.team1_id, m.team1_name, m.team1_rank,
                    m.team2_id, m.team2_name, m.team2_rank,
                    m.event_id, m.event_name
                FROM upcoming_match m
                WHERE m.datetime BETWEEN ? AND ?
                AND m.status = 'upcoming'
                ORDER BY m.event_id, m.datetime
            ''', (date_start, date_end))
            
            matches = cursor.fetchall()
            
            # Логируем количество найденных матчей
            self.logger.info(f"Найдено {len(matches)} предстоящих матчей в БД")
            
            # Группируем матчи по событиям
            events = {}
            for match in matches:
                event_id = match['event_id']
                event_name = match['event_name']
                
                if event_id not in events:
                    events[event_id] = {
                        'name': event_name,
                        'matches': []
                    }
                
                events[event_id]['matches'].append({
                    'match_id': match['match_id'],
                    'datetime': match['datetime'],
                    'team1_id': match['team1_id'],
                    'team1_name': match['team1_name'],
                    'team1_rank': match['team1_rank'],
                    'team2_id': match['team2_id'],
                    'team2_name': match['team2_name'],
                    'team2_rank': match['team2_rank']
                })
            
            conn.close()
            return events
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении предстоящих матчей: {str(e)}")
            return {}
    
    def format_upcoming_matches_message(self, events):
        """
        Форматирует сообщение с предстоящими матчами
        
        Args:
            events (dict): Словарь с событиями и матчами
            
        Returns:
            str: Отформатированное сообщение
        """
        if not events:
            return "Нет данных о предстоящих матчах за указанный период."
        
        message = ""
        
        for event_id, event_data in events.items():
            event_name = event_data['name'] or "Без названия"
            matches = event_data['matches']
            
            # Добавляем название события
            message += f"🏆 <b>{event_name}</b>\n\n"
            
            for match in matches:
                # Используем полные названия команд
                team1_name = match['team1_name']
                team2_name = match['team2_name']
                
                # Форматируем дату и время с учетом московского часового пояса
                match_datetime = datetime.fromtimestamp(match['datetime'], tz=MOSCOW_TIMEZONE)
                match_date = match_datetime.strftime('%d.%m')
                match_time = match_datetime.strftime('%H:%M')
                
                # Форматируем строку с матчем
                message += f"• <b>{match_date} {match_time}</b> <code>{team1_name}</code> vs <code>{team2_name}</code>\n"
            
            # Добавляем разделитель между событиями
            message += "\n"
        
        return message
    
    async def show_live_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает список live-матчей с возможностью подписки
        """
        from src.scripts.live_matches_parser import load_json, LIVE_JSON, handle_new_subscription
        matches = load_json(LIVE_JSON, default=[])
        if not matches:
            await update.message.reply_text("Сейчас нет live-матчей.", reply_markup=self.markup)
            return
        message = "<b>Live матчи:</b>\n\n"
        keyboard = []
        for match in matches:
            t1 = match['team_names'][0] if match['team_names'] else '?'
            t2 = match['team_names'][1] if len(match['team_names']) > 1 else '?'
            score1 = match['current_map_scores'][0] if match['current_map_scores'] else '?'
            score2 = match['current_map_scores'][1] if len(match['current_map_scores']) > 1 else '?'
            maps1 = match['maps_won'][0] if match['maps_won'] else '0'
            maps2 = match['maps_won'][1] if len(match['maps_won']) > 1 else '0'
            match_id = match['match_id']
            match_url = match.get('match_url')
            if match_url:
                link = f' <a href="{match_url}">🌐</a>'
            else:
                link = ''
            btn_text = f"Подписаться на {t1} vs {t2}"
            message += f"<b>{t1}</b> ({maps1}) {score1} - {score2} ({maps2}) <b>{t2}</b>{link}\n"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"subscribe_live:{match_id}")])
        keyboard.append([InlineKeyboardButton("Назад", callback_data="back_to_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=reply_markup, disable_web_page_preview=True)
    
    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = query.data
        user = query.from_user
        if data.startswith("subscribe_live:"):
            match_id = int(data.split(":")[1])
            from src.scripts.live_matches_parser import handle_new_subscription
            handle_new_subscription(match_id, user.id)
            await query.answer("Вы подписались на live-матч!")
            await query.edit_message_reply_markup(reply_markup=None)
        elif data == "back_to_menu":
            await query.answer()
            await self.show_menu(update, context)
    
    def run(self):
        """
        Запускает бота
        """
        self.logger.info("Запуск бота...")
        
        # Создаем экземпляр приложения Telegram
        application = Application.builder().token(self.token).build()
        
        # Регистрация команд бота
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help))
        application.add_handler(CommandHandler("yesterday", self.send_yesterday_stats))
        application.add_handler(CommandHandler("today", self.send_today_stats))
        application.add_handler(CommandHandler("upcoming", self.send_upcoming_matches))
        application.add_handler(CommandHandler("menu", self.show_menu))
        
        # Обработчик текстовых сообщений
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        # Обработчик callback-запросов
        application.add_handler(CallbackQueryHandler(self.handle_callback_query))
        
        # Обработчик ошибок
        application.add_error_handler(self.error)
        
        # Запуск бота
        application.run_polling(stop_signals=None)


def main():
    """
    Основная функция для запуска бота
    """
    # Создаем экземпляр бота и запускаем его
    bot = HLTVStatsBot(TOKEN, DB_PATH)
    bot.run()

if __name__ == "__main__":
    main() 