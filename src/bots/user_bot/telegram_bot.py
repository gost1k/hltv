#!/usr/bin/env python
"""
Телеграм-бот для отправки статистики матчей HLTV (пользовательская версия)
"""

import os
import logging
import sqlite3
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from src.bots.config import load_config

# Загружаем конфигурацию
config = load_config('user')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(config['log_file'])
    ]
)
logger = logging.getLogger(__name__)

# Получаем параметры из конфигурации
TOKEN = config['token']
DB_PATH = config['hltv_db_path']
SUBSCRIBERS_DB_PATH = config['subscribers_db_path']

# Кнопки меню
MENU_UPCOMING_MATCHES = "Будущие матчи"
MENU_COMPLETED_MATCHES = "Прошедшие матчи"

class HLTVStatsBot:
    """
    Бот для отправки статистики матчей HLTV
    """
    def __init__(self, token, db_path, subscribers_db_path=None):
        """
        Инициализация бота
        
        Args:
            token (str): Токен телеграм-бота
            db_path (str): Путь к файлу базы данных HLTV
            subscribers_db_path (str): Путь к файлу базы данных подписчиков
        """
        self.token = token
        self.db_path = db_path
        self.subscribers_db_path = subscribers_db_path or SUBSCRIBERS_DB_PATH
        
        # Инициализация базы данных подписчиков
        self._init_subscribers_db()
        
        # Создаем клавиатуру с кнопками меню
        self.menu_keyboard = [
            [KeyboardButton(MENU_COMPLETED_MATCHES)],
            [KeyboardButton(MENU_UPCOMING_MATCHES)],
            [KeyboardButton("Матчи за день")]
        ]
        self.markup = ReplyKeyboardMarkup(self.menu_keyboard, resize_keyboard=True)
        
    async def error(self, update, context):
        """
        Обработчик ошибок
        """
        logger.error(f"Ошибка: {context.error} при обработке запроса {update}")
    
    def _init_subscribers_db(self):
        """
        Инициализирует базу данных подписчиков
        """
        try:
            conn = sqlite3.connect(self.subscribers_db_path)
            cursor = conn.cursor()
            
            # Создаем таблицу подписчиков, если она не существует
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    username TEXT,
                    subscribed_date INTEGER,
                    is_active INTEGER DEFAULT 1
                )
            ''')
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации базы данных подписчиков: {str(e)}")
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик команды /start
        """
        user = update.effective_user
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Запуск команды /start")
        
        message = (
            f"Привет, {user.first_name}! 👋\n\n"
            f"Я бот для отображения статистики матчей HLTV.\n\n"
            f"Доступные команды:\n"
            f"/yesterday - Показать статистику матчей за вчерашний день\n"
            f"/today - Показать статистику матчей за сегодня\n"
            f"/subscribe - Подписаться на ежедневную рассылку\n"
            f"/unsubscribe - Отписаться от ежедневной рассылки\n"
            f"/menu - Показать меню\n"
            f"/help - Показать справку"
        )
        await update.message.reply_text(message, reply_markup=self.markup)
    
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик команды /help
        """
        user = update.effective_user
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Запуск команды /help")
        
        message = (
            "Справка по командам бота:\n\n"
            "/yesterday - Показать статистику матчей за вчерашний день\n"
            "/today - Показать статистику матчей за сегодня\n"
            "/subscribe - Подписаться на ежедневную рассылку\n"
            "/unsubscribe - Отписаться от ежедневной рассылки\n"
            "/menu - Показать меню\n"
            "/help - Показать эту справку"
        )
        await update.message.reply_text(message, reply_markup=self.markup)
    
    async def show_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает меню с кнопками
        """
        user = update.effective_user
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Вызов основного меню")
        
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
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Сообщение: '{message_text}'")
        
        if message_text == MENU_COMPLETED_MATCHES:
            logger.info(f"{user_info} - Запрос прошедших матчей")
            await self.show_completed_matches(update, context)
        elif message_text == MENU_UPCOMING_MATCHES:
            logger.info(f"{user_info} - Запрос предстоящих матчей")
            await self.show_upcoming_matches(update, context)
        elif message_text == "За сегодня":
            logger.info(f"{user_info} - Запрос матчей за сегодня")
            await self.send_today_stats(update, context)
        elif message_text == "За вчера":
            logger.info(f"{user_info} - Запрос матчей за вчера")
            await self.show_matches_for_period(update, context, 1)
        elif message_text == "За 3 дня":
            logger.info(f"{user_info} - Запрос матчей за 3 дня")
            await self.show_matches_for_period(update, context, 3)
        elif message_text == "По событию":
            logger.info(f"{user_info} - Запрос списка событий")
            await self.show_events_list(update, context)
        elif message_text == "Назад":
            logger.info(f"{user_info} - Возврат в главное меню")
            await self.show_menu(update, context)
        elif message_text == "Матчи за день":
            logger.info(f"{user_info} - Запрос матчей за последний день")
            await self.show_last_day_matches_menu(update, context)
        elif 'match_mapping' in context.user_data and message_text in context.user_data['match_mapping']:
            # Если текст сообщения совпадает с названием матча в нашем словаре
            match_id = context.user_data['match_mapping'][message_text]
            logger.info(f"{user_info} - Запрос статистики матча ID {match_id}")
            await self.show_match_details(update, context, match_id)
        elif 'event_mapping' in context.user_data and message_text in context.user_data['event_mapping']:
            # Если текст сообщения совпадает с названием события в нашем словаре
            event_id = context.user_data['event_mapping'][message_text]
            logger.info(f"{user_info} - Запрос матчей события ID {event_id}")
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
            await update.message.reply_text(
                "Используйте кнопки меню или команды для взаимодействия с ботом.\n"
                "Для показа меню наберите /menu\n"
                "Для получения списка команд наберите /help",
                reply_markup=self.markup
            )
    
    async def show_completed_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает прошедшие матчи
        """
        # Показываем меню выбора периода матчей
        keyboard = [
            [KeyboardButton("За сегодня")],
            [KeyboardButton("За вчера")],
            [KeyboardButton("За 3 дня")],
            [KeyboardButton("По событию")],
            [KeyboardButton("Назад")]
        ]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(
            "Выберите период для просмотра результатов матчей:",
            reply_markup=markup
        )
    
    async def show_upcoming_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает предстоящие матчи (пока не реализовано)
        """
        await update.message.reply_text(
            "Функция отображения предстоящих матчей пока не реализована. Следите за обновлениями!",
            reply_markup=self.markup
        )
    
    async def subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Подписывает пользователя на ежедневные отчеты
        """
        chat_id = update.effective_chat.id
        user = update.effective_user
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Попытка подписки на ежедневную рассылку")
        
        try:
            conn = sqlite3.connect(self.subscribers_db_path)
            cursor = conn.cursor()
            
            # Проверяем, есть ли уже такой подписчик
            cursor.execute('SELECT is_active FROM subscribers WHERE chat_id = ?', (chat_id,))
            result = cursor.fetchone()
            
            if result:
                if result[0] == 1:
                    await update.message.reply_text("Вы уже подписаны на ежедневную рассылку! 👍")
                    conn.close()
                    return
                else:
                    # Обновляем запись, если пользователь был отписан ранее
                    cursor.execute(
                        'UPDATE subscribers SET is_active = 1, subscribed_date = ? WHERE chat_id = ?',
                        (int(datetime.now().timestamp()), chat_id)
                    )
            else:
                # Добавляем нового подписчика
                cursor.execute(
                    'INSERT INTO subscribers (chat_id, first_name, last_name, username, subscribed_date, is_active) VALUES (?, ?, ?, ?, ?, 1)',
                    (
                        chat_id,
                        user.first_name,
                        user.last_name,
                        user.username,
                        int(datetime.now().timestamp())
                    )
                )
            
            conn.commit()
            conn.close()
            
            await update.message.reply_text(
                "Вы успешно подписались на ежедневную рассылку статистики матчей! 🎮\n"
                "Каждое утро вы будете получать результаты матчей за прошедший день."
            )
            
        except Exception as e:
            logger.error(f"Ошибка при подписке пользователя {chat_id}: {str(e)}")
            await update.message.reply_text("Произошла ошибка при подписке. Пожалуйста, попробуйте позже.")
    
    async def unsubscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Отписывает пользователя от ежедневных отчетов
        """
        chat_id = update.effective_chat.id
        user = update.effective_user
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Попытка отписки от ежедневной рассылки")
        
        try:
            conn = sqlite3.connect(self.subscribers_db_path)
            cursor = conn.cursor()
            
            # Проверяем, есть ли такой подписчик
            cursor.execute('SELECT is_active FROM subscribers WHERE chat_id = ?', (chat_id,))
            result = cursor.fetchone()
            
            if result and result[0] == 1:
                # Отписываем пользователя
                cursor.execute('UPDATE subscribers SET is_active = 0 WHERE chat_id = ?', (chat_id,))
                conn.commit()
                await update.message.reply_text("Вы успешно отписались от ежедневной рассылки. 👋")
            else:
                await update.message.reply_text("Вы не были подписаны на ежедневную рассылку.")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Ошибка при отписке пользователя {chat_id}: {str(e)}")
            await update.message.reply_text("Произошла ошибка при отписке. Пожалуйста, попробуйте позже.")
    
    def get_matches_by_date(self, date_start, date_end):
        """
        Получает матчи из базы данных за указанный период
        
        Args:
            date_start (int): Начало периода (UNIX timestamp)
            date_end (int): Конец периода (UNIX timestamp)
            
        Returns:
            list: Список матчей, сгруппированных по событиям
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
                FROM match_details m
                WHERE m.status = 'completed'
                AND m.datetime BETWEEN ? AND ?
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
            logger.error(f"Ошибка при получении матчей: {str(e)}")
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
                team1_name = match['team1_name'].split()[0]  # Берем первое слово как никнейм
                team2_name = match['team2_name'].split()[0]  # Берем первое слово как никнейм
                team1_score = match['team1_score']
                team2_score = match['team2_score']
                match_id = match['match_id']
                
                # Выделяем победителя
                if team1_score > team2_score:
                    team1_name = f"<b>{team1_name}</b>"
                elif team2_score > team1_score:
                    team2_name = f"<b>{team2_name}</b>"
                
                # Формируем строку результата
                message += f"• <code>{team1_name}</code> {team1_score} : {team2_score} <code>{team2_name}</code>\n"
            
            # Добавляем разделитель между событиями
            message += "\n"
        
        message += "Используйте меню 'Матчи за день' для просмотра подробной статистики по матчам.\n"
        
        return message
    
    async def send_yesterday_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Отправляет статистику матчей за вчерашний день
        """
        user = update.effective_user
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Запрос статистики за вчера через команду")
        await self.show_matches_for_period(update, context, 1)
    
    async def send_today_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Отправляет статистику матчей за сегодняшний день
        """
        user = update.effective_user
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Запрос статистики за сегодня")
        
        # Получаем временные метки начала и конца текущего дня
        today = datetime.now()
        start_of_today = datetime(today.year, today.month, today.day, 0, 0, 0).timestamp()
        end_of_today = datetime(today.year, today.month, today.day, 23, 59, 59).timestamp()
        
        # Получаем матчи за текущий день
        events = self.get_matches_by_date(start_of_today, end_of_today)
        
        # Форматируем сообщение
        message = f"📊 <b>Результаты матчей за {today.strftime('%d.%m.%Y')}</b>\n\n"
        message += self.format_matches_message(events)
        
        # Логируем количество найденных матчей
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        logger.info(f"{user_info} - Найдено {match_count} матчей за сегодня")
        
        # Отправляем сообщение
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
    
    async def show_matches_for_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days=1):
        """
        Показывает прошедшие матчи за указанный период
        
        Args:
            update: Объект обновления Telegram
            context: Контекст обработчика
            days (int): Количество дней для выборки (по умолчанию 1 день)
        """
        user = update.effective_user
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        
        today = datetime.now()
        
        # Вычисляем начало и конец периода
        end_date = datetime(today.year, today.month, today.day, 0, 0, 0) - timedelta(days=1)
        end_timestamp = end_date.timestamp() + 86399  # Конец дня (23:59:59)
        start_date = end_date - timedelta(days=days-1)
        start_timestamp = start_date.timestamp()
        
        logger.info(f"{user_info} - Запрос матчей за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}")
        
        # Получаем матчи за период
        events = self.get_matches_by_date(start_timestamp, end_timestamp)
        
        # Логируем количество найденных матчей
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        logger.info(f"{user_info} - Найдено {match_count} матчей за указанный период")
        
        if days == 1:
            period_text = f"за {end_date.strftime('%d.%m.%Y')}"
        else:
            period_text = f"за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}"
        
        # Форматируем сообщение
        message = f"📊 <b>Результаты матчей {period_text}</b>\n\n"
        message += self.format_matches_message(events)
        
        # Отправляем сообщение
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
    
    async def show_events_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает список событий за последнюю неделю
        """
        today = datetime.now()
        # Начало периода - 7 дней назад
        start_date = today - timedelta(days=7)
        start_timestamp = start_date.timestamp()
        # Конец периода - текущий момент
        end_timestamp = today.timestamp()
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Получаем список уникальных событий за указанный период
            cursor.execute('''
                SELECT DISTINCT event_id, event_name 
                FROM match_details
                WHERE datetime BETWEEN ? AND ?
                AND event_id IS NOT NULL
                AND event_name IS NOT NULL
                ORDER BY event_name
            ''', (start_timestamp, end_timestamp))
            
            events = cursor.fetchall()
            conn.close()
            
            if not events:
                await update.message.reply_text(
                    "Нет данных о событиях за последнюю неделю.",
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
            
            await update.message.reply_text(
                "Выберите событие для просмотра результатов матчей:",
                reply_markup=markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка событий: {str(e)}")
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
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Получаем название события
            cursor.execute('SELECT event_name FROM match_details WHERE event_id = ? LIMIT 1', (event_id,))
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
                FROM match_details
                WHERE event_id = ?
                AND status = 'completed'
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
            
            # Отправляем сообщение
            await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
            
        except Exception as e:
            logger.error(f"Ошибка при получении матчей события {event_id}: {str(e)}")
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
        user_info = f"User: {user.first_name} {user.last_name or ''} (@{user.username or 'no_username'}) [ID: {user.id}]"
        logger.info(f"{user_info} - Запрос детальной информации о матче ID {match_id}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Получаем информацию о матче
            cursor.execute('''
                SELECT 
                    m.match_id, m.datetime, 
                    m.team1_id, m.team1_name, m.team1_score, m.team1_rank,
                    m.team2_id, m.team2_name, m.team2_score, m.team2_rank,
                    m.event_id, m.event_name
                FROM match_details m
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
                
            # Получаем статистику игроков для этого матча
            cursor.execute('''
                SELECT 
                    p.nickname, p.team_id, p.kills, p.deaths, 
                    p.kd_ratio, p.adr, p.kast, p.rating
                FROM player_stats p
                WHERE p.match_id = ?
                ORDER BY p.team_id, p.rating DESC
            ''', (match_id,))
            
            player_stats = cursor.fetchall()
            conn.close()
            
            # Форматируем информацию о матче
            match_time = datetime.fromtimestamp(match['datetime']).strftime('%d.%m.%Y %H:%M')
            
            # Получаем короткие имена команд (никнеймы)
            team1_name = match['team1_name'].split()[0]  # Берем первое слово как никнейм
            team2_name = match['team2_name'].split()[0]  # Берем первое слово как никнейм
            team1_score = match['team1_score']
            team2_score = match['team2_score']
            
            message = f"<b>⏰ {match_time}</b>\n"
            message += f"<b>🏆 {match['event_name']}</b>\n\n"
            
            # Выделяем победителя
            if team1_score > team2_score:
                team1_name = f"🏆 <b>{team1_name}</b>"
                team2_name = f"{team2_name} ❌"
            elif team2_score > team1_score:
                team1_name = f"❌ {team1_name}"
                team2_name = f"<b>{team2_name}</b> 🏆"
                
            message += f"<b>{team1_name} {team1_score} : {team2_score} {team2_name}</b>\n\n"
            
            # Если есть информация о рейтинге команд
            if match['team1_rank'] or match['team2_rank']:
                team1_rank = f"#{match['team1_rank']}" if match['team1_rank'] else "нет данных"
                team2_rank = f"#{match['team2_rank']}" if match['team2_rank'] else "нет данных"
                message += f"Рейтинг команд:\n{team1_rank} - {match['team1_name']}\n{team2_rank} - {match['team2_name']}\n\n"
            
            # Группируем статистику по командам
            team1_players = [p for p in player_stats if p['team_id'] == match['team1_id']]
            team2_players = [p for p in player_stats if p['team_id'] == match['team2_id']]
            
            if team1_players or team2_players:
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
            else:
                message += "<i>Нет данных о статистике игроков для этого матча.</i>"
            
            # Отправляем сообщение
            await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных о матче {match_id}: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при получении данных о матче.",
                reply_markup=self.markup
            )
    
    async def show_last_day_matches_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показывает меню матчей за последний день для выбора конкретного матча
        """
        # Получаем дату вчерашнего дня
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        start_timestamp = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0).timestamp()
        end_timestamp = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59).timestamp()
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Получаем матчи за вчерашний день
            cursor.execute('''
                SELECT 
                    match_id, datetime, 
                    team1_id, team1_name, team1_score, 
                    team2_id, team2_name, team2_score,
                    event_name
                FROM match_details
                WHERE datetime BETWEEN ? AND ?
                AND status = 'completed'
                ORDER BY datetime DESC
            ''', (start_timestamp, end_timestamp))
            
            matches = cursor.fetchall()
            conn.close()
            
            if not matches:
                await update.message.reply_text(
                    f"Нет данных о завершенных матчах за {yesterday.strftime('%d.%m.%Y')}.",
                    reply_markup=self.markup
                )
                return
            
            # Создаем кнопки для каждого матча
            keyboard = []
            
            # Сохраняем соответствие между названием матча и его ID в контексте пользователя
            if 'match_mapping' not in context.user_data:
                context.user_data['match_mapping'] = {}
            
            for match in matches:
                team1_name = match['team1_name'].split()[0]
                team2_name = match['team2_name'].split()[0]
                team1_score = match['team1_score']
                team2_score = match['team2_score']
                match_id = match['match_id']
                event_name = match['event_name']
                
                # Создаем текст кнопки
                match_text = f"{team1_name} {team1_score}:{team2_score} {team2_name}"
                
                # Сохраняем соответствие
                context.user_data['match_mapping'][match_text] = match_id
                
                # Добавляем кнопку
                keyboard.append([KeyboardButton(match_text)])
            
            # Добавляем кнопку "Назад"
            keyboard.append([KeyboardButton("Назад")])
            
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(
                f"Матчи за {yesterday.strftime('%d.%m.%Y')}. Выберите матч для просмотра подробной статистики:",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка матчей: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при получении списка матчей.",
                reply_markup=self.markup
            )
    
    def run(self):
        """
        Запускает бота
        """
        logger.info("Запуск бота...")
        application = Application.builder().token(self.token).build()
        
        # Регистрация обработчиков команд
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help))
        application.add_handler(CommandHandler("yesterday", self.send_yesterday_stats))
        application.add_handler(CommandHandler("today", self.send_today_stats))
        application.add_handler(CommandHandler("subscribe", self.subscribe))
        application.add_handler(CommandHandler("unsubscribe", self.unsubscribe))
        application.add_handler(CommandHandler("menu", self.show_menu))
        
        # Обработчик текстовых сообщений
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Обработчик ошибок
        application.add_error_handler(self.error)
        
        # Запуск бота
        application.run_polling(stop_signals=None)

def main():
    """
    Основная функция для запуска бота
    """
    bot = HLTVStatsBot(TOKEN, DB_PATH, SUBSCRIBERS_DB_PATH)
    bot.run()

if __name__ == "__main__":
    main() 