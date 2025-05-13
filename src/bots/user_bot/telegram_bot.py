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
            [KeyboardButton(MENU_UPCOMING_MATCHES)]
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
        await update.message.reply_text(
            "Выберите действие:",
            reply_markup=self.markup
        )
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик текстовых сообщений и нажатий на кнопки
        """
        message_text = update.message.text
        
        if message_text == MENU_COMPLETED_MATCHES:
            await self.show_completed_matches(update, context)
        elif message_text == MENU_UPCOMING_MATCHES:
            await self.show_upcoming_matches(update, context)
        elif message_text == "За вчера":
            await self.show_matches_for_period(update, context, 1)
        elif message_text == "За 3 дня":
            await self.show_matches_for_period(update, context, 3)
        elif message_text == "За неделю":
            await self.show_matches_for_period(update, context, 7)
        elif message_text == "Назад":
            await self.show_menu(update, context)
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
            [KeyboardButton("За вчера")],
            [KeyboardButton("За 3 дня")],
            [KeyboardButton("За неделю")],
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
        
        message_parts = []
        
        for event_id, event_data in events.items():
            event_name = event_data['name'] or "Без названия"
            matches = event_data['matches']
            
            event_header = f"🏆 *{event_name}*\n"
            message_parts.append(event_header)
            
            for match in matches:
                # Форматируем время
                match_time = datetime.fromtimestamp(match['datetime']).strftime('%d.%m %H:%M')
                
                # Форматируем результат
                team1_name = match['team1_name']
                team2_name = match['team2_name']
                team1_score = match['team1_score']
                team2_score = match['team2_score']
                
                # Выделяем победителя
                if team1_score > team2_score:
                    team1_name = f"*{team1_name}*"
                elif team2_score > team1_score:
                    team2_name = f"*{team2_name}*"
                
                match_line = f"• {match_time} {team1_name} {team1_score}:{team2_score} {team2_name}\n"
                message_parts.append(match_line)
            
            # Добавляем разделитель между событиями
            message_parts.append("\n")
        
        return "".join(message_parts)
    
    async def send_yesterday_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Отправляет статистику матчей за вчерашний день
        """
        await self.show_matches_for_period(update, context, 1)
    
    async def send_today_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Отправляет статистику матчей за сегодняшний день
        """
        # Получаем временные метки начала и конца текущего дня
        today = datetime.now()
        start_of_today = datetime(today.year, today.month, today.day, 0, 0, 0).timestamp()
        end_of_today = datetime(today.year, today.month, today.day, 23, 59, 59).timestamp()
        
        # Получаем матчи за текущий день
        events = self.get_matches_by_date(start_of_today, end_of_today)
        
        # Форматируем сообщение
        message = f"📊 *Результаты матчей за {today.strftime('%d.%m.%Y')}*\n\n"
        message += self.format_matches_message(events)
        
        # Отправляем сообщение
        await update.message.reply_text(message, parse_mode="Markdown", reply_markup=self.markup)
    
    async def show_matches_for_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days=1):
        """
        Показывает прошедшие матчи за указанный период
        
        Args:
            update: Объект обновления Telegram
            context: Контекст обработчика
            days (int): Количество дней для выборки (по умолчанию 1 день)
        """
        today = datetime.now()
        
        # Вычисляем начало и конец периода
        end_date = datetime(today.year, today.month, today.day, 0, 0, 0) - timedelta(days=1)
        end_timestamp = end_date.timestamp() + 86399  # Конец дня (23:59:59)
        start_date = end_date - timedelta(days=days-1)
        start_timestamp = start_date.timestamp()
        
        # Получаем матчи за период
        events = self.get_matches_by_date(start_timestamp, end_timestamp)
        
        if days == 1:
            period_text = f"за {end_date.strftime('%d.%m.%Y')}"
        else:
            period_text = f"за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}"
        
        # Форматируем сообщение
        message = f"📊 *Результаты матчей {period_text}*\n\n"
        message += self.format_matches_message(events)
        
        # Отправляем сообщение
        await update.message.reply_text(message, parse_mode="Markdown", reply_markup=self.markup)
    
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