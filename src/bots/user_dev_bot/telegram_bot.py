#!/usr/bin/env python
"""
Телеграм-бот для отправки статистики матчей HLTV (версия для разработчиков)
"""

import logging
import sqlite3
from datetime import datetime
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import sys

from src.bots.config import load_config
from src.bots.common.base_bot import BaseHLTVBot
from src.bots.common.constants import (
    MOSCOW_TIMEZONE, MENU_UPCOMING_MATCHES, MENU_COMPLETED_MATCHES,
    DATE_FORMAT, TIME_FORMAT, DATETIME_FORMAT
)

# Загружаем конфигурацию
config = load_config('user_dev')

# Настройка логгера
logger = logging.getLogger(__name__)

class UserDevBot(BaseHLTVBot):
    """
    Телеграм-бот для отображения статистики HLTV (версия для разработчиков)
    """
    
    def __init__(self, token, db_path):
        """
        Инициализация бота
        
        Args:
            token (str): Токен для Telegram API
            db_path (str): Путь к БД со статистикой HLTV
        """
        super().__init__(token, db_path, name="UserDevBot")
        
        # Создаем клавиатуру с кнопками меню
        self.menu_keyboard = [
            [KeyboardButton(MENU_COMPLETED_MATCHES)],
            [KeyboardButton(MENU_UPCOMING_MATCHES)]
        ]
        self.markup = ReplyKeyboardMarkup(self.menu_keyboard, resize_keyboard=True)
        
        self.logger.info("Инициализация бота UserDevBot завершена")
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик команды /start
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запуск команды /start")
        
        message = (
            f"Привет, {user.first_name}! 👋\n\n"
            f"Я бот для отображения статистики матчей HLTV (версия для разработчиков).\n\n"
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
        
        if message_text == MENU_COMPLETED_MATCHES:
            self.logger.info(f"{user_info} - Запрос прошедших матчей")
            context.user_data['showing_menu'] = MENU_COMPLETED_MATCHES
            await self.show_completed_matches(update, context)
        elif message_text == MENU_UPCOMING_MATCHES:
            self.logger.info(f"{user_info} - Запрос предстоящих матчей")
            context.user_data['showing_menu'] = MENU_UPCOMING_MATCHES
            await self.show_upcoming_matches(update, context)
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
            # Обработка запроса статистики с ID в скобках
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
    
    # Здесь будет остальная логика, аналогичная user_bot
    # В финальной версии все методы должны быть реализованы

# Используем импорт из user_bot для сохранения идентичной логики
from src.bots.user_bot.telegram_bot import HLTVStatsBot

# Наследуем методы из базового бота HLTV
for method_name in dir(HLTVStatsBot):
    # Пропускаем магические методы, приватные методы и методы, которые уже определены
    if (not method_name.startswith('_') or method_name == '_get_safe_user_info') and method_name not in dir(UserDevBot):
        setattr(UserDevBot, method_name, getattr(HLTVStatsBot, method_name))