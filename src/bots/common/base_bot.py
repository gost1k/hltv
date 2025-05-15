#!/usr/bin/env python
"""
Базовый класс для телеграм-ботов HLTV
"""

import logging
import sys
import sqlite3
from datetime import datetime
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from src.bots.common.constants import MOSCOW_TIMEZONE

class BaseHLTVBot:
    """
    Базовый класс для телеграм-ботов HLTV
    """
    
    def __init__(self, token, db_path, name="BaseBot"):
        """
        Инициализация бота
        
        Args:
            token (str): Токен для Telegram API
            db_path (str): Путь к БД со статистикой
            name (str): Имя бота для логов
        """
        self.token = token
        self.db_path = db_path
        self.name = name
        
        # Настройка логирования для этого класса
        self.logger = logging.getLogger(f"{__name__}.{self.name}")
        # Проверяем, есть ли уже обработчики у логгера
        if not self.logger.handlers:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            
        # Используем локальный логгер
        self.logger.info(f"Инициализация бота {self.name}")
    
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
        Обработчик команды /start - должен быть переопределен в дочерних классах
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запуск команды /start")
        
        message = f"Привет, {user.first_name}! 👋\n\nЭто базовый бот HLTV."
        await update.message.reply_text(message)
    
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик команды /help - должен быть переопределен в дочерних классах
        """
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Запуск команды /help")
        
        message = "Справка по командам бота:\n\n/start - Начать работу с ботом\n/help - Показать эту справку"
        await update.message.reply_text(message)
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик текстовых сообщений - должен быть переопределен в дочерних классах
        """
        message_text = update.message.text
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(f"{user_info} - Сообщение: '{message_text}'")
        
        await update.message.reply_text("Это базовый обработчик сообщений. Переопределите его в дочернем классе.")
    
    def run(self):
        """
        Запускает бота
        """
        self.logger.info(f"Запуск бота {self.name}...")
        
        # Создаем экземпляр приложения Telegram
        application = Application.builder().token(self.token).build()
        
        # Регистрация обработчиков команд
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help))
        
        # Обработчик текстовых сообщений
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Обработчик ошибок
        application.add_error_handler(self.error)
        
        # Запуск бота
        application.run_polling(stop_signals=None) 