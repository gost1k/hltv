#!/usr/bin/env python
"""
Телеграм-бот для разработчиков HLTV (получение ошибок и отладочной информации)
"""

import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import os

from src.bots.config import load_config

# Загружаем конфигурацию
config = load_config('dev')

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

class DevBot:
    """
    Бот для разработчиков проекта HLTV
    """
    def __init__(self, token, db_path):
        """
        Инициализация бота
        
        Args:
            token (str): Токен телеграм-бота
            db_path (str): Путь к файлу базы данных HLTV
        """
        self.token = token
        self.db_path = db_path
        self.admin_chat_ids = config.get('admin_chat_ids', [])
        
        # Создаем простую клавиатуру с кнопками
        self.menu_keyboard = [
            [KeyboardButton("Статус системы")],
            [KeyboardButton("Скачать БД")]
        ]
        self.markup = ReplyKeyboardMarkup(self.menu_keyboard, resize_keyboard=True)
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик команды /start
        """
        user = update.effective_user
        message = (
            f"Привет, {user.first_name}! 👋\n\n"
            f"Это бот для разработчиков HLTV."
        )
        await update.message.reply_text(message, reply_markup=self.markup)
    
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик команды /help
        """
        message = "Справка по командам бота:\n\n/start - Начать работу с ботом\n/help - Показать эту справку"
        await update.message.reply_text(message, reply_markup=self.markup)
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Обработчик текстовых сообщений и нажатий на кнопки
        """
        user = update.effective_user
        chat_id = user.id
        text = update.message.text.strip()
        if text == "Скачать БД":
            db_path = os.path.abspath(self.db_path)
            logger.info(f"Путь к базе: {db_path}, Существует: {os.path.exists(db_path)}")
            if self.admin_chat_ids and chat_id not in self.admin_chat_ids:
                await update.message.reply_text("У вас нет прав для скачивания базы данных.", reply_markup=self.markup)
                return
            if not os.path.exists(db_path):
                await update.message.reply_text(f"Файл базы данных не найден: {db_path}", reply_markup=self.markup)
                return
            await update.message.reply_document(document=InputFile(db_path), filename=os.path.basename(db_path), caption="Файл базы данных HLTV")
            return
        # На любое другое сообщение отвечаем "Работает"
        await update.message.reply_text("Работает", reply_markup=self.markup)
    
    async def error(self, update, context):
        """
        Обработчик ошибок
        """
        logger.error(f"Ошибка: {context.error} при обработке запроса {update}")
    
    def run(self):
        """
        Запускает бота
        """
        logger.info("Запуск бота для разработчиков...")
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

def main():
    """
    Основная функция для запуска бота
    """
    bot = DevBot(TOKEN, DB_PATH)
    bot.run()

if __name__ == "__main__":
    main() 