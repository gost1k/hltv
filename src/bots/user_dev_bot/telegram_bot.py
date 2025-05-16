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
                FROM match_details m
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
                    FROM match_upcoming m
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
                    FROM match_upcoming_players p
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
    
    # Здесь будет остальная логика, аналогичная user_bot
    # В финальной версии все методы должны быть реализованы

# Используем импорт из user_bot для сохранения идентичной логики
from src.bots.user_bot.telegram_bot import HLTVStatsBot

# Наследуем методы из базового бота HLTV
for method_name in dir(HLTVStatsBot):
    # Пропускаем магические методы, приватные методы и методы, которые уже определены
    if (not method_name.startswith('_') or method_name == '_get_safe_user_info') and method_name not in dir(UserDevBot):
        setattr(UserDevBot, method_name, getattr(HLTVStatsBot, method_name))