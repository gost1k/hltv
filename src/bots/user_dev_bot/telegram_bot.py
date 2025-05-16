#!/usr/bin/env python
"""
Телеграм-бот для отправки статистики матчей HLTV (версия для разработчиков)
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import sys
import tempfile
import os

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
            
            # Выводим счёт, если он есть (прошедший матч)
            team1_score = match['team1_score']
            team2_score = match['team2_score']
            if team1_score is not None and team2_score is not None:
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
            else:
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
            
            # Добавляем информацию о стримах для предстоящих матчей
            ics_button_markup = None
            ics_file_path = None
            if match_type == 'upcoming':
                try:
                    conn2 = sqlite3.connect(self.db_path)
                    conn2.row_factory = sqlite3.Row
                    cursor2 = conn2.cursor()
                    cursor2.execute('''
                        SELECT name, lang, url FROM upcoming_match_streamers WHERE match_id = ?
                    ''', (match_id,))
                    streams = cursor2.fetchall()
                    conn2.close()
                    if streams:
                        message += '\n<b>Где посмотреть:</b>\n'
                        for s in streams:
                            lang = f" ({s['lang']})" if s['lang'] else ''
                            message += f"• <a href=\"{s['url']}\">{s['name']}{lang}</a>\n"
                except Exception as e:
                    self.logger.error(f"Ошибка при получении стримеров для матча {match_id}: {str(e)}")
            # Отправляем сообщение
            await update.message.reply_text(message, parse_mode="HTML", reply_markup=ics_button_markup if ics_button_markup else self.markup)
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении данных о матче {match_id}: {str(e)}")
            await update.message.reply_text(
                "Произошла ошибка при получении данных о матче.",
                reply_markup=self.markup
            )
    
    async def show_completed_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи."
        )

    async def show_upcoming_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи."
        )

    async def show_matches_for_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days=1):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        today = datetime.now(MOSCOW_TIMEZONE)
        end_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=MOSCOW_TIMEZONE) - timedelta(days=1)
        end_timestamp = end_date.timestamp() + 86399
        start_date = end_date - timedelta(days=days-1)
        start_timestamp = start_date.timestamp()
        self.logger.info(f"{user_info} - Запрос матчей за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}")
        events = self.get_matches_by_date(start_timestamp, end_timestamp)
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        self.logger.info(f"{user_info} - Найдено {match_count} матчей за указанный период")
        if days == 1:
            period_text = f"за {end_date.strftime('%d.%m.%Y')}"
        else:
            period_text = f"за период с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}"
        message = f"📊 <b>Результаты матчей {period_text}</b>\n\n"
        message += self.format_matches_message(events)
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
        await update.message.reply_text(
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи команды."
        )

    async def show_upcoming_matches_for_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days=0):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        today = datetime.now(MOSCOW_TIMEZONE)
        current_timestamp = today.timestamp()
        if days == 0:
            start_timestamp = current_timestamp
            end_date = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=MOSCOW_TIMEZONE)
            end_timestamp = end_date.timestamp()
            period_text = "на сегодня"
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на сегодня ({start_timestamp} - {end_timestamp})")
        elif days == 1:
            tomorrow = today + timedelta(days=1)
            start_date = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0, tzinfo=MOSCOW_TIMEZONE)
            end_date = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 23, 59, 59, tzinfo=MOSCOW_TIMEZONE)
            start_timestamp = start_date.timestamp()
            end_timestamp = end_date.timestamp()
            period_text = f"на завтра ({start_date.strftime('%d.%m.%Y')})"
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на завтра ({start_timestamp} - {end_timestamp})")
        else:
            start_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=MOSCOW_TIMEZONE)
            end_date = start_date + timedelta(days=days)
            start_timestamp = current_timestamp
            end_timestamp = end_date.timestamp()
            period_text = f"на ближайшие {days} дней"
            self.logger.info(f"{user_info} - Запрос предстоящих матчей на {days} дней ({start_timestamp} - {end_timestamp})")
        events = self.get_upcoming_matches_by_date(start_timestamp, end_timestamp)
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        self.logger.info(f"{user_info} - Найдено {match_count} предстоящих матчей за указанный период")
        message = f"📅 <b>Предстоящие матчи {period_text}</b>\n\n"
        message += self.format_upcoming_matches_message(events)
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)
        await update.message.reply_text(
            "Введите название команды, например Natus Vincere, чтобы посмотреть будущие и прошедшие матчи команды."
        )

# Используем импорт из user_bot для сохранения идентичной логики
from src.bots.user_bot.telegram_bot import HLTVStatsBot

# Наследуем методы из базового бота HLTV
for method_name in dir(HLTVStatsBot):
    # Пропускаем магические методы, приватные методы и методы, которые уже определены
    if (not method_name.startswith('_') or method_name == '_get_safe_user_info') and method_name not in dir(UserDevBot):
        setattr(UserDevBot, method_name, getattr(HLTVStatsBot, method_name))