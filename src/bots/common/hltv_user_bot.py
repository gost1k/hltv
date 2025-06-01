#!/usr/bin/env python
"""
Общий класс Telegram-бота HLTV для пользователей (user и user_dev)
"""

import os
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import sys
import tempfile
import traceback
import asyncio
import time
import uuid
import re
import json

from src.bots.config import load_config
from src.scripts.live_matches_parser import handle_new_subscription, load_json, save_json, SUBS_JSON, LIVE_JSON, subscriber_event, move_future_subscribers_to_live, subscribe_user, unsubscribe_user, load_subs_json
from src.bots.common.hltv_user_bot_texts import BOT_TEXTS

# Отключаем лишние логи Telegram API и httpx/urllib3
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.vendor.ptb_urllib3.urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._application").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._updater").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

BOT_LOG_PATH = os.path.join(os.path.dirname(__file__), '../../../storage/json/bot/user_actions.json')

def log_user_action(telegram_id, action, value=None):
    os.makedirs(os.path.dirname(BOT_LOG_PATH), exist_ok=True)
    try:
        if os.path.exists(BOT_LOG_PATH):
            with open(BOT_LOG_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
        else:
            data = {"users": {}}
    except Exception:
        data = {"users": {}}
    user_log = data["users"].setdefault(str(telegram_id), {})
    # Лог действий
    actions = user_log.setdefault("actions", [])
    now = datetime.now().isoformat(timespec='seconds')
    actions.append({"action": action, "value": value, "timestamp": now})
    user_log["actions"] = actions
    # Счётчик всех действий
    user_log["total_actions"] = user_log.get("total_actions", 0) + 1
    # Дата последнего действия
    user_log["last_action_time"] = now
    data["users"][str(telegram_id)] = user_log
    with open(BOT_LOG_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# Класс HLTVUserBot будет содержать всю бизнес-логику user-бота
class HLTVUserBot:
    def __init__(self, token, db_path, log_file, config_name='user'):
        self.token = token
        self.db_path = db_path
        self.log_file = log_file
        self.config_name = config_name

        # Настройка логирования для этого класса
        self.logger = logging.getLogger(__name__)
        self.logger.info(BOT_TEXTS['log']['init'].format(config_name=config_name))

        # Кнопки меню
        self.MENU_AI_PREDICTIONS = "AI прогнозы"
        self.MENU_UPCOMING_MATCHES = "Будущие матчи"
        self.MENU_COMPLETED_MATCHES = "Прошедшие матчи"
        self.MENU_LIVE_MATCHES = "Live матчи"
        self.MOSCOW_TIMEZONE = timezone(timedelta(hours=3))
        self.menu_keyboard = [
            [KeyboardButton(self.MENU_AI_PREDICTIONS)],
            [KeyboardButton(self.MENU_LIVE_MATCHES)],
            [KeyboardButton(self.MENU_UPCOMING_MATCHES)],
            [KeyboardButton(self.MENU_COMPLETED_MATCHES)]
        ]
        self.markup = ReplyKeyboardMarkup(self.menu_keyboard, resize_keyboard=True)
        self.live_keyboard = [[KeyboardButton("Назад")]]
        self.live_markup = ReplyKeyboardMarkup(self.live_keyboard, resize_keyboard=True)
        self._last_ok_log = 0
        self._ok_log_interval = 5 * 60  # 5 минут

    def _get_safe_user_info(self, user):
        try:
            first_name = user.first_name if user.first_name else ""
            last_name = user.last_name if user.last_name else ""
            username = user.username if user.username else "no_username"
            first_name = ''.join(c if ord(c) < 128 else '_' for c in first_name)
            last_name = ''.join(c if ord(c) < 128 else '_' for c in last_name)
            return f"User: {first_name} {last_name} (@{username}) [ID: {user.id}]"
        except:
            return f"User ID: {user.id}"

    async def error(self, update, context):
        self.logger.error(f"Ошибка: {context.error} при обработке запроса {update}")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(BOT_TEXTS['log']['start_command'].format(user_info=user_info))
        message = BOT_TEXTS['start'].format(first_name=user.first_name)
        await update.message.reply_text(message, reply_markup=self.markup)

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(BOT_TEXTS['log']['help_command'].format(user_info=user_info))
        message = BOT_TEXTS['help']
        await update.message.reply_text(message, reply_markup=self.markup)

    async def show_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(BOT_TEXTS['log']['main_menu'].format(user_info=user_info))
        await update.message.reply_text(BOT_TEXTS['menu'], reply_markup=self.markup)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message_text = update.message.text
        self.logger.info(f"handle_message called, message_text: {message_text}")
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        log_user_action(user.id, "button", message_text)
        period_buttons = [
            "За сегодня", "За вчера", "За 3 дня",
            "На сегодня", "На завтра", "На 3 дня",
            "По событию", "Назад",
            self.MENU_COMPLETED_MATCHES, self.MENU_UPCOMING_MATCHES, self.MENU_LIVE_MATCHES, self.MENU_AI_PREDICTIONS
        ]
        if message_text not in period_buttons:
            self.logger.info(BOT_TEXTS['log']['message'].format(user_info=user_info, message_text=message_text))
        context.user_data['last_button'] = message_text
        if message_text.startswith("/subscribe_live "):
            try:
                match_id = int(message_text.split(" ")[1])
                handle_new_subscription(match_id, user.id)
                log_user_action(user.id, "subscribe_live", match_id)
                self.logger.info(BOT_TEXTS['log']['subscribe_live'].format(user_info=user_info, match_id=match_id))
                await update.message.reply_text(BOT_TEXTS['subscribe_success'].format(match_id=match_id))
            except Exception as e:
                await update.message.reply_text(BOT_TEXTS['subscribe_error'])
            return
        if message_text.startswith("/unsubscribe_live "):
            try:
                match_id = int(message_text.split(" ")[1])
                subs = load_json(SUBS_JSON, default={})
                match_id_str = str(match_id)
                if match_id_str in subs and user.id in subs[match_id_str]:
                    subs[match_id_str].remove(user.id)
                    if not subs[match_id_str]:
                        subs.pop(match_id_str)
                    save_json(SUBS_JSON, subs)
                    from src.scripts.live_matches_parser import subscriber_event
                    subscriber_event.set()
                    log_user_action(user.id, "unsubscribe_live", match_id)
                    self.logger.info(BOT_TEXTS['log']['unsubscribe_live'].format(user_info=user_info, match_id=match_id))
                    await update.message.reply_text(BOT_TEXTS['unsubscribe_success'].format(match_id=match_id))
                else:
                    await update.message.reply_text(BOT_TEXTS['not_subscribed'])
            except Exception as e:
                await update.message.reply_text(BOT_TEXTS['unsubscribe_error'])
            return
        if message_text == self.MENU_COMPLETED_MATCHES:
            self.logger.info(BOT_TEXTS['log']['completed_matches_request'].format(user_info=user_info))
            context.user_data['showing_menu'] = self.MENU_COMPLETED_MATCHES
            await self.show_completed_matches(update, context)
            return
        elif message_text == self.MENU_UPCOMING_MATCHES:
            self.logger.info(BOT_TEXTS['log']['upcoming_matches_request'].format(user_info=user_info))
            context.user_data['showing_menu'] = self.MENU_UPCOMING_MATCHES
            await self.show_upcoming_matches(update, context)
            return
        elif message_text == self.MENU_LIVE_MATCHES:
            self.logger.info(BOT_TEXTS['log']['live_matches_request'].format(user_info=user_info))
            await self.show_live_matches(update, context)
            return
        elif message_text == self.MENU_AI_PREDICTIONS:
            self.logger.info("AI прогнозы: пользователь запросил прогнозы на будущие матчи")
            await self.show_ai_predictions(update, context)
            return
        elif message_text == "За сегодня":
            today = datetime.now(self.MOSCOW_TIMEZONE)
            date_str = today.strftime('%d.%m.%Y')
            self.logger.info(BOT_TEXTS['log']['period_matches_request'].format(user_info=user_info, start=date_str, end=date_str))
            await self.show_matches_for_period(update, context, days=1, for_today=True)
            return
        elif message_text == "За вчера":
            today = datetime.now(self.MOSCOW_TIMEZONE)
            end_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=self.MOSCOW_TIMEZONE) - timedelta(days=1)
            date_str = end_date.strftime('%d.%m.%Y')
            self.logger.info(BOT_TEXTS['log']['period_matches_request'].format(user_info=user_info, start=date_str, end=date_str))
            await self.show_matches_for_period(update, context, days=1, for_today=False)
            return
        elif message_text == "За 3 дня":
            today = datetime.now(self.MOSCOW_TIMEZONE)
            end_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=self.MOSCOW_TIMEZONE) - timedelta(days=1)
            start_date = end_date - timedelta(days=2)
            self.logger.info(BOT_TEXTS['log']['period_matches_request'].format(user_info=user_info, start=start_date.strftime('%d.%m.%Y'), end=end_date.strftime('%d.%m.%Y')))
            await self.show_matches_for_period(update, context, 3)
            return
        elif message_text == "На сегодня":
            today = datetime.now(self.MOSCOW_TIMEZONE)
            date_str = today.strftime('%d.%m.%Y')
            self.logger.info(BOT_TEXTS['log']['period_matches_request'].format(user_info=user_info, start=date_str, end=date_str))
            await self.show_upcoming_matches_for_period(update, context, 0)
            return
        elif message_text == "На завтра":
            tomorrow = datetime.now(self.MOSCOW_TIMEZONE) + timedelta(days=1)
            date_str = tomorrow.strftime('%d.%m.%Y')
            self.logger.info(BOT_TEXTS['log']['period_matches_request'].format(user_info=user_info, start=date_str, end=date_str))
            await self.show_upcoming_matches_for_period(update, context, 1)
            return
        elif message_text == "На 3 дня":
            today = datetime.now(self.MOSCOW_TIMEZONE)
            start_date = today
            end_date = today + timedelta(days=2)
            self.logger.info(BOT_TEXTS['log']['period_matches_request'].format(user_info=user_info, start=start_date.strftime('%d.%m.%Y'), end=end_date.strftime('%d.%m.%Y')))
            await self.show_upcoming_matches_for_period(update, context, 3)
            return
        elif message_text == "По событию":
            self.logger.info(BOT_TEXTS['log']['events_list_request'].format(user_info=user_info))
            await self.show_events_list(update, context)
            return
        elif message_text == "Назад":
            self.logger.info(BOT_TEXTS['log']['back_to_menu'].format(user_info=user_info))
            await self.show_menu(update, context)
            return
        elif 'match_mapping' in context.user_data:
            self.logger.info(f"match_mapping: {context.user_data['match_mapping']}")
            self.logger.info(f"message_text: {message_text}")
        if 'match_mapping' in context.user_data and message_text in context.user_data['match_mapping']:
            match_id = context.user_data['match_mapping'][message_text]
            self.logger.info(BOT_TEXTS['log']['match_details_request'].format(user_info=user_info, match_id=match_id))
            await self.show_match_details(update, context, match_id)
            return
        elif 'event_mapping' in context.user_data and message_text in context.user_data['event_mapping']:
            event_id = context.user_data['event_mapping'][message_text]
            self.logger.info(BOT_TEXTS['log']['matches_for_event_request'].format(user_info=user_info, event_id=event_id))
            await self.show_matches_for_event(update, context, event_id)
            return
        elif "(" in message_text and ")" in message_text:
            try:
                match_id_text = message_text.split("(")[-1].split(")")[0].strip()
                match_id = int(''.join(filter(str.isdigit, match_id_text)))
                await self.show_match_details(update, context, match_id)
            except (ValueError, IndexError):
                await update.message.reply_text(
                    BOT_TEXTS['errors']['unknown_match_id'],
                    reply_markup=self.markup
                )
        elif 'live_match_mapping' in context.user_data and message_text in context.user_data['live_match_mapping']:
            match_id = context.user_data['live_match_mapping'][message_text]
            await self.show_match_details(update, context, match_id)
            return
        elif 'upcoming_match_mapping' in context.user_data and message_text in context.user_data['upcoming_match_mapping']:
            match_id = context.user_data['upcoming_match_mapping'][message_text]
            await self.show_match_details(update, context, match_id)
            return
        elif 'fallback_live_match_mapping' in context.user_data and message_text in context.user_data['fallback_live_match_mapping']:
            match_id = context.user_data['fallback_live_match_mapping'][message_text]
            await self.show_live_match_details(update, context, match_id)
            return
        else:
            await self.find_matches_by_team(update, context, message_text)

    async def periodic_ok_log(self):
        while True:
            now = time.time()
            if now - self._last_ok_log > self._ok_log_interval:
                self.logger.info("Telegram polling: OK")
                self._last_ok_log = now
            await asyncio.sleep(10)

    def run(self):
        self.logger.info("Запуск бота...")
        application = Application.builder().token(self.token).build()
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        application.add_handler(CallbackQueryHandler(self.handle_callback_query))
        application.add_error_handler(self.error)
        loop = asyncio.get_event_loop()
        loop.create_task(self.periodic_ok_log())
        application.run_polling(stop_signals=None)

    async def show_completed_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [KeyboardButton("Назад")],
            [KeyboardButton("За сегодня")],
            [KeyboardButton("За вчера")],
            [KeyboardButton("По событию")],
        ]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            BOT_TEXTS['choose_period_completed'],
            reply_markup=markup
        )
        await update.message.reply_text(
            BOT_TEXTS['input_team']
        )

    async def show_upcoming_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [KeyboardButton("Назад")],
            [KeyboardButton("На сегодня")],
            [KeyboardButton("На завтра")],
            [KeyboardButton("По событию")],
        ]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            BOT_TEXTS['choose_period_upcoming'],
            reply_markup=markup
        )
        await update.message.reply_text(
            BOT_TEXTS['input_team']
        )

    async def send_yesterday_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(BOT_TEXTS['log']['yesterday_stats_request'].format(user_info=user_info))
        await self.show_matches_for_period(update, context, 1)

    async def send_today_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(BOT_TEXTS['log']['today_stats_request'].format(user_info=user_info))
        today = datetime.now(self.MOSCOW_TIMEZONE)
        start_of_today = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=self.MOSCOW_TIMEZONE).timestamp()
        end_of_today = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=self.MOSCOW_TIMEZONE).timestamp()
        events = self.get_matches_by_date(start_of_today, end_of_today)
        message = f"📊 <b>{BOT_TEXTS['today_stats_title'].format(date=today.strftime('%d.%m.%Y'))}</b>\n\n"
        message += self.format_matches_message(events)
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        self.logger.info(BOT_TEXTS['log']['found_matches_today'].format(user_info=user_info, count=match_count))
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup)

    async def show_matches_for_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days=1, for_today=False):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        log_user_action(user.id, "view_completed_period", days)
        today = datetime.now(self.MOSCOW_TIMEZONE)
        if for_today:
            start_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=self.MOSCOW_TIMEZONE)
            end_date = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=self.MOSCOW_TIMEZONE)
        else:
            end_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=self.MOSCOW_TIMEZONE) - timedelta(days=1)
            start_date = end_date.replace(hour=0, minute=0, second=0)
            end_date = end_date.replace(hour=23, minute=59, second=59)
        start_timestamp = start_date.timestamp()
        end_timestamp = end_date.timestamp()
        events = self.get_matches_by_date(start_timestamp, end_timestamp)
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        if days == 1:
            period_text = f"{BOT_TEXTS['period_text_single']} {start_date.strftime('%d.%m.%Y')}"
        else:
            period_text = f"{BOT_TEXTS['period_text_range']} {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}"
        message = f"📊 <b>{period_text}</b>\n\n"
        message += self.format_matches_message(events)
        keyboard = [[KeyboardButton('Назад')]]
        if 'match_mapping' not in context.user_data:
            context.user_data['match_mapping'] = {}
        # Собираем все матчи из events
        all_matches = []
        for event_data in events.values():
            all_matches.extend(event_data['matches'])
        all_matches.sort(key=lambda m: m['datetime'])
        for match in all_matches:
            team1_name = match['team1_name']
            team2_name = match['team2_name']
            match_id = match['match_id']
            # Если есть счёт, это завершённый матч
            if 'team1_score' in match and 'team2_score' in match:
                match_text = f"{team1_name} {match['team1_score']}:{match['team2_score']} {team2_name}"
            else:
                match_text = f"{team1_name} vs {team2_name}"
            context.user_data['match_mapping'][match_text] = match_id
            keyboard.append([KeyboardButton(match_text)])
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=reply_markup)

    async def show_events_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Определяем, какой тип событий запрашивается (прошедшие или предстоящие)
        if 'showing_menu' not in context.user_data:
            if context.user_data.get('last_button') in ["На сегодня", "На завтра", "На 3 дня", self.MENU_UPCOMING_MATCHES]:
                context.user_data['showing_menu'] = self.MENU_UPCOMING_MATCHES
            else:
                context.user_data['showing_menu'] = self.MENU_COMPLETED_MATCHES
        event_type = context.user_data['showing_menu']
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(BOT_TEXTS['log']['events_list_request'].format(user_info=user_info))
        today = datetime.now(self.MOSCOW_TIMEZONE)
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if event_type == self.MENU_COMPLETED_MATCHES:
                start_date = today - timedelta(days=7)
                start_timestamp = start_date.timestamp()
                end_timestamp = today.timestamp()
                cursor.execute('''
                    SELECT DISTINCT event_id, event_name 
                    FROM result_match
                    WHERE datetime BETWEEN ? AND ?
                    AND event_id IS NOT NULL
                    AND event_name IS NOT NULL
                    ORDER BY event_name
                ''', (start_timestamp, end_timestamp))
            else:
                start_timestamp = today.timestamp()
                end_date = today + timedelta(days=14)
                end_timestamp = end_date.timestamp()
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
                period_str = BOT_TEXTS['no_events_week'] if event_type == self.MENU_COMPLETED_MATCHES else BOT_TEXTS['no_events_14days']
                await update.message.reply_text(period_str, reply_markup=self.markup)
                return
            if 'event_mapping' not in context.user_data:
                context.user_data['event_mapping'] = {}
            keyboard = []
            for event in events:
                event_name = event['event_name']
                event_id = event['event_id']
                context.user_data['event_mapping'][event_name] = event_id
                keyboard.append([KeyboardButton(event_name)])
            keyboard = [[KeyboardButton(BOT_TEXTS['back'])]] + keyboard
            markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            event_type_str = BOT_TEXTS['choose_event_completed'] if event_type == self.MENU_COMPLETED_MATCHES else BOT_TEXTS['choose_event_upcoming']
            await update.message.reply_text(event_type_str, reply_markup=markup)
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка событий: {str(e)}")
            await update.message.reply_text(BOT_TEXTS['error_getting_events'], reply_markup=self.markup)

    async def show_matches_for_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE, event_id):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        log_user_action(user.id, "view_event", event_id)
        self.logger.info(BOT_TEXTS['log']['matches_for_event_request'].format(user_info=user_info, event_id=event_id))
        event_type = context.user_data.get('showing_menu', self.MENU_COMPLETED_MATCHES)
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if event_type == self.MENU_COMPLETED_MATCHES:
                cursor.execute('SELECT event_name FROM result_match WHERE event_id = ? LIMIT 1', (event_id,))
                event_result = cursor.fetchone()
                if not event_result:
                    message = BOT_TEXTS['event_not_found']
                    await update.message.reply_text(message, reply_markup=self.markup)
                    conn.close()
                    return
                event_name = event_result['event_name']
                cursor.execute('''
                    SELECT match_id, datetime, team1_id, team1_name, team1_score, team2_id, team2_name, team2_score
                    FROM result_match
                    WHERE event_id = ?
                    ORDER BY datetime
                ''', (event_id,))
                matches = cursor.fetchall()
                conn.close()
                if not matches:
                    message = BOT_TEXTS['no_matches_event_completed'].format(event_name=event_name)
                    await update.message.reply_text(message, reply_markup=self.markup)
                    return
                # Новый заголовок и формат для прошедших матчей по событию
                message = f"<b>Прошедшие матчи события:</b>\n{event_name}\n\n"
                match_buttons = []
                for match in matches:
                    match_datetime = datetime.fromtimestamp(match['datetime'], tz=self.MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M')
                    team1_name = match['team1_name']
                    team2_name = match['team2_name']
                    team1_score = match['team1_score']
                    team2_score = match['team2_score']
                    message += f"• <b>{match_datetime}</b> <code>{team1_name}</code> {team1_score}:{team2_score} <code>{team2_name}</code>\n"
                    match_text = f"{team1_name} {team1_score}:{team2_score} {team2_name}"
                    match_buttons.append([KeyboardButton(match_text)])
                # Клавиатура выбора матча для прошедших матчей по событию
                keyboard = [[KeyboardButton('Назад')]] + match_buttons
                if 'match_mapping' not in context.user_data:
                    context.user_data['match_mapping'] = {}
                else:
                    self.logger.info("Перезаписываю match_mapping для новых матчей по событию")
                    context.user_data['match_mapping'].clear()
                for match in matches:
                    team1_name = match['team1_name']
                    team2_name = match['team2_name']
                    team1_score = match['team1_score']
                    team2_score = match['team2_score']
                    match_id = match['match_id']
                    match_text = f"{team1_name} {team1_score}:{team2_score} {team2_name}"
                    context.user_data['match_mapping'][match_text] = match_id
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                await update.message.reply_text(message, parse_mode="HTML", reply_markup=reply_markup)
                return
            else:
                # Ветка для будущих матчей по событию
                cursor.execute('SELECT event_name FROM upcoming_match WHERE event_id = ? LIMIT 1', (event_id,))
                event_result = cursor.fetchone()
                if not event_result:
                    message = BOT_TEXTS['event_not_found']
                    await update.message.reply_text(message, reply_markup=self.markup)
                    conn.close()
                    return
                event_name = event_result['event_name']
                cursor.execute('''
                    SELECT match_id, datetime, team1_id, team1_name, team2_id, team2_name
                    FROM upcoming_match
                    WHERE event_id = ?
                    ORDER BY datetime
                ''', (event_id,))
                matches = cursor.fetchall()
                conn.close()
                if not matches:
                    message = BOT_TEXTS['no_matches_event_upcoming'].format(event_name=event_name)
                    await update.message.reply_text(message, reply_markup=self.markup)
                    return
                # Формируем текст и клавиатуру
                message = BOT_TEXTS['upcoming_event_matches_header'].format(event_name=event_name)
                for match in matches:
                    match_datetime = datetime.fromtimestamp(match['datetime'], tz=self.MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M')
                    team1_name = match['team1_name']
                    team2_name = match['team2_name']
                    message += f"<b>{match_datetime}</b>: <code>{team1_name}</code> vs <code>{team2_name}</code>\n"
                # Клавиатура выбора матча для будущих матчей по событию
                keyboard = [[KeyboardButton('Назад')]]
                if 'match_mapping' not in context.user_data:
                    context.user_data['match_mapping'] = {}
                else:
                    context.user_data['match_mapping'].clear()
                for match in matches:
                    team1_name = match['team1_name']
                    team2_name = match['team2_name']
                    match_id = match['match_id']
                    match_text = f"{team1_name} vs {team2_name}"
                    context.user_data['match_mapping'][match_text] = match_id
                    keyboard.append([KeyboardButton(match_text)])
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                await update.message.reply_text(message, parse_mode="HTML", reply_markup=reply_markup)
                return
        except Exception as e:
            self.logger.error(f"Ошибка при получении матчей события {event_id}: {str(e)}")
            await update.message.reply_text(BOT_TEXTS['error_getting_event'], reply_markup=self.markup)

    async def show_match_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE, match_id):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        log_user_action(user.id, "view_match", match_id)
        self.logger.info(BOT_TEXTS['log']['match_details_request'].format(user_info=user_info, match_id=match_id))
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('''
                SELECT m.match_id, m.datetime, m.team1_id, m.team1_name, m.team1_score, m.team1_rank,
                       m.team2_id, m.team2_name, m.team2_score, m.team2_rank, m.event_id, m.event_name, m.demo_id, 'completed' as match_type
                FROM result_match m WHERE m.match_id = ?
            ''', (match_id,))
            match = cursor.fetchone()
            if not match:
                cursor.execute('''
                    SELECT m.match_id, m.datetime, m.team1_id, m.team1_name, 0 as team1_score, m.team1_rank,
                           m.team2_id, m.team2_name, 0 as team2_score, m.team2_rank, m.event_id, m.event_name, NULL as demo_id, 'upcoming' as match_type
                    FROM upcoming_match m WHERE m.match_id = ?
                ''', (match_id,))
                match = cursor.fetchone()
            if not match:
                await update.message.reply_text(BOT_TEXTS['match_not_found'].format(match_id=match_id), reply_markup=self.markup)
                conn.close()
                return
            player_stats = []
            match_type = match['match_type']
            if match_type == 'completed':
                cursor.execute('''
                    SELECT p.nickname, p.team_id, p.kills, p.deaths, p.kd_ratio, p.adr, p.kast, p.rating
                    FROM player_stats p WHERE p.match_id = ? ORDER BY p.team_id, p.rating DESC
                ''', (match_id,))
                player_stats = cursor.fetchall()
            elif match_type == 'upcoming':
                cursor.execute('''
                    SELECT p.player_nickname as nickname, p.team_id
                    FROM upcoming_match_players p WHERE p.match_id = ? ORDER BY p.team_id
                ''', (match_id,))
                player_stats = cursor.fetchall()
            conn.close()
            match_datetime = datetime.fromtimestamp(match['datetime'], tz=self.MOSCOW_TIMEZONE)
            team1_name = match['team1_name']
            team2_name = match['team2_name']
            event_name = match['event_name']
            message = BOT_TEXTS['match_details_header'].format(datetime=match_datetime.strftime('%d.%m.%Y %H:%M'), event_name=event_name)
            if match_type == 'completed':
                team1_score = match['team1_score']
                team2_score = match['team2_score']
                t1 = f"🏆 <b>{team1_name}</b>" if team1_score > team2_score else team1_name
                t2 = f"<b>{team2_name}</b>" if team2_score > team1_score else team2_name
                message += BOT_TEXTS['match_score'].format(team1=t1, score1=team1_score, score2=team2_score, team2=t2)
                try:
                    conn2 = sqlite3.connect(self.db_path)
                    conn2.row_factory = sqlite3.Row
                    cursor2 = conn2.cursor()
                    cursor2.execute('''
                        SELECT map_name, team1_rounds, team2_rounds, rounds
                        FROM result_match_maps WHERE match_id = ? ORDER BY id
                    ''', (match_id,))
                    maps = cursor2.fetchall()
                    conn2.close()
                    if maps:
                        message += BOT_TEXTS['maps_stats_header']
                        for m in maps:
                            rounds = f" {m['rounds']}" if m['rounds'] else ""
                            message += BOT_TEXTS['maps_stats_line'].format(map_name=m['map_name'], team1_rounds=m['team1_rounds'], rounds=rounds, team2_rounds=m['team2_rounds'])
                        message += '\n'
                except Exception as e:
                    self.logger.error(f"Ошибка при получении сыгранных карт для матча {match_id}: {str(e)}")
            else:
                message += BOT_TEXTS['match_vs'].format(team1=team1_name, team2=team2_name)
            if match['team1_rank'] or match['team2_rank']:
                team1_rank = f"#{match['team1_rank']}" if match['team1_rank'] else "нет данных"
                team2_rank = f"#{match['team2_rank']}" if match['team2_rank'] else "нет данных"
                message += BOT_TEXTS['team_rating'].format(team1_rank=team1_rank, team1_name=match['team1_name'], team2_rank=team2_rank, team2_name=match['team2_name'])
            if match_type == 'upcoming' and hasattr(match, 'head_to_head_team1_wins') and hasattr(match, 'head_to_head_team2_wins'):
                if match['head_to_head_team1_wins'] is not None and match['head_to_head_team2_wins'] is not None:
                    message += BOT_TEXTS['h2h_header']
                    message += BOT_TEXTS['h2h_line'].format(team=match['team1_name'], wins=match['head_to_head_team1_wins'])
                    message += BOT_TEXTS['h2h_line'].format(team=match['team2_name'], wins=match['head_to_head_team2_wins'])
            if match_type == 'completed' and match['demo_id']:
                demo_url = f"https://www.hltv.org/download/demo/{match['demo_id']}"
                message += BOT_TEXTS['demo_link'].format(url=demo_url)
            team1_players = [p for p in player_stats if p['team_id'] == match['team1_id']]
            team2_players = [p for p in player_stats if p['team_id'] == match['team2_id']]
            if match_type == 'completed' and (team1_players or team2_players):
                message += BOT_TEXTS['player_stats_header']
                if team1_players:
                    players_block = ""
                    for player in team1_players:
                        nick = player['nickname']
                        if len(nick) > 12:
                            nick = nick[:9] + "..."
                        kd = f"{player['kills'] or 0}-{player['deaths'] or 0}"
                        kd_ratio = f"{player['kd_ratio']:.2f}" if player['kd_ratio'] else "0.00"
                        adr = f"{player['adr']:.1f}" if player['adr'] else "0.0"
                        kast = f"{player['kast']*100:.0f}%" if player['kast'] else "0%"
                        rating = f"{player['rating']:.2f}" if player['rating'] else "0.00"
                        players_block += BOT_TEXTS['player_stats_line'].format(nick=nick.ljust(12), kd=kd.ljust(5), kd_ratio=kd_ratio.ljust(4), adr=adr.ljust(3), kast=kast.ljust(4), rating=rating)
                    message += BOT_TEXTS['player_stats_team'].format(team_name=match['team1_name'], players=players_block)
                if team2_players:
                    players_block = ""
                    for player in team2_players:
                        nick = player['nickname']
                        if len(nick) > 12:
                            nick = nick[:9] + "..."
                        kd = f"{player['kills'] or 0}-{player['deaths'] or 0}"
                        kd_ratio = f"{player['kd_ratio']:.2f}" if player['kd_ratio'] else "0.00"
                        adr = f"{player['adr']:.1f}" if player['adr'] else "0.0"
                        kast = f"{player['kast']*100:.0f}%" if player['kast'] else "0%"
                        rating = f"{player['rating']:.2f}" if player['rating'] else "0.00"
                        players_block += BOT_TEXTS['player_stats_line'].format(nick=nick.ljust(12), kd=kd.ljust(5), kd_ratio=kd_ratio.ljust(4), adr=adr.ljust(3), kast=kast.ljust(4), rating=rating)
                    message += BOT_TEXTS['player_stats_team'].format(team_name=match['team2_name'], players=players_block)
            elif match_type == 'upcoming' and (team1_players or team2_players):
                message += BOT_TEXTS['lineups_header']
                if team1_players:
                    players_block = ""
                    for player in team1_players:
                        players_block += f"• {player['nickname']}\n"
                    message += BOT_TEXTS['lineups_team'].format(team_name=match['team1_name'], players=players_block)
                if team2_players:
                    players_block = ""
                    for player in team2_players:
                        players_block += f"• {player['nickname']}\n"
                    message += BOT_TEXTS['lineups_team'].format(team_name=match['team2_name'], players=players_block)
            else:
                message += BOT_TEXTS['no_lineups']
            if match_type == 'upcoming':
                try:
                    conn2 = sqlite3.connect(self.db_path)
                    conn2.row_factory = sqlite3.Row
                    cursor2 = conn2.cursor()
                    cursor2.execute('SELECT name, lang, url FROM upcoming_match_streamers WHERE match_id = ?', (match_id,))
                    streams = cursor2.fetchall()
                    conn2.close()
                    if streams:
                        message += BOT_TEXTS['where_to_watch']
                        # Сначала ищем cmarty
                        cmarty_streams = []
                        other_streams = []
                        for s in streams:
                            url = s['url'] or ''
                            if 'twitch.tv/cmarty' in url.replace('https://', '').replace('http://', '').replace('www.', ''):
                                cmarty_streams.append(s)
                            else:
                                other_streams.append(s)
                        # Сначала cmarty, потом остальные
                        for s in cmarty_streams + other_streams:
                            lang = f" ({s['lang']})" if s['lang'] else ''
                            message += f"• <a href=\"{s['url']}\">{s['name']}{lang}</a>\n"
                except Exception as e:
                    self.logger.error(BOT_TEXTS['error_streamers'].format(match_id=match_id, error=str(e)))
            reply_markup = self.markup
            if match_type == 'upcoming':
                from telegram import InlineKeyboardMarkup, InlineKeyboardButton
                import urllib.parse
                # Формируем ссылку для Google Calendar
                start_utc = match_datetime.strftime('%Y%m%dT%H%M%SZ')
                end_utc = (match_datetime + timedelta(hours=2)).strftime('%Y%m%dT%H%M%SZ')
                summary = f"Матч {team1_name} vs {team2_name}"
                details = f"Турнир: {event_name}" if event_name else "HLTV.org"
                url = (
                    "https://calendar.google.com/calendar/render?action=TEMPLATE"
                    f"&text={urllib.parse.quote(summary)}"
                    f"&dates={start_utc}/{end_utc}"
                    f"&details={urllib.parse.quote(details)}"
                )
                # Проверяем, есть ли подписка пользователя на этот матч
                subs_data = self.load_subs_json()
                user_id = update.effective_user.id
                match_id_str = str(match_id)
                user_sub = next((s for s in subs_data['upcoming_live'].get(match_id_str, []) if s['id'] == user_id), None)
                calendar_button = InlineKeyboardMarkup([[InlineKeyboardButton("Добавить в Google Календарь", url=url)]])
                if user_sub:
                    sub_buttons = InlineKeyboardMarkup([[InlineKeyboardButton(f"Отписаться от Live ({self._type_to_text(user_sub['type'])})", callback_data=f"unsubscribe_upcoming:{match_id}")]])
                else:
                    sub_buttons = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("Раунды", callback_data=f"subscribe_upcoming_round:{match_id}"),
                            InlineKeyboardButton("Карты", callback_data=f"subscribe_upcoming_map:{match_id}"),
                            InlineKeyboardButton("Результат", callback_data=f"subscribe_upcoming_match:{match_id}")
                        ]
                    ])
                await update.message.reply_text(message, parse_mode="HTML", reply_markup=calendar_button)
                await update.message.reply_text("Подписаться на Live", reply_markup=sub_buttons)
                return
            await update.message.reply_text(message, parse_mode="HTML", reply_markup=reply_markup)
        except Exception as e:
            self.logger.error(BOT_TEXTS['error_getting_matches_period'].format(error=str(e)))
            await update.message.reply_text(BOT_TEXTS['error_getting_match'], reply_markup=self.markup)

    async def find_matches_by_team(self, update: Update, context: ContextTypes.DEFAULT_TYPE, team_name):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(BOT_TEXTS['log']['search_team'].format(user_info=user_info, team_name=team_name))
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('''
                SELECT match_id, datetime, team1_id, team1_name, team1_score, team2_id, team2_name, team2_score, event_name, 'completed' as match_type
                FROM result_match
                WHERE (LOWER(team1_name) = LOWER(?) OR LOWER(team2_name) = LOWER(?))
                ORDER BY datetime DESC
                LIMIT 10
            ''', (team_name, team_name))
            completed_matches = cursor.fetchall()
            cursor.execute('''
                SELECT match_id, datetime, team1_id, team1_name, 0 as team1_score, team2_id, team2_name, 0 as team2_score, event_name, 'upcoming' as status, 'upcoming' as match_type
                FROM upcoming_match
                WHERE (LOWER(team1_name) = LOWER(?) OR LOWER(team2_name) = LOWER(?))
                ORDER BY datetime ASC
                LIMIT 10
            ''', (team_name, team_name))
            upcoming_matches = cursor.fetchall()
            conn.close()
            all_matches = list(upcoming_matches) + list(completed_matches)
            if not all_matches:
                await update.message.reply_text('Ничего не найдено. Нашли баг, вопросы, предложения? Пишите: @TarAn0o', reply_markup=self.markup)
                return
            keyboard = []
            if 'match_mapping' not in context.user_data:
                context.user_data['match_mapping'] = {}
            matches_list = BOT_TEXTS['matches_team_header'].format(team_name=team_name)
            if upcoming_matches:
                matches_list += BOT_TEXTS['matches_team_upcoming']
                for i, match in enumerate(upcoming_matches, 1):
                    team1_name = match['team1_name']
                    team2_name = match['team2_name']
                    match_id = match['match_id']
                    match_date = datetime.fromtimestamp(match['datetime'], tz=self.MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M')
                    matches_list += f"{i}. <b>{match_date}</b>: {team1_name} vs {team2_name}\n"
                    match_text = f"{team1_name} vs {team2_name}"
                    context.user_data['match_mapping'][match_text] = match_id
                    keyboard.append([KeyboardButton(match_text)])
                matches_list += "\n"
            if completed_matches:
                matches_list += BOT_TEXTS['matches_team_completed']
                for i, match in enumerate(completed_matches, 1):
                    team1_name = match['team1_name']
                    team2_name = match['team2_name']
                    team1_score = match['team1_score']
                    team2_score = match['team2_score']
                    match_id = match['match_id']
                    match_date = datetime.fromtimestamp(match['datetime'], tz=self.MOSCOW_TIMEZONE).strftime('%d.%m.%Y')
                    matches_list += f"{i}. <b>{match_date}</b>: {team1_name} {team1_score}:{team2_score} {team2_name}\n"
                    match_text = f"{team1_name} {team1_score}:{team2_score} {team2_name}"
                    context.user_data['match_mapping'][match_text] = match_id
                    keyboard.append([KeyboardButton(match_text)])
            keyboard = [[KeyboardButton(BOT_TEXTS['back'])]] + keyboard
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(matches_list, parse_mode="HTML", reply_markup=reply_markup)
        except Exception as e:
            self.logger.error(f"Ошибка при поиске матчей команды: {str(e)}")
            await update.message.reply_text(BOT_TEXTS['error_search_team'], reply_markup=self.markup)

    def get_matches_by_date(self, date_start, date_end):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('''
                SELECT m.match_id, m.datetime, m.team1_id, m.team1_name, m.team1_score, m.team1_rank,
                       m.team2_id, m.team2_name, m.team2_score, m.team2_rank, m.event_id, m.event_name
                FROM result_match m
                WHERE m.datetime BETWEEN ? AND ?
                ORDER BY m.event_id, m.datetime
            ''', (date_start, date_end))
            matches = cursor.fetchall()
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
            self.logger.error(BOT_TEXTS['error_getting_matches_period'].format(error=str(e)))
            return {}

    def format_matches_message(self, events):
        if not events:
            return BOT_TEXTS['no_matches_period']
        message = ""
        all_matches = []
        for event_data in events.values():
            all_matches.extend(event_data['matches'])
        all_matches.sort(key=lambda m: m['datetime'])
        for match in all_matches:
            team1_name = match['team1_name']
            team2_name = match['team2_name']
            team1_score = match['team1_score']
            team2_score = match['team2_score']
            match_datetime = datetime.fromtimestamp(match['datetime'], tz=self.MOSCOW_TIMEZONE)
            match_time = match_datetime.strftime('%H:%M')
            if team1_score > team2_score:
                team1_name = f"<b>{team1_name}</b>"
            elif team2_score > team1_score:
                team2_name = f"<b>{team2_name}</b>"
            message += f"• <b>{match_time}</b> <code>{team1_name}</code> {team1_score} : {team2_score} <code>{team2_name}</code>\n"
        return message

    async def show_upcoming_matches_for_period(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days=0):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        log_user_action(user.id, "view_upcoming_period", days)
        today = datetime.now(self.MOSCOW_TIMEZONE)
        current_timestamp = today.timestamp()
        if days == 0:
            start_timestamp = current_timestamp
            end_date = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=self.MOSCOW_TIMEZONE)
            end_timestamp = end_date.timestamp()
            period_text = "на сегодня"
            self.logger.info(BOT_TEXTS['log']['upcoming_matches_request'].format(user_info=user_info, days=0))
        elif days == 1:
            tomorrow = today + timedelta(days=1)
            start_date = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0, tzinfo=self.MOSCOW_TIMEZONE)
            end_date = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 23, 59, 59, tzinfo=self.MOSCOW_TIMEZONE)
            start_timestamp = start_date.timestamp()
            end_timestamp = end_date.timestamp()
            period_text = f"на завтра ({start_date.strftime('%d.%m.%Y')})"
            self.logger.info(BOT_TEXTS['log']['upcoming_matches_request'].format(user_info=user_info, days=1))
        else:
            start_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=self.MOSCOW_TIMEZONE)
            end_date = start_date + timedelta(days=days)
            start_timestamp = current_timestamp
            end_timestamp = end_date.timestamp()
            period_text = f"на ближайшие {days} дней"
            self.logger.info(BOT_TEXTS['log']['upcoming_matches_request'].format(user_info=user_info, days=days))
        events = self.get_upcoming_matches_by_date(start_timestamp, end_timestamp)
        match_count = sum(len(event_data['matches']) for event_data in events.values()) if events else 0
        self.logger.info(BOT_TEXTS['log']['found_matches_period'].format(user_info=user_info, count=match_count))
        message = f"📅 <b>Предстоящие матчи {period_text}</b>\n\n"
        message += self.format_upcoming_matches_message(events)
        # Формируем клавиатуру выбора матча
        keyboard = [[KeyboardButton('Назад')]]
        if 'match_mapping' not in context.user_data:
            context.user_data['match_mapping'] = {}
        all_matches = []
        for event_data in events.values():
            all_matches.extend(event_data['matches'])
        all_matches.sort(key=lambda m: m['datetime'])
        for match in all_matches:
            team1_name = match['team1_name']
            team2_name = match['team2_name']
            match_id = match['match_id']
            match_text = f"{team1_name} vs {team2_name}"
            context.user_data['match_mapping'][match_text] = match_id
            keyboard.append([KeyboardButton(match_text)])
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=reply_markup)

    async def send_upcoming_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_info = self._get_safe_user_info(user)
        self.logger.info(BOT_TEXTS['log']['upcoming_matches_request'].format(user_info=user_info, days=0))
        await self.show_upcoming_matches_for_period(update, context, 0)

    def get_upcoming_matches_by_date(self, date_start, date_end):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('''
                SELECT m.match_id, m.datetime, m.team1_id, m.team1_name, m.team1_rank,
                       m.team2_id, m.team2_name, m.team2_rank, m.event_id, m.event_name
                FROM upcoming_match m
                WHERE m.datetime BETWEEN ? AND ? AND m.status = 'upcoming'
                ORDER BY m.event_id, m.datetime
            ''', (date_start, date_end))
            matches = cursor.fetchall()
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
        if not events:
            return BOT_TEXTS['no_matches_upcoming']
        message = ""
        all_matches = []
        for event_data in events.values():
            all_matches.extend(event_data['matches'])
        all_matches.sort(key=lambda m: m['datetime'])
        for match in all_matches:
            team1_name = match['team1_name']
            team2_name = match['team2_name']
            match_datetime = datetime.fromtimestamp(match['datetime'], tz=self.MOSCOW_TIMEZONE)
            match_time = match_datetime.strftime('%H:%M')
            message += f"• <b>{match_time}</b> <code>{team1_name}</code> vs <code>{team2_name}</code>\n"
        return message

    async def show_live_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Универсально получаем user_id для Update и CallbackQuery
        user_id = None
        if hasattr(update, "effective_user") and update.effective_user:
            user_id = update.effective_user.id
        elif hasattr(update, "from_user") and update.from_user:
            user_id = update.from_user.id
        elif hasattr(update, "callback_query") and update.callback_query and update.callback_query.from_user:
            user_id = update.callback_query.from_user.id
        else:
            raise ValueError("Не удалось определить пользователя для show_live_matches")
        subs_data = self.load_subs_json()
        matches = load_json(LIVE_JSON, default=[])
        move_future_subscribers_to_live(matches)
        subs_data = self.load_subs_json()  # обновить после переноса
        live_match_mapping = {}
        upcoming_match_mapping = {}
        keyboard = [[KeyboardButton("Назад")]]  # Кнопка "Назад" теперь первая
        if matches:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            for match in matches:
                match_id = match['match_id']
                t1 = match['team_names'][0] if match['team_names'] else '?'
                t2 = match['team_names'][1] if len(match['team_names']) > 1 else '?'
                match_text = f"{t1} vs {t2}"
                cursor.execute('SELECT team1_name, team2_name FROM upcoming_match WHERE match_id = ?', (match_id,))
                db_match = cursor.fetchone()
                if db_match:
                    live_match_mapping[match_text] = match_id
                    keyboard.append([KeyboardButton(match_text)])
            conn.close()
        # Будущие матчи, на которые подписан пользователь
        user_future_matches = []
        for match_id_str, users in subs_data['upcoming_live'].items():
            if any(s['id'] == user_id for s in users):
                user_future_matches.append(int(match_id_str))
        if user_future_matches:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            q_marks = ','.join(['?'] * len(user_future_matches))
            cursor.execute(f'SELECT match_id, team1_name, team2_name FROM upcoming_match WHERE match_id IN ({q_marks})', tuple(user_future_matches))
            rows = cursor.fetchall()
            for row in rows:
                match_id = row['match_id']
                t1 = row['team1_name']
                t2 = row['team2_name']
                match_text = f"{t1} vs {t2}"
                if match_text not in live_match_mapping and match_text not in upcoming_match_mapping:
                    upcoming_match_mapping[match_text] = match_id
                    keyboard.append([KeyboardButton(match_text)])
            conn.close()
        context.user_data['live_match_mapping'] = live_match_mapping
        context.user_data['upcoming_match_mapping'] = upcoming_match_mapping
        reply_markup_kb = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        # Новое: сообщение 'Live матчи' перед списком live-матчей
        if matches:
            await update.message.reply_text("Live матчи.\nВыберите тип уведомления, чтоб получать уведомления о матче.", reply_markup=reply_markup_kb)
        else:
            await update.message.reply_text("В текущий момент live матчей нет.", reply_markup=reply_markup_kb)
        # --- Inline-кнопки для live-матчей ---
        live_message = BOT_TEXTS['live_matches_header']
        for match in matches:
            t1 = match['team_names'][0] if match['team_names'] else '?'
            t2 = match['team_names'][1] if len(match['team_names']) > 1 else '?'
            score1 = match['current_map_scores'][0] if match['current_map_scores'] else '?'
            score2 = match['current_map_scores'][1] if len(match['current_map_scores']) > 1 else '?'
            maps1 = match['maps_won'][0] if match['maps_won'] else '0'
            maps2 = match['maps_won'][1] if len(match['maps_won']) > 1 else '0'
            match_id = match['match_id']

            # Новый формат: счет + AI-строки
            # Получаем данные из predict и upcoming_match
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('''
                SELECT u.team1_id, u.team2_id, u.team1_rank, u.team2_rank, u.team1_name, u.team2_name, 
                       p.team1_score, p.team2_score
                FROM upcoming_match u
                LEFT JOIN predict p ON u.match_id = p.match_id
                WHERE u.match_id = ?
            ''', (match_id,))
            row = cursor.fetchone()
            # Для объема данных и стабильности
            result_df = cursor.execute('SELECT team1_id, team2_id FROM result_match').fetchall()
            def count_matches(team_id):
                if team_id is None:
                    return 0
                return sum(1 for r in result_df if r['team1_id'] == team_id or r['team2_id'] == team_id)
            def data_emoji(val):
                if val is None:
                    return '⚪'
                val = int(val)
                if val < 10:
                    return '🔴'
                elif val < 20:
                    return '🟡'
                else:
                    return '🟢'
            def get_team_stability(team_id, n_matches=20):
                q = '''SELECT team1_id, team2_id, team1_score, team2_score, datetime FROM result_match WHERE (team1_id = ? OR team2_id = ?) AND team1_score IS NOT NULL AND team2_score IS NOT NULL ORDER BY datetime DESC LIMIT ?'''
                df = cursor.execute(q, (team_id, team_id, n_matches)).fetchall()
                if not df:
                    return None
                diffs = []
                for r in df:
                    if r['team1_id'] == team_id:
                        diffs.append(r['team1_score'] - r['team2_score'])
                    else:
                        diffs.append(r['team2_score'] - r['team1_score'])
                if len(diffs) > 1:
                    import numpy as np
                    return float(np.std(diffs))
                else:
                    return None
            def stability_emoji(val):
                if val is None:
                    return '⬜'
                if val <= 2:
                    return '🟩'
                elif val <= 3:
                    return '🟨'
                else:
                    return '🟥'
            # Данные по командам
            t1_id = row['team1_id'] if row else None
            t2_id = row['team2_id'] if row else None
            t1_rank = f"#{row['team1_rank']}" if row and row['team1_rank'] else "—"
            t2_rank = f"#{row['team2_rank']}" if row and row['team2_rank'] else "—"
            p1 = row['team1_score'] if row else None
            p2 = row['team2_score'] if row else None
            p1_pct = f"{round(p1*100):.0f}%" if p1 is not None else "-"
            p2_pct = f"{round(p2*100):.0f}%" if p2 is not None else "-"
            t1_matches = count_matches(t1_id)
            t2_matches = count_matches(t2_id)
            t1_stab = get_team_stability(t1_id)
            t2_stab = get_team_stability(t2_id)
            t1_data_emoji = data_emoji(t1_matches)
            t2_data_emoji = data_emoji(t2_matches)
            t1_stab_emoji = stability_emoji(t1_stab)
            t2_stab_emoji = stability_emoji(t2_stab)
            # Формируем сообщение
            match_text = f"<b>{t1}</b> ({maps1}) {score1} - {score2} ({maps2}) <b>{t2}</b>\n"
            match_text += f"{t1_data_emoji}{t1_stab_emoji} {t1_rank} {t1} - вероятность победы = {p1_pct}\n"
            match_text += f"{t2_data_emoji}{t2_stab_emoji} {t2_rank} {t2} - вероятность победы = {p2_pct}"
            conn.close()
            user_sub = next((s for s in subs_data['live'].get(str(match_id), []) if s['id'] == user_id), None)
            if user_sub:
                btn_text = f"Отписаться от {t1} vs {t2} ({self._type_to_text(user_sub['type'])})"
                callback = f"unsubscribe_live:{match_id}"
                inline_markup = InlineKeyboardMarkup([[InlineKeyboardButton(btn_text, callback_data=callback)]])
            else:
                inline_markup = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("Раунды", callback_data=f"subscribe_live_round:{match_id}"),
                        InlineKeyboardButton("Карты", callback_data=f"subscribe_live_map:{match_id}"),
                        InlineKeyboardButton("Результат", callback_data=f"subscribe_live_match:{match_id}")
                    ]
                ])
            await update.message.reply_text(match_text, reply_markup=inline_markup, parse_mode="HTML", disable_web_page_preview=True)
        # --- Inline-кнопки для будущих матчей ---
        if user_future_matches:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            q_marks = ','.join(['?'] * len(user_future_matches))
            cursor.execute(f'SELECT match_id, datetime, team1_name, team2_name FROM upcoming_match WHERE match_id IN ({q_marks})', tuple(user_future_matches))
            rows = cursor.fetchall()
            conn.close()
            if rows:
                await update.message.reply_text("Ваши подписки на будущие Live-матчи.\nВам будут приходить уведомления кода матч начнется.", reply_markup=reply_markup_kb)
                msg = ''
                future_inline_keyboard = []
                for row in rows:
                    dt = datetime.fromtimestamp(row['datetime'], tz=self.MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M')
                    t1 = row['team1_name']
                    t2 = row['team2_name']
                    match_id = row['match_id']
                    user_sub = next((s for s in subs_data['upcoming_live'].get(str(match_id), []) if s['id'] == user_id), None)
                    msg += f"<b>{dt}</b>: <code>{t1}</code> vs <code>{t2}</code>\n"
                    if user_sub:
                        btn_text = f"Отписаться от Live: {t1} vs {t2} ({self._type_to_text(user_sub['type'])})"
                        callback = f"unsubscribe_upcoming:{match_id}"
                        future_inline_keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback)])
                    else:
                        future_inline_keyboard.append([
                            InlineKeyboardButton("Раунды", callback_data=f"subscribe_upcoming_round:{match_id}"),
                            InlineKeyboardButton("Карты", callback_data=f"subscribe_upcoming_map:{match_id}"),
                            InlineKeyboardButton("Результат", callback_data=f"subscribe_upcoming_match:{match_id}")
                        ])
                future_inline_markup = InlineKeyboardMarkup(future_inline_keyboard)
                await update.message.reply_text(msg, parse_mode="HTML", reply_markup=future_inline_markup)

    def _type_to_text(self, sub_type):
        return {"round": "Раунды", "map": "Карты", "match": "Результат"}.get(sub_type, sub_type)

    async def handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = query.data
        user = query.from_user
        user_id = user.id
        subs_data = self.load_subs_json()
        if data.startswith("subscribe_live_"):
            sub_type, match_id = data.split(":")[0].split("_")[-1], int(data.split(":")[1])
            # Получаем информацию о матче
            matches = load_json(LIVE_JSON, default=[])
            match = next((m for m in matches if m['match_id'] == match_id), None)
            if match:
                t1 = match['team_names'][0] if match['team_names'] else '?'
                t2 = match['team_names'][1] if len(match['team_names']) > 1 else '?'
                subscribe_user(match_id, user_id, sub_type, section="live")
                log_user_action(user_id, "subscribe_live", match_id)
                await query.answer("Подписка оформлена!")
                await query.message.reply_text(f"Вы успешно подписались на live-матч {t1} vs {t2}!")
            return
        elif data.startswith("unsubscribe_live:"):
            match_id = int(data.split(":")[1])
            # Получаем информацию о матче
            matches = load_json(LIVE_JSON, default=[])
            match = next((m for m in matches if m['match_id'] == match_id), None)
            if match:
                t1 = match['team_names'][0] if match['team_names'] else '?'
                t2 = match['team_names'][1] if len(match['team_names']) > 1 else '?'
                unsubscribe_user(match_id, user_id, section="live")
                log_user_action(user_id, "unsubscribe_live", match_id)
                await query.answer("Вы отписались от матча!")
                await query.message.reply_text(f"Вы успешно отписались от live-матча {t1} vs {t2}!")
            return
        elif data.startswith("subscribe_upcoming_"):
            sub_type, match_id = data.split(":")[0].split("_")[-1], int(data.split(":")[1])
            # Получаем информацию о матче из базы данных
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT team1_name, team2_name FROM upcoming_match WHERE match_id = ?', (match_id,))
            match = cursor.fetchone()
            conn.close()
            if match:
                t1 = match['team1_name']
                t2 = match['team2_name']
                subscribe_user(match_id, user_id, sub_type, section="upcoming_live")
                log_user_action(user_id, "subscribe_upcoming", match_id)
                await query.answer("Подписка оформлена!")
                await query.message.reply_text(f"Вы успешно подписались на будущий live-матч {t1} vs {t2}!")
            return
        elif data.startswith("unsubscribe_upcoming:"):
            match_id = int(data.split(":")[1])
            # Получаем информацию о матче из базы данных
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT team1_name, team2_name FROM upcoming_match WHERE match_id = ?', (match_id,))
            match = cursor.fetchone()
            conn.close()
            if match:
                t1 = match['team1_name']
                t2 = match['team2_name']
                unsubscribe_user(match_id, user_id, section="upcoming_live")
                log_user_action(user_id, "unsubscribe_upcoming", match_id)
                await query.answer("Вы отписались от будущего матча!")
                await query.message.reply_text(f"Вы успешно отписались от будущего live-матча {t1} vs {t2}!")
            return

    def load_subs_json(self):
        subs_path = os.path.join(os.path.dirname(__file__), '../../../storage/json/live/live_subscribers.json')
        data = load_json(subs_path, default=None)
        if not data or not isinstance(data, dict):
            if data is None or data == {}:
                return {"live": {}, "upcoming_live": {}}
            return {"live": {}, "upcoming_live": data}
        if "live" not in data:
            data["live"] = {}
        if "upcoming_live" not in data:
            data["upcoming_live"] = {}
        return data

    def save_subs_json(self, data):
        subs_path = os.path.join(os.path.dirname(__file__), '../../../storage/json/live/live_subscribers.json')
        save_json(subs_path, data)

    async def show_live_match_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE, match_id):
        matches = load_json(LIVE_JSON, default=[])
        match = next((m for m in matches if m['match_id'] == match_id), None)
        if not match:
            await update.message.reply_text(BOT_TEXTS['match_not_found'].format(match_id=match_id), reply_markup=self.markup)
            return
        t1 = match['team_names'][0] if match['team_names'] else '?'
        t2 = match['team_names'][1] if len(match['team_names']) > 1 else '?'
        score1 = match['current_map_scores'][0] if match['current_map_scores'] else '?'
        score2 = match['current_map_scores'][1] if len(match['current_map_scores']) > 1 else '?'
        maps1 = match['maps_won'][0] if match['maps_won'] else '0'
        maps2 = match['maps_won'][1] if len(match['maps_won']) > 1 else '0'
        match_url = match.get('match_url')
        link = f' <a href="{match_url}">🌐</a>' if match_url else ''
        message = f"<b>{t1}</b> ({maps1}) {score1} - {score2} ({maps2}) <b>{t2}</b>\n"
        await update.message.reply_text(message, parse_mode="HTML", reply_markup=self.markup, disable_web_page_preview=True)

    async def show_ai_predictions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает AI прогнозы на будущие матчи с вероятностями, стабильностью и объемом данных (только 10 ближайших, начиная с -1 часа от текущего времени)"""
        import sqlite3
        from datetime import datetime, timedelta
        db_path = self.db_path
        MOSCOW_TIMEZONE = self.MOSCOW_TIMEZONE
        # Исправлено: вычисляем min_time в UTC, чтобы фильтрация работала корректно по московскому времени
        now = datetime.now(MOSCOW_TIMEZONE)
        min_time = int((now - timedelta(hours=2)).timestamp())
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # Получаем прогнозы для будущих матчей начиная с -1 часа
        query = '''
        SELECT
            p.match_id,
            u.team1_id,
            u.team2_id,
            u.team1_name,
            u.team1_rank,
            u.team2_name,
            u.team2_rank,
            u.datetime,
            p.team1_score,
            p.team2_score
        FROM predict p
        LEFT JOIN upcoming_match u ON p.match_id = u.match_id
        WHERE u.match_id IS NOT NULL AND u.datetime >= ?
        ORDER BY u.datetime ASC
        LIMIT 20
        '''
        matches = cursor.execute(query, (min_time,)).fetchall()
        # Для расчета объема данных
        result_df = cursor.execute('SELECT team1_id, team2_id FROM result_match').fetchall()
        # Вспомогательные функции
        def count_matches(team_id):
            if team_id is None:
                return 0
            return sum(1 for row in result_df if row['team1_id'] == team_id or row['team2_id'] == team_id)
        def data_level(val):
            if val is None:
                return 'нет данных'
            val = int(val)
            if val < 10:
                return 'мало'
            elif val < 20:
                return 'средне'
            else:
                return 'много'
        def get_team_stability(team_id, n_matches=20):
            q = '''SELECT team1_id, team2_id, team1_score, team2_score, datetime FROM result_match WHERE (team1_id = ? OR team2_id = ?) AND team1_score IS NOT NULL AND team2_score IS NOT NULL ORDER BY datetime DESC LIMIT ?'''
            df = cursor.execute(q, (team_id, team_id, n_matches)).fetchall()
            if not df:
                return None
            diffs = []
            for row in df:
                if row['team1_id'] == team_id:
                    diffs.append(row['team1_score'] - row['team2_score'])
                else:
                    diffs.append(row['team2_score'] - row['team1_score'])
            if len(diffs) > 1:
                import numpy as np
                return float(np.std(diffs))
            else:
                return None
        def stability_level(val):
            if val is None:
                return 'нет данных'
            if val <= 1:
                return 'очень стабильная'
            elif val <= 2:
                return 'стабильная'
            elif val <= 3:
                return 'средняя'
            else:
                return 'нестабильная'
        # Формируем сообщение
        if not matches:
            await update.message.reply_text("Нет данных о прогнозах на будущие матчи.", reply_markup=self.markup)
            conn.close()
            return
        # Оставляем только 30 ближайших
        matches = matches[:30]
        # Легенда
        legend = (
            "AI прогнозы на ближайшие матчи:\n"
            "Время московское (+3)\n\n"
            "Данных о команде в базе:\n"
            "🟢 много данных  🟡 средне  🔴 мало  \n\n"
            "Стабильность игр команды:\n"
            "🟩 стабильная  🟨 средняя  🟥 нестабильная  ⬜ нет данных\n"
            "\n"  # отступ перед таблицей
        )
        msg = legend
        # Функции для эмодзи
        def data_emoji(val):
            if val is None:
                return '⚪'  # нет данных
            val = int(val)
            if val < 10:
                return '🔴'
            elif val < 20:
                return '🟡'
            else:
                return '🟢'
        def stability_emoji(val):
            if val is None:
                return '⬜'  # нет данных
            if val <= 2:
                return '🟩'
            elif val <= 3:
                return '🟨'
            else:
                return '🟥'
        # Для выравнивания
        def pad(s, n):
            return s + ' ' * (n - len(s)) if len(s) < n else s
        max_t1 = max(len(str(match['team1_name'])) for match in matches) if matches else 8
        max_t2 = max(len(str(match['team2_name'])) for match in matches) if matches else 8
        for match in matches:
            t1 = str(match['team1_name'])
            t2 = str(match['team2_name'])
            p1 = match['team1_score']
            p2 = match['team2_score']
            p1_pct = f"{round(p1*100):.0f}%" if p1 is not None else "-"
            p2_pct = f"{round(p2*100):.0f}%" if p2 is not None else "-"
            t1_matches = count_matches(match['team1_id'])
            t2_matches = count_matches(match['team2_id'])
            t1_stab = get_team_stability(match['team1_id'])
            t2_stab = get_team_stability(match['team2_id'])
            # Эмодзи
            t1_data_emoji = data_emoji(t1_matches)
            t2_data_emoji = data_emoji(t2_matches)
            t1_stab_emoji = stability_emoji(t1_stab)
            t2_stab_emoji = stability_emoji(t2_stab)
            # Ранги
            t1_rank = f"#{match['team1_rank']}" if match['team1_rank'] else "—"
            t2_rank = f"#{match['team2_rank']}" if match['team2_rank'] else "—"
            # Дата и время
            match_dt = datetime.fromtimestamp(match['datetime'], tz=MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M') if match['datetime'] else ''
            # Новый формат: разделитель, дата-время, команда1, команда2, разделитель
            msg += f"{'-'*32}\n"
            msg += f"{match_dt}\n"
            msg += f"{t1_data_emoji}{t1_stab_emoji} {t1_rank} {t1} {p1_pct}\n"
            msg += f"{t2_data_emoji}{t2_stab_emoji} {t2_rank} {t2} {p2_pct}\n"
        await update.message.reply_text(f"<pre>{msg}</pre>", parse_mode="HTML", reply_markup=self.markup)
        conn.close()
        # Дисклеймер
        disclaimer = (
            "<b>Данные не предназначены для ставок!</b>\n"
            "Используйте только для ознакомительных целей.\n"
            "Администрация ответственности за точность данных и результаты не несет."
        )
        await update.message.reply_text(disclaimer, parse_mode="HTML", reply_markup=self.markup)

if __name__ == "__main__":
    from src.bots.config import load_config
    config = load_config("user")
    token = config["token"]
    db_path = config["db_path"]
    log_file = config.get("log_file", "user_bot.log")
    bot = HLTVUserBot(token, db_path, log_file, config_name='user')
    bot.run()