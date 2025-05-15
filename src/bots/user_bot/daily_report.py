#!/usr/bin/env python
"""
Скрипт для автоматической ежедневной отправки статистики матчей
"""

import os
import sys
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
import telegram
import asyncio

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

# Определяем московское время (UTC+3)
MOSCOW_TIMEZONE = timezone(timedelta(hours=3))

class DailyReportSender:
    """
    Класс для отправки ежедневных отчетов
    """
    def __init__(self, token, hltv_db_path):
        """
        Инициализация отправщика отчетов
        
        Args:
            token (str): Токен телеграм-бота
            hltv_db_path (str): Путь к базе данных с матчами
        """
        self.token = token
        self.hltv_db_path = hltv_db_path
        self.bot = telegram.Bot(token=self.token)
    
    def get_matches_by_period(self, days=1):
        """
        Получает матчи за указанный период дней
        
        Args:
            days (int): Количество дней для выборки (по умолчанию 1 день назад)
            
        Returns:
            dict: Словарь с событиями и матчами
        """
        try:
            today = datetime.now(MOSCOW_TIMEZONE)
            
            # Вычисляем начало и конец периода
            end_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=MOSCOW_TIMEZONE) - timedelta(days=1)
            end_timestamp = end_date.timestamp() + 86399  # Конец дня (23:59:59)
            start_date = end_date - timedelta(days=days-1)
            start_timestamp = start_date.timestamp()
            
            conn = sqlite3.connect(self.hltv_db_path)
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
            ''', (start_timestamp, end_timestamp))
            
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
            return events, start_date, end_date
            
        except Exception as e:
            logger.error(f"Ошибка при получении матчей за период: {str(e)}")
            return {}, None, None
    
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
                message += f"• <code>{team1_name}</code> {team1_score} : {team2_score} <code>{team2_name}</code> <code>({match_id})</code>\n"
            
            # Добавляем разделитель между событиями
            message += "\n"
        
        message += "Скопируйте и отправьте <code>(ID)</code>, чтобы увидеть подробную статистику игроков.\n"
        
        return message
    
    async def send_daily_report(self, chat_id=None):
        """
        Отправляет отчет администратору или по указанному chat_id
        
        Args:
            chat_id: ID чата для отправки (если None, используются admin_chat_ids из конфигурации)
        """
        # Получаем отчет за один день (вчера)
        events, start_date, end_date = self.get_matches_by_period(1)
        
        if not events:
            logger.info("Нет матчей за вчерашний день для отправки")
            return
        
        # Форматируем заголовок сообщения
        yesterday = datetime.now(MOSCOW_TIMEZONE) - timedelta(days=1)
        period_text = f"за {yesterday.strftime('%d.%m.%Y')}"
        
        # Форматируем сообщение
        message = f"📊 <b>Результаты матчей {period_text}</b>\n\n"
        message += self.format_matches_message(events)
        
        # Если не указан chat_id, используем admin_chat_ids из конфигурации
        recipients = []
        if chat_id:
            recipients = [chat_id]
        elif 'admin_chat_ids' in config and config['admin_chat_ids']:
            recipients = config['admin_chat_ids']
        
        if not recipients:
            logger.warning("Не указаны получатели для отправки отчета")
            return
        
        # Отправляем отчет каждому получателю
        for chat_id in recipients:
            try:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode="HTML"
                )
                logger.info(f"Отчет успешно отправлен получателю {chat_id}")
            except Exception as e:
                logger.error(f"Ошибка при отправке отчета получателю {chat_id}: {str(e)}")
        
        logger.info(f"Отправка отчетов завершена.")


async def main():
    """
    Основная функция для отправки ежедневного отчета
    """
    try:
        logger.info("Запуск отправки ежедневного отчета...")
        sender = DailyReportSender(TOKEN, DB_PATH)
        
        # Если в конфигурации есть админские чаты, отправляем им отчет
        if 'admin_chat_ids' in config and config['admin_chat_ids']:
            for admin_id in config['admin_chat_ids']:
                await sender.send_daily_report(admin_id)
                logger.info(f"Отчет отправлен администратору {admin_id}")
        else:
            logger.warning("Не указаны admin_chat_ids в конфигурации, отчет не отправлен")
        
        logger.info("Отправка ежедневного отчета завершена.")
    except Exception as e:
        logger.error(f"Ошибка при отправке ежедневного отчета: {str(e)}")
    finally:
        # Закрываем ресурсы
        if hasattr(sender, 'bot'):
            await sender.bot.close()

if __name__ == "__main__":
    asyncio.run(main()) 