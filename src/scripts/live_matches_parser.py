#!/usr/bin/env python
"""
Парсер live-матчей HLTV.org для хранения в локальном json и рассылки уведомлений подписчикам.
"""
import os
import sys
import time
import json
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import threading
from src.bots.notify import send_telegram_message
from src.parser.matches import MatchesParser
from src.parser.simple_html import SimpleHTMLParser

# Отключаем лишние логи
logging.getLogger("tensorflow").setLevel(logging.ERROR)
logging.getLogger("tensorflow.lite").setLevel(logging.ERROR)
logging.getLogger("tensorflow.lite.python").setLevel(logging.ERROR)
logging.getLogger("tensorflow.lite.python.lite").setLevel(logging.ERROR)

LIVE_JSON = "storage/json/live/live_matches.json"
PREV_JSON = "storage/json/live/live_matches_prev.json"
SUBS_JSON = "storage/json/live/live_subscribers.json"
HTML_DIR = "storage/html/live"
HTML_PATH = os.path.join(HTML_DIR, "live_matches.html")
FUTURE_SUBS_JSON = "storage/json/live/live_subscribers.json"

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/live_matches_parser.log")]
)
logger = logging.getLogger("live_parser")

# Отключаем лишние логи webdriver-manager
logging.getLogger("WDM").setLevel(logging.WARNING)
logging.getLogger("webdriver_manager").setLevel(logging.WARNING)

# --- Вспомогательные функции ---
def load_json(path, default=None):
    if not os.path.exists(path):
        return default if default is not None else []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_all_subscribed_match_ids():
    subs = load_json(SUBS_JSON, default={})
    return set(map(int, subs.keys()))

def get_last_match_state(match_id):
    prev = load_json(PREV_JSON, default=[])
    for m in prev:
        if m['match_id'] == match_id:
            return m
    return None

# --- Парсинг HLTV ---
def parse_live_matches(html):
    soup = BeautifulSoup(html, 'html.parser')
    live_matches = soup.find('div', class_='liveMatches')
    if not live_matches:
        return []
    matches = []
    for match_wrapper in live_matches.find_all('div', class_='match-wrapper live-match-container'):
        try:
            match_id = int(match_wrapper['data-match-id'])
            event_block = match_wrapper.find('div', class_='match-event text-ellipsis')
            event_name = event_block.find('div', class_='text-ellipsis').text.strip() if event_block else ''
            bo_type = ''
            for m in match_wrapper.find_all('div', class_='match-meta'):
                if 'bo' in m.text:
                    bo_type = m.text.strip()
                    break
            team_names = [div.text.strip() for div in match_wrapper.find_all('div', class_='match-teamname')]
            # Счёт на карте
            current_map_scores = [span.text.strip() for span in match_wrapper.find_all('span', class_='current-map-score')]
            # Счёт по картам (число в скобках)
            maps_won = [span.text.strip() for span in match_wrapper.find_all('span', attrs={'data-livescore-maps-won-for': True})]
            # Ссылка на матч
            match_a = match_wrapper.find('div', class_='match').find('a', href=True)
            match_url = None
            if match_a and match_a['href']:
                match_url = f'https://www.hltv.org{match_a["href"]}'
            matches.append({
                'match_id': match_id,
                'event_name': event_name,
                'bo_type': bo_type,
                'team_names': team_names,
                'current_map_scores': current_map_scores,
                'maps_won': maps_won,
                'match_url': match_url
            })
        except Exception as e:
            logger.warning(f"Ошибка парсинга live-матча: {e}")
    return matches

def format_score(match):
    # Новый шаблон: Команда1 (карты) счёт - счёт (карты) Команда2
    t1 = match['team_names'][0] if match['team_names'] else '?'
    t2 = match['team_names'][1] if len(match['team_names']) > 1 else '?'
    score1 = match['current_map_scores'][0] if match['current_map_scores'] else '?'
    score2 = match['current_map_scores'][1] if len(match['current_map_scores']) > 1 else '?'
    maps1 = match['maps_won'][0] if match['maps_won'] else '0'
    maps2 = match['maps_won'][1] if len(match['maps_won']) > 1 else '0'
    return f"{t1} ({maps1}) {score1} - {score2} ({maps2}) {t2}"

def get_winner(match):
    bo = match.get('bo_type', '').lower()
    if not bo.startswith('bo'):
        return None
    try:
        win_maps = int(bo[2:])
    except Exception:
        return None
    maps1 = int(match['maps_won'][0]) if match['maps_won'] and match['maps_won'][0].isdigit() else 0
    maps2 = int(match['maps_won'][1]) if len(match['maps_won']) > 1 and match['maps_won'][1].isdigit() else 0
    if maps1 == win_maps:
        return match['team_names'][0]
    elif maps2 == win_maps:
        return match['team_names'][1]
    return None

# --- Вспомогательные функции для новой структуры ---
def migrate_subs_structure(data):
    # Миграция старой структуры: {match_id: [user_id, ...]} -> {match_id: [{id, type}], ...}
    def migrate_list(lst):
        if lst and isinstance(lst[0], dict):
            return lst  # уже новая структура
        return [{"id": uid, "type": "round"} for uid in lst]
    for section in ("live", "upcoming_live"):
        if section in data:
            for match_id in list(data[section].keys()):
                data[section][match_id] = migrate_list(data[section][match_id])
    return data

def load_subs_json():
    data = load_json(FUTURE_SUBS_JSON, default=None)
    if not data or not isinstance(data, dict):
        if data is None or data == {}:
            return {"live": {}, "upcoming_live": {}}
        return {"live": {}, "upcoming_live": data}
    if "live" not in data:
        data["live"] = {}
    if "upcoming_live" not in data:
        data["upcoming_live"] = {}
    data = migrate_subs_structure(data)
    return data

def save_subs_json(data):
    save_json(FUTURE_SUBS_JSON, data)

# --- Подписка/отписка с типом ---
def subscribe_user(match_id, user_id, sub_type, section="live"):
    data = load_subs_json()
    match_id = str(match_id)
    if match_id not in data[section]:
        data[section][match_id] = []
    # Удаляем старую подписку пользователя на этот матч (если есть)
    data[section][match_id] = [s for s in data[section][match_id] if s["id"] != user_id]
    data[section][match_id].append({"id": user_id, "type": sub_type})
    save_subs_json(data)

def unsubscribe_user(match_id, user_id, section="live"):
    data = load_subs_json()
    match_id = str(match_id)
    if match_id in data[section]:
        data[section][match_id] = [s for s in data[section][match_id] if s["id"] != user_id]
        if not data[section][match_id]:
            data[section].pop(match_id)
        save_subs_json(data)

# --- Фильтрация подписчиков по типу ---
def get_subscribers(match_id, sub_type, section="live"):
    data = load_subs_json()
    match_id = str(match_id)
    return [s["id"] for s in data[section].get(match_id, []) if s["type"] == sub_type]

# --- notify_live_changes с учётом типа подписки ---
def notify_live_changes():
    old = load_json(PREV_JSON, default=[])
    new = load_json(LIVE_JSON, default=[])
    subs = load_subs_json()
    old_dict = {m['match_id']: m for m in old}
    new_dict = {m['match_id']: m for m in new}
    
    # Для каждого матча и типа подписки
    for match_id, match in new_dict.items():
        old_match = old_dict.get(match_id)
        # Раунды: любое изменение счёта
        if old_match and match['current_map_scores'] != old_match['current_map_scores']:
            msg = format_score(match)
            for user_id in get_subscribers(match_id, "round"):
                send_telegram_message(user_id, msg)
        # Карты: только изменение maps_won
        if old_match and match['maps_won'] != old_match['maps_won']:
            msg = f"Закончилась карта!\n{format_score(match)}"
            for user_id in get_subscribers(match_id, "map"):
                send_telegram_message(user_id, msg)
        # Победитель: только при определении победителя
        winner = get_winner(match)
        if winner and (not old_match or get_winner(old_match) != winner):
            msg = f"Победа: {winner} 🏆\n{format_score(match)}"
            for user_id in get_subscribers(match_id, "match"):
                send_telegram_message(user_id, msg)
    
    # Завершение матча: отписка всех
    finished = set(old_dict) - set(new_dict)
    for match_id in finished:
        last_state = old_dict[match_id]
        logger.info(f"Матч {match_id} завершён. Отправка уведомлений...")
        
        # Формируем сообщение о завершении
        t1 = last_state['team_names'][0] if last_state['team_names'] else '?'
        t2 = last_state['team_names'][1] if len(last_state['team_names']) > 1 else '?'
        maps1 = last_state['maps_won'][0] if last_state['maps_won'] else '0'
        maps2 = last_state['maps_won'][1] if len(last_state['maps_won']) > 1 else '0'
        event_name = last_state.get('event_name', '')
        bo_type = last_state.get('bo_type', '')
        
        # Формируем сообщение в нужном формате
        msg = "Матч завершен.\n"
        if bo_type and event_name:
            msg += f"{bo_type} - {event_name}\n"
        msg += f"{t1} ({maps1}) {last_state['current_map_scores'][0]} - {last_state['current_map_scores'][1]} ({maps2}) {t2}"
        
        # Отправляем уведомления для всех секций
        for section in ("live", "upcoming_live"):
            if str(match_id) in subs[section]:
                for sub in subs[section][str(match_id)]:
                    try:
                        # Для подписчиков типа "match" отправляем сообщение о завершении
                        if sub["type"] == "match":
                            send_telegram_message(sub["id"], msg)
                            logger.info(f"Уведомление о завершении матча {match_id} отправлено пользователю {sub['id']}")
                        else:
                            # Для остальных отправляем краткое сообщение
                            short_msg = f"Матч завершён. Итог:\n{format_score(last_state)}"
                            send_telegram_message(sub["id"], short_msg)
                            logger.info(f"Краткое уведомление о завершении матча {match_id} отправлено пользователю {sub['id']}")
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления о завершении матча {match_id} пользователю {sub['id']}: {str(e)}")
                # Удаляем подписку после отправки
                subs[section].pop(str(match_id), None)
    
    save_subs_json(subs)
    save_json(PREV_JSON, new)

subscriber_event = threading.Event()

def handle_new_subscription(match_id, user_id):
    """Добавить подписку и сразу отправить результат, если матч уже не live."""
    subs = load_json(SUBS_JSON, default={})
    match_id_str = str(match_id)
    if match_id_str not in subs:
        subs[match_id_str] = []
    if user_id not in subs[match_id_str]:
        subs[match_id_str].append(user_id)
        # Сигнализируем основному циклу о появлении подписчика
        subscriber_event.set()
    save_json(SUBS_JSON, subs)
    # Проверяем, есть ли матч в live
    live = load_json(LIVE_JSON, default=[])
    if not any(m['match_id'] == match_id for m in live):
        # Проверяем, есть ли он в prev (только что закончился)
        last = get_last_match_state(match_id)
        if last:
            send_telegram_message(user_id, f"Матч завершён. Итог: {format_score(last)}")
        else:
            send_telegram_message(user_id, "К сожалению матч уже закончился или недоступен")
        # Удаляем подписку
        subs = load_json(SUBS_JSON, default={})
        if match_id_str in subs and user_id in subs[match_id_str]:
            subs[match_id_str].remove(user_id)
            if not subs[match_id_str]:
                subs.pop(match_id_str)
            save_json(SUBS_JSON, subs)

def download_live_page():
    os.makedirs(HTML_DIR, exist_ok=True)
    parser = SimpleHTMLParser()
    try:
        logger.info("Download html")
        html = parser.get_html("https://www.hltv.org/matches")
        with open(HTML_PATH, "w", encoding="utf-8") as f:
            f.write(html)
        return HTML_PATH
    finally:
        pass  # SimpleHTMLParser сам закрывает драйвер

# --- Перенос отложенных подписчиков ---
def move_future_subscribers_to_live(live_matches):
    """
    Переносит подписчиков на будущие матчи в live-подписчики, если матч стал live.
    Дополнительно отправляет уведомление пользователю о начале матча.
    """
    data = load_subs_json()
    changed = False
    live_ids = {str(m['match_id']) for m in live_matches}
    for match_id in list(data["upcoming_live"].keys()):
        if match_id in live_ids:
            users = data["upcoming_live"].pop(match_id)
            if match_id not in data["live"]:
                data["live"][match_id] = []
            # --- Новый код: отправка уведомления ---
            match = next((m for m in live_matches if str(m['match_id']) == match_id), None)
            if match:
                t1 = match['team_names'][0] if match['team_names'] else '?'
                t2 = match['team_names'][1] if len(match['team_names']) > 1 else '?'
                for user_id in users:
                    send_telegram_message(
                        user_id,
                        f"Ваш матч {t1} vs {t2}, на который вы подписались — начался. Вам будут приходить сообщения о результате."
                    )
            # --- Конец нового кода ---
            for user_id in users:
                if user_id not in data["live"][match_id]:
                    data["live"][match_id].append(user_id)
                    changed = True
            changed = True
    if changed:
        save_subs_json(data)

# --- Очистка подписок на завершённые live-матчи ---
def clean_dead_live_subscriptions():
    subs = load_subs_json()
    live = subs.get("live", {})
    matches = load_json(LIVE_JSON, default=[])
    live_ids = {str(m['match_id']) for m in matches}
    to_remove = [mid for mid in live if mid not in live_ids]
    for mid in to_remove:
        live.pop(mid, None)
    save_subs_json(subs)

# --- Основной цикл ---
def main_loop():
    while True:
        html_path = download_live_page()
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        new_matches = parse_live_matches(html)
        
        # Загружаем текущие матчи
        current_matches = load_json(LIVE_JSON, default=[])
        current_dict = {m['match_id']: m for m in current_matches}
        
        # Проверяем значения перед обновлением
        for match in new_matches:
            if match['match_id'] in current_dict:
                # Проверяем новые значения на пустые скобки
                current_map_scores = match['current_map_scores']
                maps_won = match['maps_won']
                if not (len(current_map_scores) >= 2 and len(maps_won) >= 2 and 
                       all(current_map_scores) and all(maps_won)):
                    # Если новые значения некорректны, оставляем старые
                    match['current_map_scores'] = current_dict[match['match_id']]['current_map_scores']
                    match['maps_won'] = current_dict[match['match_id']]['maps_won']
        
        save_json(LIVE_JSON, new_matches)  # Сохраняем матчи
        clean_dead_live_subscriptions()  # Очищаем подписки на завершённые матчи
        move_future_subscribers_to_live(new_matches)
        data = load_subs_json()
        total_live = sum(len(u) for u in data["live"].values())
        total_upcoming = sum(len(u) for u in data["upcoming_live"].values())
        unique_live_users = len(set(uid["id"] for users in data["live"].values() for uid in users))
        unique_upcoming_users = len(set(uid["id"] for users in data["upcoming_live"].values() for uid in users))
        notify_live_changes()
        has_live = bool(new_matches)
        has_subs = total_live > 0
        if has_live and has_subs:
            next_update = 60
            logger.info(f"Update Live: {len(new_matches)} | Live: {total_live} ({unique_live_users}) | Live upcoming - {total_upcoming} ({unique_upcoming_users}) | Refetch: {next_update} sec")
            subscriber_event.clear()
            subscriber_event.wait(timeout=next_update)
            if subscriber_event.is_set():
                logger.info("Subscriber trigger: update started immediately.")
        else:
            now = datetime.now()
            next_minute = (now.minute // 10 + 1) * 10
            if next_minute == 60:
                next_minute = 0
                next_hour = now.hour + 1
            else:
                next_hour = now.hour
            next_time = now.replace(hour=next_hour, minute=next_minute, second=0, microsecond=0)
            wait = (next_time - now).total_seconds()
            next_update = int(max(60, wait))
            logger.info(f"Update Live: {len(new_matches)} | Live: {total_live} ({unique_live_users}) | Live upcoming - {total_upcoming} ({unique_upcoming_users}) | Refetch: {next_update} sec")
            subscriber_event.clear()
            subscriber_event.wait(timeout=next_update)
            if subscriber_event.is_set():
                logger.info("Subscriber trigger: update started immediately.")

if __name__ == "__main__":
    # Очистить подписки на завершённые live-матчи при запуске
    notify_live_changes()
    main_loop() 