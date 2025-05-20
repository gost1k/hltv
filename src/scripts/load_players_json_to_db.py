import os
import json
import sqlite3
from datetime import datetime
import logging
from src.utils.telegram_log_handler import TelegramLogHandler

DB_PATH = 'hltv.db'
JSON_DIR = 'storage/json/player'

# Настройка Telegram логгера (используем тот же токен и chat_id, что и в других скриптах)
try:
    with open("src/bots/config/dev_bot_config.json", encoding="utf-8") as f:
        dev_bot_config = json.load(f)
    dev_bot_token = dev_bot_config["token"]
    telegram_handler = TelegramLogHandler(dev_bot_token, chat_id="7146832422")
    telegram_handler.setLevel(logging.INFO)
    logger = logging.getLogger("players_loader")
    logger.addHandler(telegram_handler)
except Exception as e:
    logger = logging.getLogger("players_loader")
    logger.warning(f"[Telegram] Not configured: {e}")

def update_player(conn, data):
    cursor = conn.cursor()
    fields = [
        "country", "real_name", "age", "current_team", "prize_money", "maps_past3",
        "rating_2_1", "firepower", "entrying", "trading", "opening", "clutching",
        "sniping", "utility", "teams_count", "days_in_current_team", "days_in_teams",
        "majors_played", "majors_won", "lans_played", "lans_won", "faceit_url", "faceit_matches", "faceit_winrate",
        "faceit_winstreak", "faceit_avgkdr", "faceit_headshots"
    ]
    set_clause = ", ".join([f"{f}=?" for f in fields] + ["last_update=?"])
    values = [data.get(f) for f in fields] + [datetime.now().isoformat(), data["player_id"]]
    sql = f"UPDATE players SET {set_clause} WHERE player_id=?"
    cursor.execute(sql, values)
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    processed = 0
    success = 0
    error = 0
    for filename in os.listdir(JSON_DIR):
        if not filename.endswith('.json'):
            continue
        json_path = os.path.join(JSON_DIR, filename)
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            update_player(conn, data)
            print(f"[OK] Updated player {data['player_id']}")
            os.remove(json_path)
            success += 1
        except Exception as e:
            print(f"[ERR] Failed to update from {filename}: {e}")
            error += 1
        processed += 1
    conn.close()
    # Telegram уведомление
    try:
        msg = f"Загрузка игроков завершена. Всего: {processed}, успешно: {success}, ошибок: {error}"
        logger.info(msg, extra={"telegram_firstline": True})
        if hasattr(telegram_handler, 'send_buffer'):
            telegram_handler.send_buffer()
    except Exception as e:
        print(f"[Telegram] Failed to send notification: {e}")

if __name__ == '__main__':
    main() 