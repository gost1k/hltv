import os
import sqlite3
from datetime import datetime, timedelta
from src.parser.base import BaseParser
import time
import logging
from src.utils.telegram_log_handler import TelegramLogHandler
import json

DB_PATH = 'hltv.db'
HTML_DIR = 'storage/html/player'
os.makedirs(HTML_DIR, exist_ok=True)

PLAYER_URL = 'https://www.hltv.org/player/{player_id}/{player_nickname}'

# Настройка Telegram логгера
try:
    with open("src/bots/config/dev_bot_config.json", encoding="utf-8") as f:
        dev_bot_config = json.load(f)
    dev_bot_token = dev_bot_config["token"]
    telegram_handler = TelegramLogHandler(dev_bot_token, chat_id="7146832422")
    telegram_handler.setLevel(logging.INFO)
    logger = logging.getLogger("players_html_downloader")
    logger.addHandler(telegram_handler)
except Exception as e:
    logger = logging.getLogger("players_html_downloader")
    logger.warning(f"[Telegram] Not configured: {e}")

# Класс для скачивания HTML через BaseParser
class PlayerHTMLDownloader(BaseParser):
    def download_html(self, url):
        self.driver.get(url)
        self._wait_for_page_load()
        return self.driver.page_source
    def parse(self):
        pass  # Не используется, но требуется для абстрактного класса

# Вспомогательная функция для получения игроков из upcoming_match_players
def get_upcoming_players(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT player_id, player_nickname FROM upcoming_match_players WHERE player_id IS NOT NULL')
    return cursor.fetchall()

# Проверка наличия игрока в players
def get_player_row(conn, player_id):
    cursor = conn.cursor()
    cursor.execute('SELECT player_id, next_update FROM players WHERE player_id = ?', (player_id,))
    return cursor.fetchone()

# Добавление нового игрока (только id и nickname)
def insert_new_player(conn, player_id, player_nickname):
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO players (player_id, player_nickname) VALUES (?, ?)', (player_id, player_nickname))
    conn.commit()

# Обновление next_update и last_update
def update_player_dates(conn, player_id, next_update, last_update):
    cursor = conn.cursor()
    cursor.execute('UPDATE players SET next_update = ?, last_update = ? WHERE player_id = ?', (next_update, last_update, player_id))
    conn.commit()


def main():
    conn = sqlite3.connect(DB_PATH)
    players = get_upcoming_players(conn)
    now = datetime.now()
    updated = 0
    skipped = 0
    for player_id, player_nickname in players:
        if not player_id or not player_nickname:
            continue
        row = get_player_row(conn, player_id)
        need_download = False
        if row is None:
            insert_new_player(conn, player_id, player_nickname)
            need_download = True
        else:
            _, next_update = row
            if not next_update:
                need_download = True
            else:
                try:
                    next_update_dt = datetime.fromisoformat(next_update)
                    if now >= next_update_dt:
                        need_download = True
                except Exception:
                    need_download = True
        if need_download:
            url = PLAYER_URL.format(player_id=player_id, player_nickname=player_nickname)
            try:
                with PlayerHTMLDownloader() as parser:
                    html = parser.download_html(url)
                    # Проверка на Cloudflare
                    if '<a rel="noopener noreferrer"' in html and '>Cloudflare<' in html:
                        html_path = os.path.join(HTML_DIR, f'{player_id}.html')
                        with open(html_path, 'w', encoding='utf-8') as f:
                            f.write(html)
                        logger.warning("При парсинге игроков уперлись в защиту", extra={"telegram_firstline": True})
                        if hasattr(telegram_handler, 'send_buffer'):
                            telegram_handler.send_buffer()
                        print(f'[ERR] Cloudflare detected for {player_nickname} ({player_id}), process stopped.')
                        break
                    html_path = os.path.join(HTML_DIR, f'{player_id}.html')
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(html)
                    # next_update через 7 дней, last_update сейчас
                    next_update_val = (now + timedelta(days=7)).isoformat()
                    last_update_val = now.isoformat()
                    update_player_dates(conn, player_id, next_update_val, last_update_val)
                    print(f'[OK] Downloaded {player_nickname} ({player_id})')
                    updated += 1
            except Exception as e:
                # Сохраняем html даже при ошибке, если возможно
                html = None
                try:
                    html = parser.driver.page_source
                except Exception:
                    pass
                if html:
                    html_path = os.path.join(HTML_DIR, f'{player_id}.html')
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(html)
                    print(f'[ERR] Saved partial HTML for {player_nickname} ({player_id}) due to error: {e}')
                else:
                    print(f'[ERR] Failed to download and save HTML for {player_nickname} ({player_id}): {e}')
                break  # Останавливаем процесс
            time.sleep(25)  # Таймаут между запросами
        else:
            skipped += 1
    print(f'Done. Downloaded: {updated}, Skipped: {skipped}')
    conn.close()

if __name__ == '__main__':
    main() 