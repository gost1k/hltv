import os
import sqlite3
from datetime import datetime, timedelta
from src.parser.simple_html import SimpleHTMLParser

DB_PATH = 'hltv.db'
HTML_DIR = 'storage/html/player'
os.makedirs(HTML_DIR, exist_ok=True)

PLAYER_URL = 'https://www.hltv.org/player/{player_id}/{player_nickname}'

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
                parser = SimpleHTMLParser()  # создаём новый парсер для каждого игрока
                html = parser.get_html(url)
                html_path = os.path.join(HTML_DIR, f'{player_id}.html')
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(html)
                parser.driver.quit()  # явно закрываем драйвер
                # next_update через 7 дней, last_update сейчас
                next_update_val = (now + timedelta(days=7)).isoformat()
                last_update_val = now.isoformat()
                update_player_dates(conn, player_id, next_update_val, last_update_val)
                print(f'[OK] Downloaded {player_nickname} ({player_id})')
                updated += 1
            except Exception as e:
                print(f'[ERR] Failed to download {player_nickname} ({player_id}): {e}')
        else:
            skipped += 1
    print(f'Done. Downloaded: {updated}, Skipped: {skipped}')
    conn.close()

if __name__ == '__main__':
    main() 