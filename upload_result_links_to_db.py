import sqlite3
import time
import re

DB_PATH = 'hltv.db'
TXT_PATH = 'result_links_to_upload.txt'
SLEEP_SECONDS = 180  # 3 минуты

def parse_id_from_url(url):
    m = re.match(r"/matches/(\d+)", url)
    return int(m.group(1)) if m else None

def insert_one_link():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS result_urls (
        id INTEGER PRIMARY KEY,
        url TEXT NOT NULL,
        toParse INTEGER NOT NULL DEFAULT 1
    )''')
    conn.commit()

    # Читаем все строки
    with open(TXT_PATH, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]

    for idx, url in enumerate(lines):
        match_id = parse_id_from_url(url)
        if match_id is None:
            continue
        cursor.execute('SELECT 1 FROM result_urls WHERE id=?', (match_id,))
        if cursor.fetchone():
            continue
        # Добавляем только первую подходящую ссылку
        cursor.execute('INSERT INTO result_urls (id, url, toParse) VALUES (?, ?, 1)', (match_id, url))
        conn.commit()
        conn.close()
        # Удаляем эту строку из файла
        lines.pop(idx)
        with open(TXT_PATH, 'w', encoding='utf-8') as fw:
            for l in lines:
                fw.write(l + '\n')
        print(f"[INFO] Добавлена ссылка: {url}. Жду 3 минуты...")
        return
    conn.close()
    print("[INFO] Нет новых ссылок для добавления. Жду 3 минуты...")

if __name__ == "__main__":
    while True:
        insert_one_link()
        time.sleep(SLEEP_SECONDS) 