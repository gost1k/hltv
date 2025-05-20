import sqlite3
import os

DB_PATH = 'hltv.db'

# Описание нужных таблиц
CREATE_TABLES = [
    # Таблица с результатами матчей (прошедшие)
    '''
    CREATE TABLE IF NOT EXISTS result_match (
        match_id INTEGER PRIMARY KEY,
        url TEXT,
        datetime INTEGER,
        team1_id INTEGER,
        team1_name TEXT,
        team1_score INTEGER,
        team1_rank INTEGER,
        team2_id INTEGER,
        team2_name TEXT,
        team2_score INTEGER,
        team2_rank INTEGER,
        event_id INTEGER,
        event_name TEXT,
        demo_id INTEGER,
        head_to_head_team1_wins INTEGER,
        head_to_head_team2_wins INTEGER,
        parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''',
    # Таблица с предстоящими матчами
    '''
    CREATE TABLE IF NOT EXISTS upcoming_match (
        match_id INTEGER PRIMARY KEY,
        datetime INTEGER,
        team1_id INTEGER,
        team1_name TEXT,
        team1_rank INTEGER,
        team2_id INTEGER,
        team2_name TEXT,
        team2_rank INTEGER,
        event_id INTEGER,
        event_name TEXT,
        head_to_head_team1_wins INTEGER,
        head_to_head_team2_wins INTEGER,
        status TEXT DEFAULT 'upcoming',
        parsed INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''',
    # Игроки в предстоящих матчах
    '''
    CREATE TABLE IF NOT EXISTS upcoming_match_players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id INTEGER NOT NULL,
        player_id INTEGER,
        player_nickname TEXT,
        team_id INTEGER,
        team_position INTEGER,
        FOREIGN KEY (match_id) REFERENCES upcoming_match (match_id)
    )
    ''',
    # Ссылки на страницы результатов
    '''
    CREATE TABLE IF NOT EXISTS result_urls (
        id INTEGER PRIMARY KEY,
        url TEXT NOT NULL,
        toParse INTEGER NOT NULL DEFAULT 1
    )
    ''',
    # Ссылки на страницы предстоящих матчей
    '''
    CREATE TABLE IF NOT EXISTS upcoming_urls (
        id INTEGER PRIMARY KEY,
        url TEXT NOT NULL,
        date INTEGER NOT NULL,
        toParse INTEGER NOT NULL DEFAULT 1
    )
    ''',
    # Статистика игроков в прошедших матчах
    '''
    CREATE TABLE IF NOT EXISTS player_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id INTEGER NOT NULL,
        team_id INTEGER,
        player_id INTEGER,
        player_nickname TEXT,
        fullName TEXT,
        nickName TEXT,
        kills INTEGER,
        deaths INTEGER,
        kd_ratio REAL,
        plus_minus INTEGER,
        adr REAL,
        kast REAL,
        rating REAL,
        FOREIGN KEY (match_id) REFERENCES result_match (match_id)
    )
    ''',
    # Таблица с прогнозами по матчам
    '''
    CREATE TABLE IF NOT EXISTS predict (
        match_id INTEGER PRIMARY KEY,
        team1_score INTEGER,
        team2_score INTEGER,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''',
    # Таблица с прогнозами по картам
    '''
    CREATE TABLE IF NOT EXISTS predict_map (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id INTEGER NOT NULL,
        map_name TEXT NOT NULL,
        team1_score INTEGER,
        team2_score INTEGER,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    '''
]

def main():
    abs_db_path = os.path.abspath(DB_PATH)
    if os.path.exists(DB_PATH):
        print(f"Удаляю старую базу данных: {abs_db_path}")
        os.remove(DB_PATH)
    print(f"Создаю новую базу данных: {abs_db_path}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for sql in CREATE_TABLES:
        cursor.execute(sql)
    conn.commit()
    print("Все нужные таблицы созданы.")
    conn.close()

if __name__ == '__main__':
    main() 