import sqlite3
from datetime import datetime

def init_db():
    conn = sqlite3.connect('hltv.db')
    cursor = conn.cursor()
    
    # Create matches table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            date TEXT NOT NULL,
            toParse BOOLEAN NOT NULL DEFAULT 1
        )
    ''')
    
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect('hltv.db')
