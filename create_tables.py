import sqlite3
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_tables(db_path="hltv.db"):
    """Create necessary tables in the database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create match_details table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS match_details (
                match_id INTEGER PRIMARY KEY,
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
                status INTEGER DEFAULT 0,
                parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create player_stats table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER,
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
                parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES match_details (match_id)
            )
        ''')
        
        # Check existing tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]
        logger.info(f"Tables in database: {tables}")
        
        conn.commit()
        logger.info("Tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_tables() 