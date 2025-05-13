import sqlite3
import os

def check_db_status():
    if not os.path.exists('hltv.db'):
        print("Database file 'hltv.db' not found!")
        return

    conn = sqlite3.connect('hltv.db')
    cursor = conn.cursor()
    
    # Check match_details table
    try:
        cursor.execute("SELECT COUNT(*) FROM match_details")
        count = cursor.fetchone()[0]
        print(f"Records in match_details table: {count}")
        
        if count > 0:
            cursor.execute("SELECT match_id, team1_name, team2_name, team1_score, team2_score FROM match_details LIMIT 5")
            matches = cursor.fetchall()
            print("\nSample matches:")
            for match in matches:
                print(f"  Match {match[0]}: {match[1]} {match[3]} - {match[4]} {match[2]}")
    except sqlite3.OperationalError as e:
        print(f"Error accessing match_details table: {e}")
    
    # Check player_stats table
    try:
        cursor.execute("SELECT COUNT(*) FROM player_stats")
        count = cursor.fetchone()[0]
        print(f"\nRecords in player_stats table: {count}")
        
        if count > 0:
            cursor.execute("""
                SELECT p.match_id, p.player_nickname, p.kills, p.deaths, m.team1_name, m.team2_name 
                FROM player_stats p
                JOIN match_details m ON p.match_id = m.match_id
                LIMIT 5
            """)
            players = cursor.fetchall()
            print("\nSample player stats:")
            for player in players:
                print(f"  Match {player[0]} ({player[4]} vs {player[5]}): {player[1]} - {player[2]} kills, {player[3]} deaths")
    except sqlite3.OperationalError as e:
        print(f"Error accessing player_stats table: {e}")
    
    # List all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("\nAll tables in database:")
    for table in tables:
        print(f"  {table[0]}")
        
    conn.close()

if __name__ == "__main__":
    check_db_status() 