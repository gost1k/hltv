"""
Скрипт для исправления проблем парсера match_details.py
"""
import os
import sqlite3
import re

def fix_player_nicknames():
    """
    Исправляет имена игроков в таблице player_stats, удаляя переносы строк
    """
    print("Исправление имен игроков...")
    
    conn = sqlite3.connect('hltv.db')
    cursor = conn.cursor()
    
    # Получаем все записи с некорректными именами
    cursor.execute("SELECT id, player_nickname FROM player_stats")
    records = cursor.fetchall()
    
    updated = 0
    for record_id, nickname in records:
        if nickname and '\n' in nickname:
            # Удаляем переносы строк и лишние пробелы
            fixed_nickname = re.sub(r'\s+', ' ', nickname.replace('\n', ' ')).strip()
            
            cursor.execute(
                "UPDATE player_stats SET player_nickname = ? WHERE id = ?",
                (fixed_nickname, record_id)
            )
            updated += 1
    
    conn.commit()
    print(f"Исправлено {updated} имен игроков")
    
    # Выводим примеры исправленных имен
    if updated > 0:
        cursor.execute("SELECT player_nickname FROM player_stats LIMIT 5")
        examples = cursor.fetchall()
        print("Примеры исправленных имен:")
        for example in examples:
            print(f"  {example[0]}")
    
    conn.close()

def update_matches_status():
    """
    Помечает предстоящие матчи как 'upcoming' в таблице match_details
    """
    print("Обновление статуса матчей...")
    
    conn = sqlite3.connect('hltv.db')
    cursor = conn.cursor()
    
    # Добавляем столбец status, если его нет
    try:
        cursor.execute("ALTER TABLE match_details ADD COLUMN status TEXT")
        print("Добавлен столбец 'status' в таблицу match_details")
    except sqlite3.OperationalError:
        print("Столбец 'status' уже существует")
    
    # Обновляем статус для всех матчей
    current_time = int(os.popen('date +%s').read().strip())
    
    # Помечаем предстоящие матчи
    cursor.execute(
        "UPDATE match_details SET status = 'upcoming' WHERE datetime > ? AND team1_score IS NULL AND team2_score IS NULL",
        (current_time,)
    )
    upcoming_count = cursor.rowcount
    
    # Помечаем прошедшие матчи с результатами
    cursor.execute(
        "UPDATE match_details SET status = 'completed' WHERE team1_score IS NOT NULL AND team2_score IS NOT NULL"
    )
    completed_count = cursor.rowcount
    
    # Помечаем прошедшие матчи без результатов как 'unknown'
    cursor.execute(
        "UPDATE match_details SET status = 'unknown' WHERE status IS NULL"
    )
    unknown_count = cursor.rowcount
    
    conn.commit()
    print(f"Обновлены статусы матчей: {upcoming_count} предстоящих, {completed_count} завершенных, {unknown_count} неизвестных")
    
    conn.close()

def create_stats_view():
    """
    Создает представление для удобного отображения статистики матчей и игроков
    """
    print("Создание представления для статистики...")
    
    conn = sqlite3.connect('hltv.db')
    cursor = conn.cursor()
    
    # Создаем представление
    try:
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS match_stats_view AS
        SELECT 
            m.match_id,
            m.team1_name,
            m.team2_name,
            m.team1_score,
            m.team2_score,
            m.status,
            p.player_nickname,
            p.kills,
            p.deaths,
            p.rating,
            CASE WHEN p.team_id = m.team1_id THEN m.team1_name ELSE m.team2_name END as player_team
        FROM 
            match_details m
        LEFT JOIN 
            player_stats p ON m.match_id = p.match_id
        ORDER BY 
            m.datetime DESC, p.team_id, p.rating DESC
        """)
        print("Создано представление match_stats_view")
    except sqlite3.OperationalError as e:
        print(f"Ошибка при создании представления: {e}")
    
    conn.commit()
    conn.close()

def generate_recommended_fixes():
    """
    Генерирует рекомендуемые исправления для match_details.py
    """
    print("\nРекомендуемые исправления для match_details.py:")
    
    fixes = [
        "1. В методе _extract_player_stats добавить очистку имени игрока от переносов строк:",
        "   player_data['player_nickname'] = player_link.text.strip().replace('\\n', ' ')",
        "",
        "2. В методе _parse_match_details добавить определение статуса матча:",
        "   match_data['status'] = 'upcoming' if match_data['team1_score'] is None and match_data['team2_score'] is None else 'completed'",
        "",
        "3. В методе _save_match_details добавить сохранение статуса:",
        "   cursor.execute('UPDATE match_details SET status = ? WHERE match_id = ?', (match_data['status'], match_data['match_id']))",
        "",
        "4. Добавить проверку на datetime в будущем для предстоящих матчей",
        "   import time",
        "   current_time = int(time.time())",
        "   if match_data['datetime'] > current_time:",
        "       match_data['status'] = 'upcoming'"
    ]
    
    for fix in fixes:
        print(fix)

if __name__ == "__main__":
    print("Запуск исправлений для парсера match_details.py")
    
    # Исправляем имена игроков
    fix_player_nicknames()
    
    # Обновляем статусы матчей
    update_matches_status()
    
    # Создаем представление для статистики
    create_stats_view()
    
    # Выводим рекомендуемые исправления
    generate_recommended_fixes() 