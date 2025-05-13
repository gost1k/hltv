#!/usr/bin/env python
"""
Script to update team ratings format in telegram_bot.py
"""

def main():
    file_path = 'src/bots/user_bot/telegram_bot.py'
    
    # Read the file content
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Replace team ratings display
    old_ratings = '''            # Если есть информация о рейтинге команд
            if match['team1_rank'] or match['team2_rank']:
                team1_rank = f"#{match['team1_rank']}" if match['team1_rank'] else "нет данных"
                team2_rank = f"#{match['team2_rank']}" if match['team2_rank'] else "нет данных"
                message += f"Рейтинг команд: {team1_name} - {team1_rank}, {team2_name} - {team2_rank}\\n\\n"'''
    
    new_ratings = '''            # Если есть информация о рейтинге команд
            if match['team1_rank'] or match['team2_rank']:
                team1_rank = f"#{match['team1_rank']}" if match['team1_rank'] else "нет данных"
                team2_rank = f"#{match['team2_rank']}" if match['team2_rank'] else "нет данных"
                message += f"Рейтинг:\\n{team1_rank} {team1_name}\\n{team2_rank} {team2_name}\\n\\n"'''
    
    # Replace player nickname truncation
    old_nick1 = '''                        nick = player['player_nickname']
                        if len(nick) > 15:
                            nick = nick[:12] + "..."'''
    
    new_nick1 = '''                        nick = player['player_nickname']'''
    
    old_nick2 = '''                        nick = player['player_nickname']
                        if len(nick) > 15:
                            nick = nick[:12] + "..."'''
    
    new_nick2 = '''                        nick = player['player_nickname']'''
    
    # Apply replacements
    content = content.replace(old_ratings, new_ratings)
    
    # Find the occurrences of player nickname processing
    lines = content.split('\n')
    modified_lines = []
    
    skip_line = False
    for i, line in enumerate(lines):
        if skip_line:
            skip_line = False
            continue
        
        if "nick = player['player_nickname']" in line:
            modified_lines.append(line)  # Add the nickname line
            # Check if the next line is the length check
            if i+1 < len(lines) and "if len(nick) > 15:" in lines[i+1]:
                skip_line = True  # Skip the next two lines (if and truncation)
                if i+2 < len(lines) and "nick = nick[:12]" in lines[i+2]:
                    skip_line = True
        else:
            modified_lines.append(line)
    
    # Write the modified content back to the file
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write('\n'.join(modified_lines))
    
    print("File updated successfully!")

if __name__ == "__main__":
    main() 