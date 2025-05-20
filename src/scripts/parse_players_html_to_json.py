import os
import json
from bs4 import BeautifulSoup

HTML_DIR = 'storage/html/player'
JSON_DIR = 'storage/json/player'
os.makedirs(JSON_DIR, exist_ok=True)

# Функция для парсинга одного HTML профиля игрока
def parse_player_html(html, player_id):
    soup = BeautifulSoup(html, "html.parser")
    def safe_text(sel, attr=None):
        el = soup.select_one(sel)
        if not el:
            return None
        if attr:
            return el.get(attr)
        return el.text.strip()
    def safe_num(sel, attr=None, cast=int):
        val = safe_text(sel, attr)
        if val is None:
            return None
        try:
            return cast(''.join(c for c in val if c.isdigit() or c in '.,' or c == '-').replace(',', '.'))
        except Exception:
            return None
    data = {
        "player_id": int(player_id),
        # player_nickname не трогаем, он уже есть в базе
        "country": safe_text('img.flag', 'title'),
        "real_name": safe_text('.playerRealname'),
        "age": safe_num('.playerAge span[itemprop="text"]'),
        "current_team": safe_text('.playerTeam a'),
        "prize_money": safe_num('.playerPrizeMoney .listRight', cast=float),
        "maps_past3": safe_num('.stats-matches .stats-window'),
        "rating_2_1": safe_num('.playerpage-container-attributes .player-stat:nth-child(1) .statsVal p', cast=float),
        "firepower": safe_num('.playerpage-container-attributes .player-stat:nth-child(2) .statsVal b', cast=float),
        "entrying": safe_num('.playerpage-container-attributes .player-stat:nth-child(3) .statsVal b', cast=float),
        "trading": safe_num('.playerpage-container-attributes .player-stat:nth-child(4) .statsVal b', cast=float),
        "opening": safe_num('.playerpage-container-attributes .player-stat:nth-child(5) .statsVal b', cast=float),
        "clutching": safe_num('.playerpage-container-attributes .player-stat:nth-child(6) .statsVal b', cast=float),
        "sniping": safe_num('.playerpage-container-attributes .player-stat:nth-child(7) .statsVal b', cast=float),
        "utility": safe_num('.playerpage-container-attributes .player-stat:nth-child(8) .statsVal b', cast=float),
        "teams_count": safe_num('#teamsBox .highlighted-stat:nth-child(1) .stat'),
        "days_in_current_team": safe_num('#teamsBox .highlighted-stat:nth-child(2) .stat'),
        "days_in_teams": safe_num('#teamsBox .highlighted-stat:nth-child(3) .stat'),
        "majors_played": safe_num('#achievementBox #majorAchievement .highlighted-stat:nth-child(2) .stat'),
        "majors_won": safe_num('#achievementBox #majorAchievement .highlighted-stat:nth-child(1) .stat'),
        "lans_played": safe_num('#lanAchievement .highlighted-stat:nth-child(2) .stat'),
        "lans_won": safe_num('#lanAchievement .highlighted-stat:nth-child(1) .stat'),
        "faceit_url": (soup.select_one('.socialMediaButtons a[href*="faceit.com"]').get('href') if soup.select_one('.socialMediaButtons a[href*="faceit.com"]') else None),
        "faceit_matches": safe_num('#faceitBox .all-time-stat:nth-child(1) .stat'),
        "faceit_winrate": safe_num('#faceitBox .all-time-stat:nth-child(2) .stat', cast=float),
        "faceit_winstreak": safe_num('#faceitBox .all-time-stat:nth-child(3) .stat'),
        "faceit_avgkdr": safe_num('#faceitBox .all-time-stat:nth-child(4) .stat', cast=float),
        "faceit_headshots": safe_num('#faceitBox .all-time-stat:nth-child(5) .stat', cast=float),
    }
    return data

def main():
    for filename in os.listdir(HTML_DIR):
        if not filename.endswith('.html'):
            continue
        player_id = filename.replace('.html', '')
        html_path = os.path.join(HTML_DIR, filename)
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()
        data = parse_player_html(html, player_id)
        json_path = os.path.join(JSON_DIR, f"{player_id}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[OK] Parsed {player_id}")
        os.remove(html_path)

if __name__ == '__main__':
    main() 