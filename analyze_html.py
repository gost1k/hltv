from bs4 import BeautifulSoup
import os

def main():
    # Путь к файлу
    html_file = 'storage/html/matches.html'

    # Проверяем существование файла
    if not os.path.exists(html_file):
        print(f"Файл {html_file} не найден")
        exit(1)

    # Читаем файл
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # Парсим HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Находим основной контент
    main_content = soup.select_one('.mainContent')
    if not main_content:
        print("Не найден основной контент (.mainContent)")
        exit(1)

    # Находим все элементы матчей
    match_elements = main_content.select('.match')
    print(f"Всего найдено {len(match_elements)} элементов матчей")

    # Анализируем первые 3 элемента матчей
    for i, match in enumerate(match_elements[:3]):
        print(f"\n--- Матч #{i+1} ---")
        print(f"HTML-структура: {match.name}.{' '.join(match.get('class', []))}")
        
        # Находим идентификатор матча
        match_id = match.get('data-livescore-match')
        print(f"ID матча: {match_id}")
        
        # Ищем информацию о командах (более детально)
        print("\nПоиск команд:")
        
        # Проверяем разные подходы к поиску команд
        team_selectors = [
            '.teamRow', '.teams', '.matchTeam', 
            'div[class*="team"]', '.team-container'
        ]
        
        for selector in team_selectors:
            elements = match.select(selector)
            print(f"  Селектор '{selector}': найдено {len(elements)} элементов")
        
        print("\nИмена команд:")
        # Прямой поиск текста команд
        team_names = []
        for anchor in match.select('a'):
            href = anchor.get('href', '')
            if '/matches/' in href and 'vs' in href:
                vs_text = href.split('/')[-1].split('-')[0]
                try:
                    teams = vs_text.split('vs')
                    team1 = teams[0]
                    team2 = teams[1] if len(teams) > 1 else "Unknown"
                    print(f"  Из ссылки: {team1} vs {team2}")
                    team_names = [team1, team2]
                    break
                except:
                    pass
        
        # Дополнительный поиск элементов с именами команд
        team_name_elements = []
        for div in match.find_all('div'):
            if 'team' in ' '.join(div.get('class', [])).lower():
                team_text = div.get_text().strip()
                if team_text and team_text not in team_name_elements:
                    team_name_elements.append(team_text)
        
        print("  Найденные имена команд через div[class*=team]:")
        for idx, name in enumerate(team_name_elements[:10]):
            print(f"    {idx+1}. {name}")
        
        # Ищем элементы времени
        print("\nПоиск информации о времени:")
        time_elements = []
        unix_timestamp = None
        
        # Искать элементы с атрибутом data-unix
        for el in match.find_all(lambda tag: tag.has_attr('data-unix')):
            time_text = el.get_text().strip()
            unix_time = el.get('data-unix')
            unix_timestamp = unix_time
            time_elements.append(f"{el.name}.{' '.join(el.get('class', []))}: {time_text} (unix: {unix_time})")
        
        if time_elements:
            print("  Найдены элементы времени:")
            for time_el in time_elements:
                print(f"    {time_el}")
        else:
            print("  Элементы времени не найдены")
            
        # Извлекаем ссылку на матч
        match_links = match.select('a')
        print(f"\nНайдено {len(match_links)} ссылок")
        for j, link in enumerate(match_links[:2]):
            href = link.get('href', '')
            print(f"  Ссылка {j+1}: {href}")
        
        # Собираем всю найденную информацию
        print("\nРезультаты анализа:")
        print(f"  ID матча: {match_id}")
        if team_names:
            print(f"  Команды: {team_names[0]} vs {team_names[1]}")
        else:
            print("  Команды не определены")
        
        print(f"  Unix-время: {unix_timestamp}")
        print(f"  URL: {match_links[0].get('href', '') if match_links else 'Не найден'}")

if __name__ == "__main__":
    main() 