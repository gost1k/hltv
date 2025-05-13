import os
import re
import glob
import argparse
from bs4 import BeautifulSoup
from src.collector.match_details import MatchDetailsCollector

def analyze_html_file(file_path):
    """
    Анализирует HTML-файл матча и выводит результаты в консоль
    
    Args:
        file_path (str): Путь к HTML-файлу
    """
    print(f"\n{'='*80}")
    print(f"Анализ файла: {os.path.basename(file_path)}")
    print(f"{'='*80}")
    
    # Извлекаем ID матча из имени файла
    match = re.search(r'match_(\d+)', os.path.basename(file_path))
    if match:
        match_id = int(match.group(1))
        print(f"ID матча: {match_id}")
    else:
        print("Ошибка: Не удалось извлечь ID матча из имени файла")
        return
    
    # Читаем HTML-файл
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Создаем экземпляр коллектора и парсим HTML
        collector = MatchDetailsCollector()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Анализ основной информации о матче
        print("\n--- Структура HTML ---")
        
        # Проверка наличия основных элементов
        time_element = soup.select_one('.timeAndEvent [data-unix]')
        print(f"Блок времени матча: {'Найден' if time_element else 'Не найден'}")
        if time_element:
            unix_time = int(time_element.get('data-unix', 0)) // 1000
            print(f"  Время матча (unix): {unix_time}")
        
        team1_element = soup.select_one('.team1-gradient')
        print(f"Блок первой команды: {'Найден' if team1_element else 'Не найден'}")
        
        team2_element = soup.select_one('.team2-gradient')
        print(f"Блок второй команды: {'Найден' if team2_element else 'Не найден'}")
        
        team1_score = soup.select_one('.team1-gradient .won div, .team1-gradient .lost div')
        print(f"Счет первой команды: {'Найден' if team1_score else 'Не найден'}")
        if team1_score:
            print(f"  Значение: {team1_score.text.strip()}")
        
        team2_score = soup.select_one('.team2-gradient .won div, .team2-gradient .lost div')
        print(f"Счет второй команды: {'Найден' if team2_score else 'Не найден'}")
        if team2_score:
            print(f"  Значение: {team2_score.text.strip()}")
        
        stats_table = soup.select_one('.stats-content table')
        print(f"Таблица статистики: {'Найдена' if stats_table else 'Не найдена'}")
        if stats_table:
            rows = stats_table.select('tr')
            print(f"  Количество строк: {len(rows)}")
            
            header_row = stats_table.select_one('tr')
            if header_row:
                headers = header_row.select('th')
                print(f"  Заголовки ({len(headers)}): {[h.get_text().strip() for h in headers]}")
        
        # Парсим данные матча
        print("\n--- Результаты парсинга ---")
        match_data = collector._parse_match_details(soup, match_id)
        
        if match_data:
            print("\nДанные матча:")
            for key, value in match_data.items():
                filled = "✓" if value is not None else "✗"
                print(f"  {key}: {value} {filled}")
        else:
            print("Ошибка: Не удалось извлечь данные матча")
        
        # Парсим статистику игроков
        players_data = collector._parse_player_stats(soup, match_id)
        
        if players_data:
            print(f"\nДанные игроков ({len(players_data)}):")
            for i, player in enumerate(players_data, 1):
                print(f"\n  Игрок {i}: {player.get('player_nickname', 'Неизвестно')}")
                for key, value in player.items():
                    if key not in ['match_id', 'player_nickname']:
                        filled = "✓" if value is not None else "✗"
                        print(f"    {key}: {value} {filled}")
        else:
            print("Ошибка: Не удалось извлечь данные игроков")
            
    except Exception as e:
        print(f"Ошибка при анализе файла: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Анализ HTML-файлов матчей HLTV")
    parser.add_argument("--id", type=int, help="ID конкретного матча для анализа")
    parser.add_argument("--all", action="store_true", help="Анализировать все файлы")
    parser.add_argument("--limit", type=int, default=5, help="Лимит количества анализируемых файлов")
    parser.add_argument("--dir", type=str, default="storage/html", help="Директория с HTML-файлами")
    
    args = parser.parse_args()
    
    html_dir = args.dir
    if not os.path.exists(html_dir):
        print(f"Ошибка: Директория {html_dir} не существует")
        return
    
    if args.id:
        # Анализ конкретного матча
        match_files = glob.glob(os.path.join(html_dir, f"match_{args.id}*.html"))
        if not match_files:
            print(f"Ошибка: Файл для матча с ID {args.id} не найден")
            return
        
        analyze_html_file(match_files[0])
    elif args.all:
        # Анализ всех файлов с учетом лимита
        match_files = glob.glob(os.path.join(html_dir, "match_*.html"))
        print(f"Найдено {len(match_files)} файлов матчей")
        
        limit = args.limit if args.limit > 0 else len(match_files)
        print(f"Будет проанализировано {min(limit, len(match_files))} файлов")
        
        for i, file_path in enumerate(match_files[:limit]):
            analyze_html_file(file_path)
            if i < limit - 1:
                input("\nНажмите Enter для анализа следующего файла...")
    else:
        print("Укажите ID матча (--id) или флаг --all для анализа всех файлов")

if __name__ == "__main__":
    main() 