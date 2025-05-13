"""
Скрипт для организации HTML-файлов матчей по директориям
"""
import os
import re
import glob
import shutil
import time
from bs4 import BeautifulSoup

def organize_match_files():
    """
    Разделяет HTML-файлы матчей на предстоящие и прошедшие
    """
    html_dir = "storage/html"
    upcoming_dir = os.path.join(html_dir, "upcoming")
    result_dir = os.path.join(html_dir, "result")
    
    # Проверяем существование директорий
    for directory in [html_dir, upcoming_dir, result_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Создана директория: {directory}")
    
    # Получаем список файлов матчей в основной директории
    match_files = glob.glob(os.path.join(html_dir, "match_*.html"))
    print(f"Найдено {len(match_files)} файлов матчей")
    
    # Счетчики для статистики
    upcoming_count = 0
    result_count = 0
    error_count = 0
    
    # Текущее время для сравнения
    current_time = int(time.time())
    
    for file_path in match_files:
        try:
            # Читаем HTML-файл
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Парсим HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Получаем имя файла
            file_name = os.path.basename(file_path)
            
            # Проверяем, завершен ли матч (ищем "Match over" в блоке countdown)
            match_over_element = soup.select_one('.countdown')
            is_match_over = match_over_element and "Match over" in match_over_element.text
            
            # Если матч завершён - перемещаем в result
            if is_match_over:
                target_path = os.path.join(result_dir, file_name)
                shutil.move(file_path, target_path)
                result_count += 1
                print(f"Перемещен в result (матч завершен): {file_name}")
            else:
                # Извлекаем время матча для определения предстоящий/прошедший
                time_element = soup.select_one('.timeAndEvent [data-unix]')
                if time_element:
                    match_time = int(time_element.get('data-unix', 0)) // 1000
                    
                    # Перемещаем в upcoming (даже если матч "пропущен" - т.е. дата в прошлом, но нет "Match over")
                    target_path = os.path.join(upcoming_dir, file_name)
                    shutil.move(file_path, target_path)
                    upcoming_count += 1
                    
                    if match_time > current_time:
                        print(f"Перемещен в upcoming (предстоящий матч): {file_name}")
                    else:
                        print(f"Перемещен в upcoming (пропущенный матч): {file_name}")
                else:
                    # Если не удалось определить время, помещаем в upcoming
                    target_path = os.path.join(upcoming_dir, file_name)
                    shutil.move(file_path, target_path)
                    upcoming_count += 1
                    print(f"Перемещен в upcoming (неизвестное время): {file_name}")
                
        except Exception as e:
            print(f"Ошибка при обработке файла {os.path.basename(file_path)}: {str(e)}")
            error_count += 1
    
    print("\nИтоги:")
    print(f"Перемещено в upcoming: {upcoming_count} файлов")
    print(f"Перемещено в result: {result_count} файлов")
    print(f"Ошибок: {error_count}")
    
    # Проверяем файлы, которые остались в корневой директории
    remaining_files = glob.glob(os.path.join(html_dir, "match_*.html"))
    if remaining_files:
        print(f"\nОсталось необработанных файлов: {len(remaining_files)}")
        for file in remaining_files[:5]:  # Показываем только первые 5 файлов
            print(f"  {os.path.basename(file)}")
        if len(remaining_files) > 5:
            print(f"  ... и еще {len(remaining_files) - 5} файлов")

if __name__ == "__main__":
    print("Запуск организации HTML-файлов матчей")
    organize_match_files() 