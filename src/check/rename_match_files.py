"""
Скрипт для переименования файлов матчей из старого формата (match_ID.html) 
в новый формат (match_ID-КОМАНДА1-vs-КОМАНДА2-ТУРНИР.html)
"""
import os
import re
import sqlite3
from urllib.parse import urljoin, urlparse

from src.config import HLTV_BASE_URL, HTML_STORAGE_DIR

def get_filename_from_url(match_id, url):
    """
    Формирует имя файла из URL матча
    
    Args:
        match_id (int): ID матча
        url (str): URL матча
        
    Returns:
        str: Имя файла
    """
    try:
        # Парсим URL
        parsed_url = urlparse(url)
        path = parsed_url.path
        
        # Проверяем, что URL соответствует формату /matches/ID/команда1-vs-команда2-турнир
        if '/matches/' in path:
            # Убираем начальный "/matches/" и ID
            match_info = path.split('/matches/')[1]
            
            # Удаляем ID (если он есть) и получаем строку с командами и турниром
            if match_info.split('/')[0].isdigit():
                match_info = '/'.join(match_info.split('/')[1:])
            
            # Если такая информация есть в URL
            if match_info:
                # Заменяем все специальные символы на дефисы
                match_info = re.sub(r'[^\w-]', '-', match_info).lower()
                # Удаляем лишние дефисы
                match_info = re.sub(r'-+', '-', match_info)
                # Удаляем дефисы в начале и конце
                match_info = match_info.strip('-')
                
                # Формируем имя файла
                return f"match_{match_id}-{match_info}.html"
        
        # Если не удалось извлечь информацию из URL, используем старый формат
        return f"match_{match_id}.html"
        
    except Exception as e:
        print(f"Ошибка при формировании имени файла из URL: {str(e)}")
        return f"match_{match_id}.html"

def get_match_url(match_id, db_path):
    """
    Получает URL матча из базы данных
    
    Args:
        match_id (int): ID матча
        db_path (str): Путь к файлу базы данных
        
    Returns:
        str or None: URL матча или None, если не найден
    """
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Сначала проверяем таблицу прошедших матчей
        cursor.execute("SELECT url FROM url_result WHERE id = ?", (match_id,))
        result = cursor.fetchone()
        
        # Если не найден, проверяем таблицу предстоящих матчей
        if not result:
            cursor.execute("SELECT url FROM url_upcoming WHERE id = ?", (match_id,))
            result = cursor.fetchone()
            
        # Если все еще не найден, проверяем старую таблицу matches
        if not result:
            cursor.execute("SELECT url FROM matches WHERE id = ?", (match_id,))
            result = cursor.fetchone()
        
        conn.close()
        
        if result:
            return result[0]
        return None
        
    except Exception as e:
        print(f"Ошибка при получении URL матча из базы данных: {str(e)}")
        return None

def rename_match_files(html_dir=HTML_STORAGE_DIR, db_path="hltv.db"):
    """
    Переименовывает файлы матчей из старого формата в новый
    
    Args:
        html_dir (str): Путь к директории с HTML-файлами
        db_path (str): Путь к файлу базы данных
        
    Returns:
        int: Количество переименованных файлов
    """
    renamed_count = 0
    
    # Проверяем, что директория существует
    if not os.path.exists(html_dir):
        print(f"Директория {html_dir} не существует")
        return renamed_count
    
    # Получаем список файлов
    files = os.listdir(html_dir)
    
    # Регулярное выражение для поиска файлов в старом формате: match_ЧИСЛО.html
    pattern = re.compile(r'^match_(\d+)\.html$')
    
    # Для каждого файла
    for file_name in files:
        match = pattern.match(file_name)
        if match:
            # Получаем ID матча из имени файла
            match_id = int(match.group(1))
            
            # Полный путь к файлу
            old_path = os.path.join(html_dir, file_name)
            
            # Получаем URL матча из базы данных
            url = get_match_url(match_id, db_path)
            
            if url:
                # Формируем новое имя файла
                new_file_name = get_filename_from_url(match_id, url)
                
                # Если новое имя отличается от старого
                if new_file_name != file_name:
                    new_path = os.path.join(html_dir, new_file_name)
                    
                    # Проверяем, не существует ли уже файл с таким именем
                    if not os.path.exists(new_path):
                        try:
                            # Переименовываем файл
                            os.rename(old_path, new_path)
                            print(f"Переименован: {file_name} -> {new_file_name}")
                            renamed_count += 1
                        except Exception as e:
                            print(f"Ошибка при переименовании {file_name}: {str(e)}")
                    else:
                        print(f"Пропущен {file_name}: файл {new_file_name} уже существует")
            else:
                print(f"Пропущен {file_name}: URL не найден в базе данных")
    
    return renamed_count

if __name__ == "__main__":
    # Запуск переименования
    print("Начало переименования файлов матчей...")
    renamed = rename_match_files()
    print(f"Переименовано файлов: {renamed}") 