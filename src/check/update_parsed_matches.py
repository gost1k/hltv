"""
Скрипт для обновления статуса проанализированных матчей

Находит файлы матчей в директории storage/html, проверяет их наличие 
в таблице url_result и устанавливает toParse = 0 для найденных
"""
import os
import re
import sqlite3
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_match_id(file_name):
    """
    Извлекает ID матча из имени файла
    
    Args:
        file_name (str): Имя файла
        
    Returns:
        int or None: ID матча или None, если не удалось извлечь
    """
    # Шаблон для поиска ID в имени файла (старый и новый формат)
    pattern = re.compile(r'^match_(\d+)(?:-.+)?\.html$')
    match = pattern.match(file_name)
    
    if match:
        return int(match.group(1))
    return None

def update_parsed_matches(html_dir="storage/html", db_path="hltv.db"):
    """
    Обновляет статус проанализированных матчей
    
    Args:
        html_dir (str): Путь к директории с HTML-файлами
        db_path (str): Путь к файлу базы данных
        
    Returns:
        dict: Статистика обновления
    """
    if not os.path.exists(html_dir):
        logger.error(f"Директория {html_dir} не существует")
        return {"error": f"Директория {html_dir} не существует"}
    
    if not os.path.exists(db_path):
        logger.error(f"База данных {db_path} не существует")
        return {"error": f"База данных {db_path} не существует"}
    
    # Статистика
    stats = {
        "total_files": 0,
        "match_files": 0,
        "found_in_db": 0,
        "updated": 0,
        "not_found": 0
    }
    
    # Получаем список файлов
    files = os.listdir(html_dir)
    stats["total_files"] = len(files)
    
    # Список найденных ID матчей
    match_ids = []
    not_found_ids = []
    
    # Извлекаем ID матчей из имен файлов
    for file_name in files:
        match_id = extract_match_id(file_name)
        if match_id:
            match_ids.append(match_id)
            stats["match_files"] += 1
    
    logger.info(f"Найдено {stats['match_files']} файлов матчей из {stats['total_files']} файлов")
    
    if not match_ids:
        logger.warning("Не найдено ни одного файла матча")
        return stats
    
    # Подключаемся к базе данных
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Обновляем статус для каждого найденного ID
        for match_id in match_ids:
            # Проверяем наличие матча в таблице url_result
            cursor.execute("SELECT id, toParse FROM url_result WHERE id = ?", (match_id,))
            result = cursor.fetchone()
            
            if result:
                stats["found_in_db"] += 1
                
                # Если toParse уже равен 0, пропускаем
                if result[1] == 0:
                    logger.debug(f"Матч ID {match_id} уже имеет toParse = 0")
                    continue
                
                # Обновляем статус
                cursor.execute("UPDATE url_result SET toParse = 0 WHERE id = ?", (match_id,))
                stats["updated"] += 1
                logger.info(f"Обновлен статус матча ID {match_id}: toParse = 0")
            else:
                stats["not_found"] += 1
                not_found_ids.append(match_id)
                logger.debug(f"Матч ID {match_id} не найден в таблице url_result")
        
        # Подтверждаем изменения
        conn.commit()
        
        # Выводим список ID, которые не найдены в базе
        if not_found_ids:
            logger.warning(f"Следующие ID не найдены в базе: {', '.join(map(str, not_found_ids))}")
        
        conn.close()
        logger.info(f"Обновление завершено. Обновлено {stats['updated']} записей из {stats['found_in_db']} найденных")
        
        return stats
    
    except Exception as e:
        logger.error(f"Ошибка при обновлении статуса матчей: {str(e)}")
        return {"error": str(e)}

if __name__ == "__main__":
    logger.info("Запуск обновления статуса проанализированных матчей")
    stats = update_parsed_matches()
    
    # Выводим статистику
    logger.info("Статистика обновления:")
    logger.info(f"- Всего файлов: {stats.get('total_files', 0)}")
    logger.info(f"- Файлов матчей: {stats.get('match_files', 0)}")
    logger.info(f"- Найдено в БД: {stats.get('found_in_db', 0)}")
    logger.info(f"- Обновлено: {stats.get('updated', 0)}")
    logger.info(f"- Не найдено в БД: {stats.get('not_found', 0)}")
    
    if "error" in stats:
        logger.error(f"Произошла ошибка: {stats['error']}")
    else:
        logger.info("Обновление успешно завершено") 