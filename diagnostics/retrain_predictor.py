"""
Скрипт автоматического переобучения предиктора CS2
Можно запускать через планировщик задач Windows или cron
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scripts.predictor_main import CS2MatchPredictor
from datetime import datetime
from loguru import logger
import sqlite3

# Настройка логирования
logger.add("logs/retrain_{time}.log", rotation="1 week")

def check_if_retrain_needed(db_path='../hltv.db', min_new_matches=50):
    """
    Проверяет, нужно ли переобучать модель
    
    Args:
        db_path: Путь к базе данных
        min_new_matches: Минимальное количество новых матчей для переобучения
    
    Returns:
        bool: True если нужно переобучение
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Проверяем количество новых матчей с момента последнего обучения
        # Для простоты проверяем матчи за последние 7 дней
        query = """
        SELECT COUNT(*) as new_matches
        FROM result_match
        WHERE datetime >= strftime('%s', 'now', '-7 days')
            AND team1_score IS NOT NULL
            AND team2_score IS NOT NULL
        """
        
        result = conn.execute(query).fetchone()
        new_matches = result[0] if result else 0
        
        conn.close()
        
        logger.info(f"Найдено {new_matches} новых матчей за последние 7 дней")
        
        return new_matches >= min_new_matches
        
    except Exception as e:
        logger.error(f"Ошибка при проверке необходимости переобучения: {e}")
        return False

def evaluate_current_model(predictor, days_back=7):
    """
    Оценивает текущую модель на последних данных
    
    Returns:
        float: Точность модели (0-1)
    """
    try:
        conn = sqlite3.connect(predictor.db_path)
        
        # Получаем предсказания и результаты за последние дни
        query = """
        SELECT 
            p.team1_score_final,
            p.team2_score_final,
            r.team1_score,
            r.team2_score
        FROM predict p
        JOIN result_match r ON p.match_id = r.match_id
        WHERE r.datetime >= strftime('%s', 'now', '-' || ? || ' days')
            AND r.team1_score IS NOT NULL
            AND r.team2_score IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn, params=[days_back])
        conn.close()
        
        if df.empty:
            return 0.0
        
        # Считаем точность предсказания победителя
        pred_winner = (df['team1_score_final'] > df['team2_score_final']).astype(int)
        real_winner = (df['team1_score'] > df['team2_score']).astype(int)
        accuracy = (pred_winner == real_winner).mean()
        
        logger.info(f"Текущая точность модели: {accuracy:.2%}")
        return accuracy
        
    except Exception as e:
        logger.error(f"Ошибка при оценке модели: {e}")
        return 0.0

def main():
    """Главная функция автоматического переобучения"""
    logger.info("="*50)
    logger.info("Запуск автоматического переобучения предиктора")
    logger.info(f"Время: {datetime.now()}")
    logger.info("="*50)
    
    # Проверяем необходимость переобучения
    if not check_if_retrain_needed():
        logger.info("Недостаточно новых данных для переобучения")
        return
    
    # Создаем экземпляр предиктора
    predictor = CS2MatchPredictor()
    
    # Оцениваем текущую модель если она существует
    current_accuracy = 0.0
    if os.path.exists(predictor.model_path):
        try:
            from autogluon.tabular import TabularPredictor
            predictor.predictor = TabularPredictor.load(predictor.model_path)
            current_accuracy = evaluate_current_model(predictor)
        except Exception as e:
            logger.warning(f"Не удалось загрузить текущую модель: {e}")
    
    # Переобучаем модель
    logger.info("Начинаем переобучение модели...")
    success = predictor.retrain(days_back=30)
    
    if not success:
        logger.error("Переобучение не удалось!")
        return
    
    # Оцениваем новую модель
    new_accuracy = evaluate_current_model(predictor)
    
    # Сравниваем результаты
    if current_accuracy > 0:
        improvement = new_accuracy - current_accuracy
        logger.info(f"Изменение точности: {improvement:+.2%}")
        
        if improvement < -0.05:  # Если точность упала более чем на 5%
            logger.warning("Новая модель показывает худшие результаты!")
            # В реальной системе здесь можно было бы откатить модель
    
    # Создаем отчет о переобучении
    report = {
        'timestamp': datetime.now().isoformat(),
        'previous_accuracy': current_accuracy,
        'new_accuracy': new_accuracy,
        'improvement': new_accuracy - current_accuracy,
        'status': 'success'
    }
    
    # Сохраняем отчет
    import json
    report_path = f"logs/retrain_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Отчет сохранен: {report_path}")
    logger.info("Переобучение завершено успешно!")
    
    # Запускаем предсказание для новых матчей
    logger.info("Обновляем предсказания...")
    predictor.predict_upcoming_matches()
    
    # Создаем визуализации
    logger.info("Создаем визуализации...")
    os.system("python predictor_visualizer.py")
    
    logger.info("Все задачи выполнены!")

if __name__ == "__main__":
    import pandas as pd  # Импортируем здесь для evaluate_current_model
    main() 