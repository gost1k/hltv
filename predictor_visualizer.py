"""
Визуализация результатов предиктора CS2
Создает графики важности признаков и анализ точности
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import json
from pathlib import Path
from datetime import datetime
import numpy as np

# Настройка стиля графиков
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# Пути
DB_PATH = 'hltv.db'
DIAGNOSTICS_PATH = 'predictor'
OUTPUT_PATH = 'predictor/visualizations'

# Создаем директорию для визуализаций
Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

def visualize_feature_importance():
    """Визуализация важности признаков"""
    # Загружаем данные о важности признаков
    importance_file = f"{DIAGNOSTICS_PATH}/feature_importance.csv"
    
    if not Path(importance_file).exists():
        print("Файл с важностью признаков не найден. Сначала запустите предиктор.")
        return
    
    importance_df = pd.read_csv(importance_file)
    
    # Топ-20 важных признаков
    top_features = importance_df.head(20)
    
    # График важности признаков
    plt.figure(figsize=(12, 8))
    plt.barh(top_features.index, top_features['importance'], color='skyblue')
    plt.xlabel('Важность признака')
    plt.ylabel('Признак')
    plt.title('Топ-20 важных признаков для предсказания результатов матчей CS2')
    plt.gca().invert_yaxis()
    
    # Добавляем значения на график
    for i, v in enumerate(top_features['importance']):
        plt.text(v + 0.001, i, f'{v:.3f}', va='center')
    
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PATH}/feature_importance.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"График важности признаков сохранен: {OUTPUT_PATH}/feature_importance.png")
    
    # Группируем признаки по категориям
    feature_categories = {
        'rank': ['rank_diff', 'rank_ratio', 'log_rank_team1', 'log_rank_team2'],
        'h2h': ['h2h_total', 'h2h_winrate_team1'],
        'time': ['hour', 'weekday'],
        'player_stats': ['rating_diff', 'kd_diff', 'firepower_diff', 'avg_rating', 'avg_kd', 'avg_adr', 'avg_kast'],
        'form': ['winrate_diff', 'matches_played_diff', 'recent_winrate', 'avg_score_for', 'avg_score_against']
    }
    
    # Считаем суммарную важность по категориям
    category_importance = {}
    for category, features in feature_categories.items():
        cat_importance = 0
        for feature in features:
            for col in importance_df.index:
                if any(f in col for f in features):
                    cat_importance += importance_df.loc[col, 'importance']
        category_importance[category] = cat_importance
    
    # График важности по категориям
    if category_importance:
        plt.figure(figsize=(10, 6))
        categories = list(category_importance.keys())
        values = list(category_importance.values())
        
        bars = plt.bar(categories, values, color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8'])
        
        # Добавляем значения на столбцы
        for bar, value in zip(bars, values):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                    f'{value:.3f}', ha='center', va='bottom')
        
        plt.xlabel('Категория признаков')
        plt.ylabel('Суммарная важность')
        plt.title('Важность категорий признаков')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_PATH}/category_importance.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"График важности категорий сохранен: {OUTPUT_PATH}/category_importance.png")

def visualize_model_comparison():
    """Визуализация сравнения моделей"""
    leaderboard_file = f"{DIAGNOSTICS_PATH}/model_leaderboard.csv"
    
    if not Path(leaderboard_file).exists():
        print("Файл с результатами моделей не найден.")
        return
    
    leaderboard = pd.read_csv(leaderboard_file)
    
    # График сравнения моделей по метрикам
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # ROC AUC
    ax = axes[0, 0]
    models = leaderboard['model'].head(10)
    scores = leaderboard['score_val'].head(10)
    bars = ax.barh(models, scores, color='lightcoral')
    ax.set_xlabel('ROC AUC Score')
    ax.set_title('Сравнение моделей по ROC AUC')
    ax.set_xlim(0.5, 1.0)
    
    # Добавляем значения
    for bar, score in zip(bars, scores):
        ax.text(score + 0.002, bar.get_y() + bar.get_height()/2,
                f'{score:.4f}', va='center')
    
    # Время предсказания
    ax = axes[0, 1]
    pred_time = leaderboard['pred_time_val'].head(10)
    bars = ax.barh(models, pred_time, color='lightgreen')
    ax.set_xlabel('Время предсказания (сек)')
    ax.set_title('Скорость предсказания моделей')
    ax.set_xscale('log')
    
    # Время обучения
    ax = axes[1, 0]
    fit_time = leaderboard['fit_time'].head(10)
    bars = ax.barh(models, fit_time, color='lightskyblue')
    ax.set_xlabel('Время обучения (сек)')
    ax.set_title('Время обучения моделей')
    
    # Комбинированная метрика (ROC AUC / время предсказания)
    ax = axes[1, 1]
    efficiency = scores / pred_time
    bars = ax.barh(models, efficiency, color='gold')
    ax.set_xlabel('Эффективность (ROC AUC / время)')
    ax.set_title('Эффективность моделей')
    
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PATH}/model_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"График сравнения моделей сохранен: {OUTPUT_PATH}/model_comparison.png")

def visualize_prediction_accuracy():
    """Визуализация точности предсказаний"""
    conn = sqlite3.connect(DB_PATH)
    
    # Загружаем предсказания и реальные результаты
    query = """
    SELECT 
        p.match_id,
        p.team1_score as pred_team1_prob,
        p.team2_score as pred_team2_prob,
        p.team1_score_final as pred_team1_score,
        p.team2_score_final as pred_team2_score,
        p.confidence,
        r.team1_score as real_team1_score,
        r.team2_score as real_team2_score,
        r.team1_name,
        r.team2_name,
        r.datetime
    FROM predict p
    JOIN result_match r ON p.match_id = r.match_id
    WHERE r.team1_score IS NOT NULL AND r.team2_score IS NOT NULL
    ORDER BY r.datetime DESC
    LIMIT 100
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("Нет данных для анализа точности")
        return
    
    # Вычисляем правильность предсказаний
    df['pred_winner'] = (df['pred_team1_score'] > df['pred_team2_score']).astype(int)
    df['real_winner'] = (df['real_team1_score'] > df['real_team2_score']).astype(int)
    df['correct_winner'] = df['pred_winner'] == df['real_winner']
    df['exact_score'] = (df['pred_team1_score'] == df['real_team1_score']) & \
                       (df['pred_team2_score'] == df['real_team2_score'])
    
    # График точности по уровню confidence
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # Точность vs Confidence
    ax = axes[0, 0]
    confidence_bins = pd.cut(df['confidence'], bins=[0, 0.1, 0.2, 0.3, 0.4, 0.5, 1.0])
    accuracy_by_conf = df.groupby(confidence_bins)['correct_winner'].agg(['mean', 'count'])
    
    x = range(len(accuracy_by_conf))
    ax.bar(x, accuracy_by_conf['mean'], color='steelblue', alpha=0.7)
    ax.set_xticks(x)
    ax.set_xticklabels([str(i) for i in accuracy_by_conf.index], rotation=45)
    ax.set_ylabel('Точность предсказания победителя')
    ax.set_xlabel('Уровень уверенности (confidence)')
    ax.set_title('Точность vs Уверенность модели')
    ax.set_ylim(0, 1)
    
    # Добавляем линию с количеством матчей
    ax2 = ax.twinx()
    ax2.plot(x, accuracy_by_conf['count'], 'r-', marker='o', label='Количество матчей')
    ax2.set_ylabel('Количество матчей', color='r')
    ax2.tick_params(axis='y', labelcolor='r')
    
    # Распределение вероятностей
    ax = axes[0, 1]
    ax.hist(df['pred_team1_prob'], bins=20, alpha=0.5, label='Вероятность победы команды 1', color='blue')
    ax.hist(df['pred_team2_prob'], bins=20, alpha=0.5, label='Вероятность победы команды 2', color='red')
    ax.set_xlabel('Вероятность')
    ax.set_ylabel('Количество матчей')
    ax.set_title('Распределение предсказанных вероятностей')
    ax.legend()
    
    # Калибровочный график
    ax = axes[1, 0]
    prob_bins = pd.cut(df['pred_team1_prob'], bins=10)
    calibration = df.groupby(prob_bins)['real_winner'].agg(['mean', 'count'])
    calibration = calibration[calibration['count'] > 0]
    
    x_calib = [interval.mid for interval in calibration.index]
    ax.scatter(x_calib, 1 - calibration['mean'], s=calibration['count']*10, alpha=0.6, color='purple')
    ax.plot([0, 1], [0, 1], 'k--', label='Идеальная калибровка')
    ax.set_xlabel('Предсказанная вероятность победы команды 1')
    ax.set_ylabel('Реальная частота победы команды 1')
    ax.set_title('Калибровочный график')
    ax.legend()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    
    # Динамика точности по времени
    ax = axes[1, 1]
    df['date'] = pd.to_datetime(df['datetime'], unit='s')
    df['week'] = df['date'].dt.to_period('W')
    weekly_accuracy = df.groupby('week')['correct_winner'].agg(['mean', 'count'])
    weekly_accuracy = weekly_accuracy[weekly_accuracy['count'] > 5]  # Только недели с достаточным количеством матчей
    
    if not weekly_accuracy.empty:
        x_weeks = range(len(weekly_accuracy))
        ax.plot(x_weeks, weekly_accuracy['mean'], marker='o', linewidth=2, markersize=8, color='green')
        ax.fill_between(x_weeks, weekly_accuracy['mean'] - 0.05, weekly_accuracy['mean'] + 0.05, alpha=0.3, color='green')
        ax.set_xticks(x_weeks[::2])  # Показываем каждую вторую неделю
        ax.set_xticklabels([str(w) for w in weekly_accuracy.index[::2]], rotation=45)
        ax.set_ylabel('Точность предсказания победителя')
        ax.set_xlabel('Неделя')
        ax.set_title('Динамика точности по времени')
        ax.set_ylim(0.4, 1.0)
        ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PATH}/prediction_accuracy.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # Выводим общую статистику
    total_accuracy = df['correct_winner'].mean()
    exact_score_accuracy = df['exact_score'].mean()
    high_conf_accuracy = df[df['confidence'] > 0.3]['correct_winner'].mean()
    
    print(f"\n📊 Статистика точности (последние {len(df)} матчей):")
    print(f"Общая точность предсказания победителя: {total_accuracy:.2%}")
    print(f"Точность при высокой уверенности (>0.3): {high_conf_accuracy:.2%}")
    print(f"Точность угадывания точного счета: {exact_score_accuracy:.2%}")
    
    print(f"\nГрафик точности сохранен: {OUTPUT_PATH}/prediction_accuracy.png")

def create_summary_report():
    """Создание итогового отчета в HTML"""
    # Загружаем метрики
    metrics_file = f"{DIAGNOSTICS_PATH}/evaluation_metrics.json"
    
    if Path(metrics_file).exists():
        with open(metrics_file, 'r') as f:
            metrics = json.load(f)
    else:
        metrics = {}
    
    # Создаем HTML отчет
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>CS2 Match Predictor - Отчет</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background-color: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }}
            h1 {{
                color: #333;
                text-align: center;
            }}
            h2 {{
                color: #666;
                border-bottom: 2px solid #eee;
                padding-bottom: 10px;
            }}
            .metric {{
                display: inline-block;
                margin: 10px;
                padding: 15px;
                background-color: #f9f9f9;
                border-radius: 5px;
                min-width: 200px;
            }}
            .metric-value {{
                font-size: 24px;
                font-weight: bold;
                color: #2196F3;
            }}
            .metric-name {{
                font-size: 14px;
                color: #666;
            }}
            img {{
                max-width: 100%;
                margin: 20px 0;
                border: 1px solid #ddd;
                border-radius: 5px;
            }}
            .timestamp {{
                text-align: right;
                color: #999;
                font-size: 12px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🎮 CS2 Match Predictor - Отчет о производительности</h1>
            
            <div class="timestamp">
                Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
            
            <h2>📊 Основные метрики</h2>
            <div class="metrics">
    """
    
    # Добавляем метрики
    for metric_name, metric_value in metrics.items():
        if isinstance(metric_value, (int, float)):
            html_content += f"""
                <div class="metric">
                    <div class="metric-value">{metric_value:.4f}</div>
                    <div class="metric-name">{metric_name}</div>
                </div>
            """
    
    html_content += """
            </div>
            
            <h2>📈 Визуализации</h2>
    """
    
    # Добавляем графики
    visualizations = [
        ("feature_importance.png", "Важность признаков"),
        ("category_importance.png", "Важность категорий признаков"),
        ("model_comparison.png", "Сравнение моделей"),
        ("prediction_accuracy.png", "Точность предсказаний")
    ]
    
    for filename, title in visualizations:
        if Path(f"{OUTPUT_PATH}/{filename}").exists():
            html_content += f"""
            <h3>{title}</h3>
            <img src="visualizations/{filename}" alt="{title}">
            """
    
    html_content += """
        </div>
    </body>
    </html>
    """
    
    # Сохраняем отчет
    with open(f"{DIAGNOSTICS_PATH}/report.html", 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"\n📄 HTML отчет сохранен: {DIAGNOSTICS_PATH}/report.html")

def export_predict_table_html():
    """Экспорт таблицы predict с названиями команд и временем матча в красивый HTML с графиком"""
    import matplotlib.pyplot as plt
    import seaborn as sns
    import pandas as pd
    import sqlite3
    from pathlib import Path
    import numpy as np
    
    DB_PATH = 'hltv.db'
    OUTPUT_PATH = 'predictor/visualizations'
    Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    
    # Загружаем predict + названия команд и время матча
    conn = sqlite3.connect(DB_PATH)
    query = '''
    SELECT 
        p.match_id,
        p.team1_score,
        p.team2_score,
        p.team1_score_final,
        p.team2_score_final,
        p.confidence,
        p.model_version,
        p.last_updated,
        r.team1_id,
        r.team2_id,
        r.team1_name,
        r.team1_rank,
        r.team2_name,
        r.team2_rank,
        r.datetime,
        r.team1_score as real_team1_score,
        r.team2_score as real_team2_score
    FROM predict p
    LEFT JOIN result_match r ON p.match_id = r.match_id
    WHERE r.datetime IS NOT NULL
    ORDER BY r.datetime DESC
    '''
    df = pd.read_sql_query(query, conn)
    # Загружаем всю таблицу result_match для подсчёта количества матчей
    result_df = pd.read_sql_query('SELECT team1_id, team2_id FROM result_match', conn)
    conn.close()
    
    if df.empty:
        print("Нет данных для экспорта таблицы predict!")
        return
    
    # Преобразуем время
    df['date'] = pd.to_datetime(df['datetime'], unit='s')
    df = df.sort_values('date', ascending=False)
    
    # Объединяем ранг с названием
    def name_with_rank(name, rank):
        if pd.notnull(rank):
            return f"#{int(rank)} {name}"
        else:
            return name
    df['team1_name'] = df.apply(lambda row: name_with_rank(row['team1_name'], row['team1_rank']), axis=1)
    df['team2_name'] = df.apply(lambda row: name_with_rank(row['team2_name'], row['team2_rank']), axis=1)

    # Добавляем столбец с реальным счетом
    def format_real_score(row):
        if pd.notnull(row['real_team1_score']) and pd.notnull(row['real_team2_score']):
            return f"{int(row['real_team1_score'])}:{int(row['real_team2_score'])}"
        else:
            return '-'
    df['real_score'] = df.apply(format_real_score, axis=1)

    # Считаем количество матчей для каждой команды
    def count_matches(team_id):
        if pd.isnull(team_id):
            return 0
        return ((result_df['team1_id'] == team_id) | (result_df['team2_id'] == team_id)).sum()
    df['team1_matches_played'] = df['team1_id'].apply(count_matches)
    df['team2_matches_played'] = df['team2_id'].apply(count_matches)

    # Добавляем индикаторы количества данных
    def data_level(val):
        if pd.isnull(val):
            return 'нет данных'
        val = int(val)
        if val < 10:
            return 'мало'
        elif val < 20:
            return 'средне'
        else:
            return 'много'
    df['team1_data_level'] = df['team1_matches_played'].apply(data_level)
    df['team2_data_level'] = df['team2_matches_played'].apply(data_level)

    # Объединяем финальный счет
    df['final_score'] = df.apply(lambda row: f"{row['team1_score_final']}-{row['team2_score_final']}" if pd.notnull(row['team1_score_final']) and pd.notnull(row['team2_score_final']) else '-', axis=1)

    # Формируем нужный порядок столбцов
    columns = [
        'date',
        'match_id',
        'team1_name',
        'team2_name',
        'team1_score',
        'team2_score',
        'confidence',
        'final_score',
        'real_score',
        'team1_data_level',
        'team2_data_level',
        'team1_matches_played',
        'team2_matches_played',
        'last_updated'
    ]
    styled = df[columns].copy()
    styled['date'] = styled['date'].dt.strftime('%Y-%m-%d %H:%M')
    styled['team1_score'] = styled['team1_score'].map(lambda x: f"{float(x)*100:.1f}%" if pd.notnull(x) else '-')
    styled['team2_score'] = styled['team2_score'].map(lambda x: f"{float(x)*100:.1f}%" if pd.notnull(x) else '-')
    styled['confidence'] = styled['confidence'].map(lambda x: f"{float(x)*100:.1f}%" if pd.notnull(x) else '-')

    # Центрирование нужных столбцов
    center_cols = ['team1_score','team2_score','confidence','final_score','real_score']
    def center_style(_):
        return 'text-align: center;'

    def highlight_score(row):
        try:
            t1 = float(row['team1_score'].replace('%',''))
            t2 = float(row['team2_score'].replace('%',''))
        except:
            return ['','']
        if t1 > t2:
            return ['background-color: #b6fcb6', 'background-color: #fcb6b6']
        elif t1 < t2:
            return ['background-color: #fcb6b6', 'background-color: #b6fcb6']
        else:
            return ['background-color: #fffcb6', 'background-color: #fffcb6']

    def highlight_data_level(val):
        if val == 'мало':
            return 'background-color: #ffcccc'
        elif val == 'средне':
            return 'background-color: #fff7cc'
        elif val == 'много':
            return 'background-color: #ccffcc'
        else:
            return ''

    # Стилизация таблицы
    html_table = styled.style \
        .set_properties(**{'text-align': 'center'}, subset=center_cols) \
        .background_gradient(subset=['confidence'], cmap='YlGnBu') \
        .apply(highlight_score, axis=1, subset=['team1_score','team2_score']) \
        .map(highlight_data_level, subset=['team1_data_level','team2_data_level']) \
        .set_caption('Таблица предсказаний матчей CS2') \
        .set_table_styles([
            {'selector': 'th', 'props': [('background-color', '#222'), ('color', 'white')]},
            {'selector': 'caption', 'props': [('caption-side', 'top'), ('font-size', '18px'), ('font-weight', 'bold')]}
        ]) \
        .hide(axis='index') \
        .format({'team1_score': '{:.1%}', 'team2_score': '{:.1%}', 'confidence': '{:.1%}'}, na_rep='-')
    
    # Сохраняем HTML-таблицу
    html_path = f"{OUTPUT_PATH}/predict_table.html"
    html_table.to_html(html_path, encoding='utf-8')
    print(f"HTML-таблица предсказаний сохранена: {html_path}")
    
    # График: распределение confidence и вероятностей
    plt.figure(figsize=(12,6))
    sns.histplot(df['confidence'].astype(float), bins=20, kde=True, color='royalblue', label='Уверенность (confidence)')
    plt.xlabel('Уверенность модели (confidence)')
    plt.ylabel('Количество матчей')
    plt.title('Распределение уверенности модели в предсказаниях')
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PATH}/confidence_distribution.png", dpi=200, bbox_inches='tight')
    plt.close()
    print(f"График распределения confidence сохранен: {OUTPUT_PATH}/confidence_distribution.png")
    
    # График вероятностей победы
    plt.figure(figsize=(12,6))
    sns.histplot(df['team1_score'].astype(float), bins=20, kde=True, color='green', label='Вероятность победы Team 1', alpha=0.6)
    sns.histplot(df['team2_score'].astype(float), bins=20, kde=True, color='red', label='Вероятность победы Team 2', alpha=0.6)
    plt.xlabel('Вероятность победы')
    plt.ylabel('Количество матчей')
    plt.title('Распределение вероятностей победы команд')
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PATH}/win_prob_distribution.png", dpi=200, bbox_inches='tight')
    plt.close()
    print(f"График вероятностей победы сохранен: {OUTPUT_PATH}/win_prob_distribution.png")

def export_upcoming_predict_table_html():
    """Экспорт предсказаний для будущих матчей (upcoming_match + predict) в красивый HTML с графиками"""
    import matplotlib.pyplot as plt
    import seaborn as sns
    import pandas as pd
    import sqlite3
    from pathlib import Path
    import numpy as np
    
    DB_PATH = 'hltv.db'
    OUTPUT_PATH = 'predictor/visualizations'
    Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    
    # Загружаем предсказания для будущих матчей
    conn = sqlite3.connect(DB_PATH)
    query = '''
    SELECT
        p.match_id,
        u.team1_id,
        u.team2_id,
        u.team1_name,
        u.team1_rank,
        u.team2_name,
        u.team2_rank,
        u.datetime,
        p.team1_score,
        p.team2_score,
        p.team1_score_final,
        p.team2_score_final,
        p.confidence,
        p.model_version,
        p.last_updated
    FROM predict p
    LEFT JOIN upcoming_match u ON p.match_id = u.match_id
    WHERE u.match_id IS NOT NULL
    ORDER BY u.datetime ASC
    '''
    df = pd.read_sql_query(query, conn)
    # Загружаем всю таблицу result_match для подсчёта количества матчей
    result_df = pd.read_sql_query('SELECT team1_id, team2_id FROM result_match', conn)
    conn.close()
    
    if df.empty:
        print("Нет данных для экспорта предсказаний будущих матчей!")
        return
    
    # Преобразуем время
    df['date'] = pd.to_datetime(df['datetime'], unit='s')
    df = df.sort_values('date', ascending=True)
    
    # Объединяем ранг с названием
    def name_with_rank(name, rank):
        if pd.notnull(rank):
            return f"#{int(rank)} {name}"
        else:
            return name
    df['team1_name'] = df.apply(lambda row: name_with_rank(row['team1_name'], row['team1_rank']), axis=1)
    df['team2_name'] = df.apply(lambda row: name_with_rank(row['team2_name'], row['team2_rank']), axis=1)

    # Считаем количество матчей для каждой команды
    def count_matches(team_id):
        if pd.isnull(team_id):
            return 0
        return ((result_df['team1_id'] == team_id) | (result_df['team2_id'] == team_id)).sum()
    df['team1_matches_played'] = df['team1_id'].apply(count_matches)
    df['team2_matches_played'] = df['team2_id'].apply(count_matches)

    # Добавляем индикаторы количества данных
    def data_level(val):
        if pd.isnull(val):
            return 'нет данных'
        val = int(val)
        if val < 10:
            return 'мало'
        elif val < 20:
            return 'средне'
        else:
            return 'много'
    df['team1_data_level'] = df['team1_matches_played'].apply(data_level)
    df['team2_data_level'] = df['team2_matches_played'].apply(data_level)

    # Объединяем финальный счет
    df['final_score'] = df.apply(lambda row: f"{row['team1_score_final']}-{row['team2_score_final']}" if pd.notnull(row['team1_score_final']) and pd.notnull(row['team2_score_final']) else '-', axis=1)

    # Формируем нужный порядок столбцов
    columns = [
        'date',
        'match_id',
        'team1_name',
        'team2_name',
        'team1_score',
        'team2_score',
        'confidence',
        'final_score',
        'team1_data_level',
        'team2_data_level',
        'team1_matches_played',
        'team2_matches_played',
        'last_updated'
    ]
    styled = df[columns].copy()
    styled['date'] = styled['date'].dt.strftime('%Y-%m-%d %H:%M')
    styled['team1_score'] = styled['team1_score'].map(lambda x: f"{float(x)*100:.1f}%" if pd.notnull(x) else '-')
    styled['team2_score'] = styled['team2_score'].map(lambda x: f"{float(x)*100:.1f}%" if pd.notnull(x) else '-')
    styled['confidence'] = styled['confidence'].map(lambda x: f"{float(x)*100:.1f}%" if pd.notnull(x) else '-')

    # Центрирование нужных столбцов
    center_cols = ['team1_score','team2_score','confidence','final_score']
    def center_style(_):
        return 'text-align: center;'

    def highlight_score(row):
        try:
            t1 = float(row['team1_score'].replace('%',''))
            t2 = float(row['team2_score'].replace('%',''))
        except:
            return ['','']
        if t1 > t2:
            return ['background-color: #b6fcb6', 'background-color: #fcb6b6']
        elif t1 < t2:
            return ['background-color: #fcb6b6', 'background-color: #b6fcb6']
        else:
            return ['background-color: #fffcb6', 'background-color: #fffcb6']

    def highlight_data_level(val):
        if val == 'мало':
            return 'background-color: #ffcccc'
        elif val == 'средне':
            return 'background-color: #fff7cc'
        elif val == 'много':
            return 'background-color: #ccffcc'
        else:
            return ''

    # Стилизация таблицы
    html_table = styled.style \
        .set_properties(**{'text-align': 'center'}, subset=center_cols) \
        .background_gradient(subset=['confidence'], cmap='YlGnBu') \
        .apply(highlight_score, axis=1, subset=['team1_score','team2_score']) \
        .map(highlight_data_level, subset=['team1_data_level','team2_data_level']) \
        .set_caption('Таблица предсказаний будущих матчей CS2') \
        .set_table_styles([
            {'selector': 'th', 'props': [('background-color', '#222'), ('color', 'white')]},
            {'selector': 'caption', 'props': [('caption-side', 'top'), ('font-size', '18px'), ('font-weight', 'bold')]}
        ]) \
        .hide(axis='index') \
        .format({'team1_score': '{:.1%}', 'team2_score': '{:.1%}', 'confidence': '{:.1%}'}, na_rep='-')
    
    # Сохраняем HTML-таблицу
    html_path = f"{OUTPUT_PATH}/upcoming_predict_table.html"
    html_table.to_html(html_path, encoding='utf-8')
    print(f"HTML-таблица предсказаний будущих матчей сохранена: {html_path}")
    
    # График: распределение confidence
    plt.figure(figsize=(12,6))
    sns.histplot(df['confidence'].astype(float), bins=20, kde=True, color='royalblue', label='Уверенность (confidence)')
    plt.xlabel('Уверенность модели (confidence)')
    plt.ylabel('Количество матчей')
    plt.title('Распределение уверенности модели в предсказаниях (будущие матчи)')
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PATH}/upcoming_confidence_distribution.png", dpi=200, bbox_inches='tight')
    plt.close()
    print(f"График распределения confidence сохранен: {OUTPUT_PATH}/upcoming_confidence_distribution.png")
    
    # График вероятностей победы
    plt.figure(figsize=(12,6))
    sns.histplot(df['team1_score'].astype(float), bins=20, kde=True, color='green', label='Вероятность победы Team 1', alpha=0.6)
    sns.histplot(df['team2_score'].astype(float), bins=20, kde=True, color='red', label='Вероятность победы Team 2', alpha=0.6)
    plt.xlabel('Вероятность победы')
    plt.ylabel('Количество матчей')
    plt.title('Распределение вероятностей победы команд (будущие матчи)')
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_PATH}/upcoming_win_prob_distribution.png", dpi=200, bbox_inches='tight')
    plt.close()
    print(f"График вероятностей победы сохранен: {OUTPUT_PATH}/upcoming_win_prob_distribution.png")

def main():
    """Главная функция визуализации"""
    print("🎨 Создание визуализаций для CS2 Match Predictor...\n")
    
    # Создаем все визуализации
    visualize_feature_importance()
    visualize_model_comparison()
    visualize_prediction_accuracy()
    export_predict_table_html()
    export_upcoming_predict_table_html()
    create_summary_report()
    
    print("\n✅ Все визуализации созданы успешно!")

if __name__ == "__main__":
    main() 