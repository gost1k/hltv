import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Пути
PRED_PATH = 'predicted_past_maps.csv'
DB_PATH = 'hltv.db'

# Загрузка предсказаний
pred = pd.read_csv(PRED_PATH)

# Загрузка информации о матчах и командах
with sqlite3.connect(DB_PATH) as conn:
    match_info = pd.read_sql_query('SELECT match_id, team1_id, team1_name, team1_rank, team2_id, team2_name, team2_rank, event_id, event_name FROM result_match', conn)
    map_info = pd.read_sql_query('SELECT match_id, map_name, team1_id, team2_id FROM result_match_maps', conn)

# Объединяем с инфой о командах и турнире
pred = pred.merge(map_info, on=['match_id', 'map_name'], how='left', suffixes=('', '_map'))
pred = pred.merge(match_info, on='match_id', how='left', suffixes=('', '_match'))

# Определяем победителя по предсказанию и по факту
pred['pred_winner'] = (pred['team1_pred'] > pred['team2_pred']).astype(int)
pred['real_winner'] = (pred['team1_real'] > pred['team2_real']).astype(int)
pred['is_error'] = (pred['pred_winner'] != pred['real_winner'])
pred['mae'] = np.abs(pred['team1_pred'] - pred['team1_real']) + np.abs(pred['team2_pred'] - pred['team2_real'])

# Признак топ-команды (top-10)
pred['t1_is_top'] = pred['team1_rank'] <= 10
pred['t2_is_top'] = pred['team2_rank'] <= 10

# Анализ ошибок по топ-командам
print('Ошибка для матчей с участием топ-1:', pred.groupby('t1_is_top')['is_error'].mean())
print('Ошибка для матчей с участием топ-2:', pred.groupby('t2_is_top')['is_error'].mean())

# Ошибки по турнирам
tournament_errors = pred.groupby('event_name')['is_error'].mean().sort_values(ascending=False)
print('Топ турниров по ошибкам:\n', tournament_errors.head(10))

# Ошибки по командам
team1_errors = pred.groupby('team1_name')['is_error'].mean().sort_values(ascending=False).head(20)
team2_errors = pred.groupby('team2_name')['is_error'].mean().sort_values(ascending=False).head(20)

# Визуализация: ошибки по командам
plt.figure(figsize=(12,5))
team1_errors.plot(kind='bar', color='red', alpha=0.7, label='team1')
plt.title('Ошибка по командам (team1, топ-20)')
plt.ylabel('Доля ошибок')
plt.tight_layout()
plt.savefig('team1_error_bar.png')
plt.close()

plt.figure(figsize=(12,5))
team2_errors.plot(kind='bar', color='blue', alpha=0.7, label='team2')
plt.title('Ошибка по командам (team2, топ-20)')
plt.ylabel('Доля ошибок')
plt.tight_layout()
plt.savefig('team2_error_bar.png')
plt.close()

# Визуализация: ошибки по турнирам
plt.figure(figsize=(12,5))
tournament_errors.head(20).plot(kind='bar', color='purple', alpha=0.7)
plt.title('Ошибка по турнирам (топ-20)')
plt.ylabel('Доля ошибок')
plt.tight_layout()
plt.savefig('tournament_error_bar.png')
plt.close()

# Heatmap ошибок по парам команд
pair_errors = pred.groupby(['team1_name', 'team2_name'])['is_error'].mean().unstack().fillna(0)
plt.figure(figsize=(16,12))
sns.heatmap(pair_errors, cmap='Reds', linewidths=0.5)
plt.title('Heatmap ошибок по парам команд')
plt.tight_layout()
plt.savefig('pair_error_heatmap.png')
plt.close()

# MAE по командам
mae_team1 = pred.groupby('team1_name')['mae'].mean().sort_values(ascending=False).head(20)
plt.figure(figsize=(12,5))
mae_team1.plot(kind='bar', color='orange', alpha=0.7)
plt.title('MAE по командам (team1, топ-20)')
plt.ylabel('MAE')
plt.tight_layout()
plt.savefig('team1_mae_bar.png')
plt.close()

# --- Анализ ошибок по новым составам (lineup_stability) ---
if 't1_lineup_stability' in pred.columns and 't2_lineup_stability' in pred.columns:
    pred['t1_new_lineup'] = pred['t1_lineup_stability'] <= 2  # можно скорректировать порог
    pred['t2_new_lineup'] = pred['t2_lineup_stability'] <= 2
    print('Ошибка для новых составов (team1):', pred.groupby('t1_new_lineup')['is_error'].mean())
    print('Ошибка для новых составов (team2):', pred.groupby('t2_new_lineup')['is_error'].mean())
    # Визуализация
    plt.figure(figsize=(6,4))
    pred.groupby('t1_new_lineup')['is_error'].mean().plot(kind='bar', color='green', alpha=0.7)
    plt.title('Ошибка для новых составов (team1)')
    plt.ylabel('Доля ошибок')
    plt.tight_layout()
    plt.savefig('new_lineup_team1_error_bar.png')
    plt.close()
    plt.figure(figsize=(6,4))
    pred.groupby('t2_new_lineup')['is_error'].mean().plot(kind='bar', color='cyan', alpha=0.7)
    plt.title('Ошибка для новых составов (team2)')
    plt.ylabel('Доля ошибок')
    plt.tight_layout()
    plt.savefig('new_lineup_team2_error_bar.png')
    plt.close()

# --- MAE по team2 ---
mae_team2 = pred.groupby('team2_name')['mae'].mean().sort_values(ascending=False).head(20)
plt.figure(figsize=(12,5))
mae_team2.plot(kind='bar', color='blue', alpha=0.7)
plt.title('MAE по командам (team2, топ-20)')
plt.ylabel('MAE')
plt.tight_layout()
plt.savefig('team2_mae_bar.png')
plt.close()

# --- MAE по турнирам ---
mae_tournament = pred.groupby('event_name')['mae'].mean().sort_values(ascending=False)
plt.figure(figsize=(12,5))
mae_tournament.head(20).plot(kind='bar', color='magenta', alpha=0.7)
plt.title('MAE по турнирам (топ-20)')
plt.ylabel('MAE')
plt.tight_layout()
plt.savefig('tournament_mae_bar.png')
plt.close()

# --- MAE по парам команд ---
pair_mae = pred.groupby(['team1_name', 'team2_name'])['mae'].mean().unstack().fillna(0)
plt.figure(figsize=(16,12))
sns.heatmap(pair_mae, cmap='Blues', linewidths=0.5)
plt.title('Heatmap MAE по парам команд')
plt.tight_layout()
plt.savefig('pair_mae_heatmap.png')
plt.close()

# --- Анализ ошибок и MAE по confidence ---
if 'confidence' in pred.columns:
    bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    pred['conf_bin'] = pd.cut(pred['confidence'], bins)
    conf_error = pred.groupby('conf_bin')['is_error'].mean()
    conf_mae = pred.groupby('conf_bin')['mae'].mean()
    print('Ошибка по confidence bin:', conf_error)
    print('MAE по confidence bin:', conf_mae)
    plt.figure(figsize=(10,5))
    conf_error.plot(kind='bar', color='brown', alpha=0.7)
    plt.title('Ошибка по confidence')
    plt.ylabel('Доля ошибок')
    plt.tight_layout()
    plt.savefig('confidence_error_bar.png')
    plt.close()
    plt.figure(figsize=(10,5))
    conf_mae.plot(kind='bar', color='gray', alpha=0.7)
    plt.title('MAE по confidence')
    plt.ylabel('MAE')
    plt.tight_layout()
    plt.savefig('confidence_mae_bar.png')
    plt.close()

# Summary
print('Общая точность (accuracy):', 1 - pred['is_error'].mean())
print('Средний MAE:', pred['mae'].mean())
print('Сохранены графики: team1_error_bar.png, team2_error_bar.png, tournament_error_bar.png, pair_error_heatmap.png, team1_mae_bar.png, team2_mae_bar.png, tournament_mae_bar.png, pair_mae_heatmap.png, new_lineup_team1_error_bar.png, new_lineup_team2_error_bar.png, confidence_error_bar.png, confidence_mae_bar.png')

# --- Текстовый отчет для передачи ассистенту ---
report = []
report.append('==== ОТЧЕТ ПО ОШИБКАМ МОДЕЛИ ====')
report.append(f'Общая точность (accuracy): {1 - pred["is_error"].mean():.3f}')
report.append(f'Средний MAE: {pred["mae"].mean():.3f}')

# Ошибки по топ-командам
report.append('\n-- Ошибка для матчей с участием топ-1:')
report.append(str(pred.groupby('t1_is_top')['is_error'].mean()))
report.append('-- Ошибка для матчей с участием топ-2:')
report.append(str(pred.groupby('t2_is_top')['is_error'].mean()))

# Ошибки по турнирам (топ-10)
report.append('\n-- Топ турниров по ошибкам:')
report.append(str(tournament_errors.head(10)))

# Ошибки по командам (топ-10)
report.append('\n-- Топ team1 по ошибкам:')
report.append(str(team1_errors.head(10)))
report.append('-- Топ team2 по ошибкам:')
report.append(str(team2_errors.head(10)))

# Ошибки по парам команд (топ-10)
pair_errors_flat = pair_errors.stack().sort_values(ascending=False)
report.append('\n-- Топ пар команд по ошибкам:')
report.append(str(pair_errors_flat.head(10)))

# Ошибки по новым составам
if 't1_lineup_stability' in pred.columns and 't2_lineup_stability' in pred.columns:
    report.append('\n-- Ошибка для новых составов (team1):')
    report.append(str(pred.groupby('t1_new_lineup')['is_error'].mean()))
    report.append('-- Ошибка для новых составов (team2):')
    report.append(str(pred.groupby('t2_new_lineup')['is_error'].mean()))

# Ошибки и MAE по confidence
if 'confidence' in pred.columns:
    report.append('\n-- Ошибка по confidence bin:')
    report.append(str(conf_error))
    report.append('-- MAE по confidence bin:')
    report.append(str(conf_mae))

# MAE по командам (топ-10)
report.append('\n-- Топ team1 по MAE:')
report.append(str(mae_team1.head(10)))
report.append('-- Топ team2 по MAE:')
report.append(str(mae_team2.head(10)))

# MAE по турнирам (топ-10)
report.append('\n-- Топ турниров по MAE:')
report.append(str(mae_tournament.head(10)))

# MAE по парам команд (топ-10)
pair_mae_flat = pair_mae.stack().sort_values(ascending=False)
report.append('\n-- Топ пар команд по MAE:')
report.append(str(pair_mae_flat.head(10)))

print('\n'.join(report))

# --- Анализ эталонных и провальных матчей ---
# Порог для MAE можно скорректировать
mae_good = 10
mae_bad = 20

# Эталон: правильный победитель и малая ошибка
pred['is_etalon'] = (~pred['is_error']) & (pred['mae'] < mae_good)
# Провал: неправильный победитель или большая ошибка
pred['is_fail'] = (pred['is_error']) | (pred['mae'] > mae_bad)

# Список параметров для анализа
params = ['confidence', 't1_lineup_stability', 't2_lineup_stability', 'team1_rank', 'team2_rank', 't1_matches_30d', 't2_matches_30d']
params = [p for p in params if p in pred.columns]

etalon_stats = pred.loc[pred['is_etalon'], params].mean()
fail_stats = pred.loc[pred['is_fail'], params].mean()
diff_stats = etalon_stats - fail_stats

print('\n==== Сравнение эталонных и провальных матчей ====' )
print('Параметр         | Эталон | Провал | Разница (эталон - провал)')
for p in params:
    print(f'{p:17} | {etalon_stats[p]:7.3f} | {fail_stats[p]:7.3f} | {diff_stats[p]:+7.3f}')

print(f'Эталонных матчей: {pred["is_etalon"].sum()}')
print(f'Провальных матчей: {pred["is_fail"].sum()}')

# --- Новый расчет confidence ---
# Для объема данных используем сумму карт, матчей и статистик по каждой команде (как в predictor.py)
with sqlite3.connect(DB_PATH) as conn:
    maps = pd.read_sql_query('SELECT team1_id, team2_id FROM result_match_maps', conn)
    matches = pd.read_sql_query('SELECT team1_id, team2_id FROM result_match', conn)
    stats = pd.read_sql_query('SELECT team_id FROM player_stats', conn)

team_ids = pd.concat([maps['team1_id'], maps['team2_id'], matches['team1_id'], matches['team2_id'], stats['team_id']]).unique()
team_data_volume = {}
max_data_volume = 1
for tid in team_ids:
    maps_count = ((maps['team1_id'] == tid) | (maps['team2_id'] == tid)).sum()
    matches_count = ((matches['team1_id'] == tid) | (matches['team2_id'] == tid)).sum()
    stats_count = (stats['team_id'] == tid).sum()
    volume = maps_count + matches_count + stats_count
    team_data_volume[tid] = volume
    if volume > max_data_volume:
        max_data_volume = volume

def calc_new_confidence(row):
    t1_vol = team_data_volume.get(row['team1_id'], 1)
    t2_vol = team_data_volume.get(row['team2_id'], 1)
    conf = min(t1_vol, t2_vol) / max_data_volume
    # Штраф за топ-команды
    if row.get('team1_rank', 100) < 30 or row.get('team2_rank', 100) < 30:
        conf *= 0.7
    # Штраф за мало матчей за 30 дней
    if row.get('t1_matches_30d', 5) < 3 or row.get('t2_matches_30d', 5) < 3:
        conf *= 0.8
    # Штраф за нестабильный состав
    if row.get('t1_lineup_stability', 5) < 2 or row.get('t2_lineup_stability', 5) < 2:
        conf *= 0.7
    return round(conf, 3)

pred['new_confidence'] = pred.apply(calc_new_confidence, axis=1)

# Анализ ошибок и MAE по new_confidence
bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
pred['new_conf_bin'] = pd.cut(pred['new_confidence'], bins)
new_conf_error = pred.groupby('new_conf_bin')['is_error'].mean()
new_conf_mae = pred.groupby('new_conf_bin')['mae'].mean()

print('\n==== Анализ по new_confidence ====' )
print('-- Ошибка по new_confidence bin:')
print(new_conf_error)
print('-- MAE по new_confidence bin:')
print(new_conf_mae)

# Сравнение средних confident/MAE для эталонных и провальных матчей
etalon_new_conf = pred.loc[pred['is_etalon'], 'new_confidence'].mean()
fail_new_conf = pred.loc[pred['is_fail'], 'new_confidence'].mean()
print(f'Средний new_confidence для эталонных: {etalon_new_conf:.3f}')
print(f'Средний new_confidence для провальных: {fail_new_conf:.3f}')

# --- Гибридный confidence ---
# Порог топ-команды: rank < 20
TOP_RANK = 20
# Топ-5 турниров и пар по ошибке
high_error_tournaments = set(tournament_errors.head(5).index)
high_error_pairs = set(pair_errors.stack().sort_values(ascending=False).head(5).index)

def hybrid_confidence(row):
    t1_vol = team_data_volume.get(row['team1_id'], 1)
    t2_vol = team_data_volume.get(row['team2_id'], 1)
    conf = min(t1_vol, t2_vol) / max_data_volume
    # Сильное понижение для матчей топ-20 vs топ-20
    if row.get('team1_rank', 100) < TOP_RANK and row.get('team2_rank', 100) < TOP_RANK:
        conf *= 0.3
    elif row.get('team1_rank', 100) < TOP_RANK or row.get('team2_rank', 100) < TOP_RANK:
        conf *= 0.5
    # Понижение для турниров с высокой ошибкой
    if row.get('event_name', None) in high_error_tournaments:
        conf *= 0.5
    # Понижение для пар команд с высокой ошибкой
    if (row.get('team1_name', None), row.get('team2_name', None)) in high_error_pairs:
        conf *= 0.5
    # Понижение за мало матчей/нестабильный состав
    if row.get('t1_matches_30d', 5) < 3 or row.get('t2_matches_30d', 5) < 3:
        conf *= 0.8
    if row.get('t1_lineup_stability', 5) < 2 or row.get('t2_lineup_stability', 5) < 2:
        conf *= 0.7
    return round(conf, 3)

pred['hybrid_confidence'] = pred.apply(hybrid_confidence, axis=1)

# Анализ ошибок и MAE по hybrid_confidence
pred['hybrid_conf_bin'] = pd.cut(pred['hybrid_confidence'], bins)
hybrid_conf_error = pred.groupby('hybrid_conf_bin')['is_error'].mean()
hybrid_conf_mae = pred.groupby('hybrid_conf_bin')['mae'].mean()

print('\n==== Анализ по hybrid_confidence ====' )
print('-- Ошибка по hybrid_confidence bin:')
print(hybrid_conf_error)
print('-- MAE по hybrid_confidence bin:')
print(hybrid_conf_mae)

# Сравнение средних confident/MAE для эталонных и провальных матчей
etalon_hybrid_conf = pred.loc[pred['is_etalon'], 'hybrid_confidence'].mean()
fail_hybrid_conf = pred.loc[pred['is_fail'], 'hybrid_confidence'].mean()
print(f'Средний hybrid_confidence для эталонных: {etalon_hybrid_conf:.3f}')
print(f'Средний hybrid_confidence для провальных: {fail_hybrid_conf:.3f}')

# --- Advanced confidence ---
def advanced_confidence(row):
    t1_vol = team_data_volume.get(row['team1_id'], 1)
    t2_vol = team_data_volume.get(row['team2_id'], 1)
    conf = min(t1_vol, t2_vol) / max_data_volume
    # Penalty for top teams
    if row.get('team1_rank', 100) < TOP_RANK and row.get('team2_rank', 100) < TOP_RANK:
        conf *= 0.3
    elif row.get('team1_rank', 100) < TOP_RANK or row.get('team2_rank', 100) < TOP_RANK:
        conf *= 0.5
    # Penalty for high-error tournaments/pairs
    if row.get('event_name', None) in high_error_tournaments:
        conf *= 0.5
    if (row.get('team1_name', None), row.get('team2_name', None)) in high_error_pairs:
        conf *= 0.5
    # Penalty for few matches/unstable lineup
    if row.get('t1_matches_30d', 5) < 3 or row.get('t2_matches_30d', 5) < 3:
        conf *= 0.8
    if row.get('t1_lineup_stability', 5) < 2 or row.get('t2_lineup_stability', 5) < 2:
        conf *= 0.7
    # Penalty for missing important features
    important_features = ['t1_agg_rating', 't2_agg_rating', 't1_winrate_last3', 't2_winrate_last3']
    missing = sum(pd.isnull(row.get(f, None)) or row.get(f, 0) == 0 for f in important_features)
    if missing > 0:
        conf *= (1 - 0.1 * missing)
    return round(conf, 3)

pred['advanced_confidence'] = pred.apply(advanced_confidence, axis=1)
pred['advanced_conf_bin'] = pd.cut(pred['advanced_confidence'], bins)
advanced_conf_error = pred.groupby('advanced_conf_bin')['is_error'].mean()
advanced_conf_mae = pred.groupby('advanced_conf_bin')['mae'].mean()

print('\n==== Анализ по advanced_confidence ====' )
print('-- Ошибка по advanced_confidence bin:')
print(advanced_conf_error)
print('-- MAE по advanced_confidence bin:')
print(advanced_conf_mae)
etalon_advanced_conf = pred.loc[pred['is_etalon'], 'advanced_confidence'].mean()
fail_advanced_conf = pred.loc[pred['is_fail'], 'advanced_confidence'].mean()
print(f'Средний advanced_confidence для эталонных: {etalon_advanced_conf:.3f}')
print(f'Средний advanced_confidence для провальных: {fail_advanced_conf:.3f}')

# --- Классификация турниров по среднему рангу команд ---
with sqlite3.connect(DB_PATH) as conn:
    event_ranks = pd.read_sql_query(
        '''
        SELECT event_id, event_name, team1_rank, team2_rank
        FROM result_match
        WHERE team1_rank IS NOT NULL AND team2_rank IS NOT NULL
        ''', conn
    )
event_ranks['team1_rank'] = pd.to_numeric(event_ranks['team1_rank'], errors='coerce')
event_ranks['team2_rank'] = pd.to_numeric(event_ranks['team2_rank'], errors='coerce')
event_ranks['all_ranks'] = event_ranks[['team1_rank', 'team2_rank']].values.tolist()
event_grouped = event_ranks.groupby(['event_id', 'event_name'])['all_ranks'].sum().reset_index()
event_grouped['mean_rank'] = event_grouped['all_ranks'].apply(lambda x: np.mean([r for r in x if not pd.isnull(r)]))
def classify_importance(mean_rank):
    if mean_rank <= 30:
        return 'top'
    elif mean_rank <= 100:
        return 'mid'
    else:
        return 'low'
event_grouped['tournament_importance'] = event_grouped['mean_rank'].apply(classify_importance)
# Словарь event_id -> importance
event_importance = dict(zip(event_grouped['event_id'], event_grouped['tournament_importance']))
# Добавляем признак в pred
if 'event_id' in pred.columns:
    pred['tournament_importance'] = pred['event_id'].map(event_importance)
else:
    pred['tournament_importance'] = None
# Анализ ошибок по категориям турниров
if 'tournament_importance' in pred.columns:
    print('\n==== Ошибки по категориям турниров (по среднему рангу) ====')
    print(pred.groupby('tournament_importance')['is_error'].mean())
    print(pred.groupby('tournament_importance')['mae'].mean())

# --- Анализ confidence и качества по категориям турниров ---
for conf_col in ['confidence', 'new_confidence', 'hybrid_confidence', 'advanced_confidence']:
    if conf_col in pred.columns:
        print(f"\n==== Анализ {conf_col} по категориям турниров ====")
        print('Средний confidence:')
        print(pred.groupby('tournament_importance')[conf_col].mean())
        print('Accuracy по категории:')
        print(1 - pred.groupby('tournament_importance')['is_error'].mean())
        print('MAE по категории:')
        print(pred.groupby('tournament_importance')['mae'].mean()) 