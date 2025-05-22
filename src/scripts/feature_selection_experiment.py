import json
import pandas as pd
import numpy as np
from lightgbm import LGBMClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from src.scripts.predictor import Predictor
from sklearn.preprocessing import LabelEncoder

# Загрузка эталонного списка признаков
with open('storage/model_features_etalon.json', 'r') as f:
    features_etalon = json.load(f)

def is_map_feature(f):
    return 'map_' in f or '_map_' in f

def is_match_feature(f):
    return not is_map_feature(f)

agg_features_team1 = [f for f in features_etalon['team1'] if not f.startswith('t1_player')]
agg_features_team2 = [f for f in features_etalon['team2'] if not f.startswith('t2_player')]
all_features = agg_features_team1 + agg_features_team2

# Разделяем на матчевые и map-специфичные
match_features = [f for f in all_features if is_match_feature(f)]
map_features = [f for f in all_features if is_map_feature(f)]

# Загрузка данных
predictor = Predictor()
predictor.load_data()
predictor.feature_engineering(for_train=True)
features_df = predictor.features.copy()

# --- Для матчей ---
features_df['win_match'] = (features_df['team1_score'] > features_df['team2_score']).astype(int)

# --- Для карт: загружаем из базы ---
def get_map_targets_from_db(db_path='hltv.db'):
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query('SELECT * FROM result_match_maps', conn)
    # Целевая переменная
    df = df[df['team1_rounds'].notnull() & df['team2_rounds'].notnull()]
    df['win_map'] = (df['team1_rounds'] > df['team2_rounds']).astype(int)
    return df

features_df_map = get_map_targets_from_db()

# --- Эксперименты ---
results = []

def test_features(X, y, label):
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
    clf = LGBMClassifier(n_estimators=200)
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_val)
    acc = accuracy_score(y_val, y_pred)
    print(f'{label}: accuracy={acc:.4f}')
    return acc

# --- Матчи ---
print('\n=== Матчи ===')
acc_match_all = test_features(features_df[all_features], features_df['win_match'], 'Матчи: общий набор')
acc_match_match = test_features(features_df[match_features], features_df['win_match'], 'Матчи: только матчевые')
acc_match_map = test_features(features_df[map_features], features_df['win_match'], 'Матчи: только map-специфичные')
acc_match_combined = test_features(features_df[match_features + map_features], features_df['win_match'], 'Матчи: матчевые + map-специфичные')
results.append(('Матчи', 'общий', acc_match_all))
results.append(('Матчи', 'только матчевые', acc_match_match))
results.append(('Матчи', 'только map', acc_match_map))
results.append(('Матчи', 'оба', acc_match_combined))

# --- Диагностика наличия и содержимого team1_rounds, team2_rounds, win_map ---
print('Колонки features_df_map:', features_df_map.columns.tolist())
if 'team1_rounds' in features_df_map.columns:
    print('Примеры team1_rounds:', features_df_map['team1_rounds'].head())
else:
    print('team1_rounds отсутствует в features_df_map')
if 'team2_rounds' in features_df_map.columns:
    print('Примеры team2_rounds:', features_df_map['team2_rounds'].head())
else:
    print('team2_rounds отсутствует в features_df_map')
if 'win_map' in features_df_map.columns:
    print('Примеры win_map:', features_df_map['win_map'].head())
    print('win_map value_counts:', features_df_map['win_map'].value_counts(dropna=False))
else:
    print('win_map отсутствует в features_df_map')

# --- Диагностика валидных признаков для карт ---
# 1. Оставляем только признаки, которые есть в данных и не всегда NaN
valid_features = [f for f in all_features if f in features_df_map.columns and features_df_map[f].notnull().any()]
print('Валидные признаки для карт:', valid_features)
print(f'Всего валидных признаков: {len(valid_features)}')

# 2. Проверяем, сколько строк останется после очистки только по этим признакам
features_df_map_clean = features_df_map.dropna(subset=valid_features + ['win_map'])
print(f'Число строк после очистки для карт (валидные признаки): {len(features_df_map_clean)}')

# 3. Если строк всё равно нет, пробуем только map-специфичные признаки
valid_map_features = [f for f in map_features if f in features_df_map.columns and features_df_map[f].notnull().any()]
print('Валидные map-специфичные признаки:', valid_map_features)
print(f'Всего валидных map-признаков: {len(valid_map_features)}')
features_df_map_clean_map = features_df_map.dropna(subset=valid_map_features + ['win_map'])
print(f'Число строк после очистки для карт (только map-признаки): {len(features_df_map_clean_map)}')

# --- Эксперимент для карт: базовые признаки + map_name ---
map_base_features = ['team1_rounds', 'team2_rounds', 'map_name']
features_df_map_clean = features_df_map.dropna(subset=map_base_features + ['win_map'])

# Кодируем map_name
le = LabelEncoder()
features_df_map_clean['map_name'] = le.fit_transform(features_df_map_clean['map_name'])

if len(features_df_map_clean) == 0:
    print('Нет данных для обучения на картах — проверь формирование features_df_map и целевой переменной win_map!')
else:
    acc_map_base = test_features(features_df_map_clean[map_base_features], features_df_map_clean['win_map'], 'Карты: базовые + map_name')
    results.append(('Карты', 'базовые+map_name', acc_map_base))

# --- Честный feature engineering для карт ---
def build_honest_map_features():
    from src.scripts.predictor import Predictor
    import sqlite3
    predictor = Predictor()
    predictor.load_data()
    predictor.feature_engineering(for_train=True)
    # Загружаем карты
    with sqlite3.connect('hltv.db') as conn:
        maps_df = pd.read_sql_query('SELECT * FROM result_match_maps', conn)
    # Загружаем матчи
    with sqlite3.connect('hltv.db') as conn:
        matches_df = pd.read_sql_query('SELECT * FROM result_match', conn)
    features = []
    for idx, row in maps_df.iterrows():
        match_id = row['match_id']
        map_name = row['map_name']
        # Находим матч
        match_row = matches_df[matches_df['match_id'] == match_id]
        if match_row.empty:
            continue
        match_series = match_row.iloc[0]
        match_series = match_series.copy()
        match_series['map_name'] = map_name
        # Получаем игроков (если есть)
        t1_id = row['team1_id']
        t2_id = row['team2_id']
        t1_players = predictor.players_stats[(predictor.players_stats['match_id'] == match_id) & (predictor.players_stats['team_id'] == t1_id)]['player_id'].tolist()
        t2_players = predictor.players_stats[(predictor.players_stats['match_id'] == match_id) & (predictor.players_stats['team_id'] == t2_id)]['player_id'].tolist()
        # Формируем признаки только по истории до этой карты
        feats = predictor.get_common_features(match_series, t1_players, t2_players)
        feats['map_name'] = map_name
        feats['win_map'] = int(row['team1_rounds'] > row['team2_rounds'])
        features.append(feats)
    return pd.DataFrame(features)

# --- Формируем честные признаки для карт ---
features_df_map_honest = build_honest_map_features()

# --- Эксперимент: честный прогноз по картам ---
map_honest_features = [f for f in features_df_map_honest.columns if f not in ['win_map', 'map_name']]
print('Честные признаки для обучения по картам:', map_honest_features)
features_df_map_honest_clean = features_df_map_honest.dropna(subset=map_honest_features + ['win_map'])

if len(features_df_map_honest_clean) == 0:
    print('Нет данных для честного обучения на картах!')
else:
    acc_map_honest = test_features(features_df_map_honest_clean[map_honest_features], features_df_map_honest_clean['win_map'], 'Карты: честные признаки')
    results.append(('Карты', 'честные', acc_map_honest))

# --- Сравнительная таблица ---
print('\n=== Сравнительная таблица accuracy ===')
print(f"{'Тип':<8} | {'Набор признаков':<20} | {'Accuracy':<8}")
print('-'*44)
for t, label, acc in results:
    print(f"{t:<8} | {label:<20} | {acc:.4f}")

# --- Вывод и сохранение честных признаков для карт ---
with open('honest_map_features.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(map_honest_features))
print('\nЧестные признаки для обучения по картам:')
for feat in map_honest_features:
    print(feat)
print('\n(Полный список сохранён в honest_map_features.txt)') 