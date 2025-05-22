import json
import pandas as pd
import numpy as np
from lightgbm import LGBMClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from src.scripts.predictor import Predictor

# Загрузка эталонного списка признаков
with open('storage/model_features_etalon.json', 'r') as f:
    features_etalon = json.load(f)

def is_agg_feature(f):
    # Оставляем только агрегаты и командные признаки, исключая playerN_*
    return not any([
        f.startswith(f't1_player') or f.startswith(f't2_player')
    ])

agg_features_team1 = [f for f in features_etalon['team1'] if is_agg_feature(f)]
agg_features_team2 = [f for f in features_etalon['team2'] if is_agg_feature(f)]
all_features = agg_features_team1 + agg_features_team2

# Загрузка данных
predictor = Predictor()
predictor.load_data()
predictor.feature_engineering(for_train=True)
features_df = predictor.features.copy()

# Целевая переменная: победитель
features_df['win'] = (features_df['team1_score'] > features_df['team2_score']).astype(int)

# Базовый accuracy на полном наборе
X = features_df[all_features]
y = features_df['win']
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
clf = LGBMClassifier(n_estimators=200)
clf.fit(X_train, y_train)
y_pred = clf.predict(X_val)
baseline_acc = accuracy_score(y_val, y_pred)
print(f'Базовый accuracy (агрегированные признаки, LightGBM): {baseline_acc:.4f}')

# Backward: удаляем по одному признаку
results_remove = []
for f in all_features:
    subset = [x for x in all_features if x != f]
    X = features_df[subset]
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
    clf = LGBMClassifier(n_estimators=200)
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_val)
    acc = accuracy_score(y_val, y_pred)
    results_remove.append((f, acc))
    print(f'Без {f}: accuracy={acc:.4f}')

results_remove.sort(key=lambda x: x[1], reverse=True)
print('\nТоп признаков, удаление которых повышает accuracy:')
for f, acc in results_remove[:10]:
    print(f'{f}: {acc:.4f}')
print('\nТоп признаков, удаление которых понижает accuracy:')
for f, acc in results_remove[-10:]:
    print(f'{f}: {acc:.4f}')

# Forward: добавляем по одному признаку к пустому набору
results_add = []
for f in all_features:
    X = features_df[[f]]
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
    clf = LGBMClassifier(n_estimators=200)
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_val)
    acc = accuracy_score(y_val, y_pred)
    results_add.append((f, acc))
    print(f'Только {f}: accuracy={acc:.4f}')

results_add.sort(key=lambda x: x[1], reverse=True)
print('\nТоп одиночных признаков по accuracy:')
for f, acc in results_add[:10]:
    print(f'{f}: {acc:.4f}')

# --- Greedy Forward Selection ---
print('\n=== Greedy Forward Selection (автоматический подбор лучшего набора, LightGBM, только агрегаты) ===')
selected = []
remaining = all_features.copy()
best_acc = 0
history = []

while remaining:
    best_feature = None
    best_feature_acc = best_acc
    for f in remaining:
        candidate = selected + [f]
        X = features_df[candidate]
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
        clf = LGBMClassifier(n_estimators=200)
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_val)
        acc = accuracy_score(y_val, y_pred)
        if acc > best_feature_acc:
            best_feature_acc = acc
            best_feature = f
    if best_feature is not None and best_feature_acc > best_acc:
        selected.append(best_feature)
        remaining.remove(best_feature)
        best_acc = best_feature_acc
        history.append((selected.copy(), best_acc))
        print(f'Добавлен {best_feature}: accuracy={best_acc:.4f} (всего {len(selected)})')
    else:
        break

print('\nЛучший найденный набор признаков (LightGBM, только агрегаты):')
print(selected)
print(f'Accuracy на лучшем наборе: {best_acc:.4f}') 