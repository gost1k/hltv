import json
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler
from src.scripts.predictor import Predictor

# Загрузка эталонного списка признаков
with open('storage/model_features_etalon.json', 'r') as f:
    features_etalon = json.load(f)
all_features = features_etalon['team1'] + features_etalon['team2']

# Загрузка данных
predictor = Predictor()
predictor.load_data()
predictor.feature_engineering(for_train=True)
features_df = predictor.features.copy()

# Целевая переменная: победитель
features_df['win'] = (features_df['team1_score'] > features_df['team2_score']).astype(int)

X = features_df[all_features].fillna(0)
y = features_df['win']
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# Нормализация признаков
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_val_scaled = scaler.transform(X_val)

# Обучение MLP
mlp = MLPClassifier(hidden_layer_sizes=(128, 64), max_iter=200, random_state=42)
mlp.fit(X_train_scaled, y_train)
y_pred = mlp.predict(X_val_scaled)
mlp_acc = accuracy_score(y_val, y_pred)
print(f"MLP accuracy: {mlp_acc:.4f}")

# Для сравнения: LightGBM
try:
    from lightgbm import LGBMClassifier
    lgbm = LGBMClassifier(n_estimators=200)
    lgbm.fit(X_train, y_train)
    y_pred_lgbm = lgbm.predict(X_val)
    lgbm_acc = accuracy_score(y_val, y_pred_lgbm)
    print(f"LightGBM accuracy: {lgbm_acc:.4f}")
except ImportError:
    print("LightGBM не установлен для сравнения.") 