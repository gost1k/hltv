import joblib
import json
import os

MODEL_PATH = 'storage/model_predictor.pkl'
FEATURES_PATH = 'storage/model_features.json'
MODEL_MAP_PATH = 'storage/model_predict_map.pkl'
FEATURES_MAP_PATH = 'storage/model_features_map.json'


def print_top_features(model_path, features_path, title):
    model1, model2, _ = joblib.load(model_path)
    with open(features_path, 'r') as f:
        features = json.load(f)
    features = list(dict.fromkeys(features))
    importances1 = model1.feature_importances_
    importances2 = model2.feature_importances_
    # Топ-20 для каждой
    top1 = sorted(zip(features, importances1), key=lambda x: -x[1])[:20]
    top2 = sorted(zip(features, importances2), key=lambda x: -x[1])[:20]
    print(f"\n{title}")
    print(f"{'team1_feature':30s} {'importance':>10s} | {'team2_feature':30s} {'importance':>10s}")
    print("-" * 86)
    for (f1, i1), (f2, i2) in zip(top1, top2):
        print(f"{f1:30s} {i1:10.2f} | {f2:30s} {i2:10.2f}")

if __name__ == "__main__":
    print_top_features(MODEL_PATH, FEATURES_PATH, 'Топ-20 признаков для матча')
    if os.path.exists(MODEL_MAP_PATH) and os.path.exists(FEATURES_MAP_PATH):
        print_top_features(MODEL_MAP_PATH, FEATURES_MAP_PATH, 'Топ-20 признаков для predict_map (карты)')
    else:
        print("\nОтдельной модели для predict_map не найдено — используются те же признаки, что и для матчей.") 