import joblib
import json
import numpy as np

MODEL_PATH = 'storage/model_predictor.pkl'
FEATURES_PATH = 'storage/model_features.json'


def main():
    # Загружаем модель и признаки
    model1, model2 = joblib.load(MODEL_PATH)
    with open(FEATURES_PATH, 'r') as f:
        features = json.load(f)
    # Важность признаков для team1
    importances1 = model1.feature_importances_
    importances2 = model2.feature_importances_
    print('Топ-20 признаков для team1_score:')
    for feat, imp in sorted(zip(features, importances1), key=lambda x: -x[1])[:20]:
        print(f'{feat:30s} {imp}')
    print('\nТоп-20 признаков для team2_score:')
    for feat, imp in sorted(zip(features, importances2), key=lambda x: -x[1])[:20]:
        print(f'{feat:30s} {imp}')

if __name__ == "__main__":
    main() 