import joblib
import json
import matplotlib.pyplot as plt
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
    print('Важность признаков для team1_score:')
    for feat, imp in sorted(zip(features, importances1), key=lambda x: -x[1]):
        print(f'{feat:30s} {imp}')
    print('\nВажность признаков для team2_score:')
    for feat, imp in sorted(zip(features, importances2), key=lambda x: -x[1]):
        print(f'{feat:30s} {imp}')
    # График для team1
    idx1 = np.argsort(importances1)[::-1][:20]
    plt.figure(figsize=(10, 6))
    plt.barh(np.array(features)[idx1][::-1], importances1[idx1][::-1])
    plt.title('Feature importance (team1_score)')
    plt.tight_layout()
    plt.show()
    # График для team2
    idx2 = np.argsort(importances2)[::-1][:20]
    plt.figure(figsize=(10, 6))
    plt.barh(np.array(features)[idx2][::-1], importances2[idx2][::-1])
    plt.title('Feature importance (team2_score)')
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main() 