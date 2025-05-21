import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
import os

if not os.path.exists('predicted_past_maps.csv'):
    print('Файл predicted_past_maps.csv не найден!')
    exit(1)

df = pd.read_csv('predicted_past_maps.csv')
print('Размерность df:', df.shape)
print(df.head())

if 'team1_pred' in df.columns and 'team2_pred' in df.columns and 'team1_real' in df.columns and 'team2_real' in df.columns:
    df['diff_pred'] = abs(df['team1_pred'] - df['team2_pred'])
    df['loser_real'] = df[['team1_real', 'team2_real']].min(axis=1)
    X = df[['diff_pred']]
    y = df['loser_real']
    if len(df) == 0:
        print('Нет данных для калибровки!')
    else:
        reg = LinearRegression().fit(X, y)
        a, b = reg.coef_[0], reg.intercept_
        print(f"Лучшая линейная формула: loser_score = {a:.2f} * diff_pred + {b:.2f}")
        df['loser_pred'] = a * df['diff_pred'] + b
        print("MAE по проигравшему (линейная формула):", mean_absolute_error(df['loser_real'], df['loser_pred']))
        # Сохраняем формулу в файл
        with open('formula.txt', 'w') as f:
            f.write(f"loser_score = {a:.6f} * diff_pred + {b:.6f}\n")
            f.write(f"MAE: {mean_absolute_error(df['loser_real'], df['loser_pred'])}\n")
else:
    print('В predicted_past_maps.csv не найдены нужные колонки!')
    print('Колонки:', list(df.columns)) 