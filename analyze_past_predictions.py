import pandas as pd
import matplotlib.pyplot as plt

THRESHOLD = 0.7  # Порог для «близких» предсказаний (используется только для матчей)

# Загрузка предсказаний и реальных результатов
try:
    df = pd.read_csv('predicted_past_maps.csv', encoding='utf-8-sig')
except FileNotFoundError:
    print('Файл predicted_past_maps.csv не найден! Сначала выполните predict_past.')
    exit(1)
except UnicodeDecodeError:
    print('Ошибка декодирования! Проверьте, что файл predicted_past_maps.csv сохранён в UTF-8 или UTF-8 с BOM.')
    exit(1)

# Определяем победителя по предсказанию и по факту
# 1 — победа team1, -1 — победа team2, 0 — ничья (если вдруг бывает)
df['winner_pred'] = (df['team1_pred'] > df['team2_pred']).astype(int) - (df['team1_pred'] < df['team2_pred']).astype(int)
df['winner_real'] = (df['team1_real'] > df['team2_real']).astype(int) - (df['team1_real'] < df['team2_real']).astype(int)

# Для карт: только точность угадывания победителя, без погрешности
strict_acc = (df['winner_pred'] == df['winner_real']).mean()

print(f"Всего карт: {len(df)}")
print(f"Точность угадывания победителя по сырым данным: {strict_acc:.2%}")

# Подробный вывод по ошибкам
errors = df[df['winner_pred'] != df['winner_real']]
correct = df[df['winner_pred'] == df['winner_real']]
print(f"\nОшибки (не угадан победитель): {len(errors)}")

# Средний confidence для правильных и ошибочных
if 'confidence' in df.columns:
    mean_conf_correct = correct['confidence'].mean()
    mean_conf_error = errors['confidence'].mean()
    print(f"Средний confidence для правильных: {mean_conf_correct:.3f}")
    print(f"Средний confidence для ошибок: {mean_conf_error:.3f}")

# Вывод ошибок с сортировкой по confidence
cols = ['match_id', 'map_name', 'team1_pred', 'team2_pred', 'team1_real', 'team2_real']
if 'confidence' in errors.columns:
    cols.append('confidence')
    errors = errors.sort_values('confidence', ascending=False)
if not errors.empty:
    print(errors[cols].to_string(index=False))

# Визуализация: barplot правильных и ошибочных предсказаний
counts = [len(correct), len(errors)]
labels = ['Правильно', 'Ошибка']
colors = ['green', 'red']
plt.figure(figsize=(6, 4))
plt.bar(labels, counts, color=colors)
plt.ylabel('Количество карт')
plt.title('Точность угадывания победителя по картам')
if 'confidence' in df.columns:
    plt.text(0, counts[0]+2, f"mean conf: {mean_conf_correct:.3f}", ha='center', color='green')
    plt.text(1, counts[1]+2, f"mean conf: {mean_conf_error:.3f}", ha='center', color='red')
plt.tight_layout()
plt.show() 