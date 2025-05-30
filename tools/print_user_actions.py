import json
from datetime import datetime, date
import os

ACTIONS_PATH = os.path.join(os.path.dirname(__file__), '../storage/json/bot/user_actions.json')
ACTIONS_PATH = os.path.normpath(ACTIONS_PATH)

def main():
    if not os.path.exists(ACTIONS_PATH):
        print(f"Файл не найден: {ACTIONS_PATH}")
        return
    with open(ACTIONS_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    users = data.get('users', {})
    if not users:
        print("Нет данных о действиях пользователей.")
        return
    today = date.today()
    any_actions = False
    for user_id, user_data in users.items():
        todays_actions = []
        for action in user_data.get('actions', []):
            ts = action['timestamp']
            try:
                action_date = datetime.fromisoformat(ts).date()
            except Exception:
                continue
            if action_date == today:
                todays_actions.append(action)
        if not todays_actions:
            continue
        any_actions = True
        print(f"Пользователь: {user_id}")
        print(f"  Всего действий сегодня: {len(todays_actions)}")
        print(f"  Последнее действие сегодня: {todays_actions[-1]['timestamp']}")
        print("  Действия:")
        for action in todays_actions:
            ts = action['timestamp']
            try:
                ts = datetime.fromisoformat(ts).strftime('%d.%m.%Y %H:%M:%S')
            except Exception:
                pass
            print(f"    [{ts}] {action['action']} -> {action['value']}")
        print('-' * 40)
    if not any_actions:
        print("Нет действий пользователей за сегодня.")

if __name__ == "__main__":
    main() 