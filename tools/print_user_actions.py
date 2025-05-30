import json
from datetime import datetime
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
    for user_id, user_data in users.items():
        print(f"Пользователь: {user_id}")
        print(f"  Всего действий: {user_data.get('total_actions', 0)}")
        print(f"  Последнее действие: {user_data.get('last_action_time', '-')}")
        print("  Действия:")
        for action in user_data.get('actions', []):
            ts = action['timestamp']
            try:
                ts = datetime.fromisoformat(ts).strftime('%d.%m.%Y %H:%M:%S')
            except Exception:
                pass
            print(f"    [{ts}] {action['action']} -> {action['value']}")
        print('-' * 40)

if __name__ == "__main__":
    main() 