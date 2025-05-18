log_file = "./logs/bot.log"

# Фразы, по которым определяем строки для удаления
patterns = [
    "HTTP Request: POST https://api.telegram.org/bot",
    "Telegram polling: OK"
]

with open(log_file, "r", encoding="utf-8") as f:
    lines = f.readlines()

with open(log_file, "w", encoding="utf-8") as f:
    for line in lines:
        if not any(pattern in line for pattern in patterns):
            f.write(line) 