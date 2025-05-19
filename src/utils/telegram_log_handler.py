import logging
import requests
import os

class TelegramLogHandler(logging.Handler):
    def __init__(self, token, chat_id_file="dev_bot_chat_id.txt", chat_id=None):
        super().__init__()
        self.token = token
        self.chat_id_file = chat_id_file
        self.chat_id = str(chat_id) if chat_id else self._load_chat_id()
        self.buffer = []
        self.first_prefix = None
        if not self.chat_id:
            print(f"[TelegramLogHandler] ERROR: chat_id not found! Please create a file '{self.chat_id_file}' with your chat_id (e.g. 7146832422) or pass chat_id explicitly.")

    def _load_chat_id(self):
        if os.path.exists(self.chat_id_file):
            with open(self.chat_id_file, "r") as f:
                return f.read().strip()
        return None

    def emit(self, record):
        # Пропускать сообщения с no_telegram=True
        if hasattr(record, "no_telegram") and record.no_telegram:
            return
        # Если это специальная первая строка
        if hasattr(record, "telegram_firstline") and record.telegram_firstline:
            if self.formatter:
                ct = self.formatter.formatTime(record, "%Y-%m-%d %H:%M:%S")
                prefix = f"{ct},{int(record.msecs):03d} - {record.levelname} {record.getMessage()}"
            else:
                prefix = f"{record.created} - {record.levelname} {record.getMessage()}"
            self.first_prefix = prefix
            return  # Не добавляем в buffer
        # Обычные логи
        msg = record.getMessage()
        if not self.buffer and not self.first_prefix:
            # fallback: если вдруг не было первой строки
            if self.formatter:
                ct = self.formatter.formatTime(record, "%Y-%m-%d %H:%M:%S")
                self.first_prefix = f"{ct},{int(record.msecs):03d} - {record.levelname}"
            else:
                self.first_prefix = f"{record.created} - {record.levelname}"
        self.buffer.append(msg)

    def send_buffer(self):
        if not self.chat_id:
            print("[TelegramLogHandler] ERROR: chat_id not set. Log not sent.")
            return
        if not self.first_prefix:
            return
        if self.buffer:
            message = self.first_prefix + "\n\n" + "\n".join(self.buffer)
        else:
            message = self.first_prefix
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        data = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        try:
            requests.post(url, data=data, timeout=10)
        except Exception as e:
            print(f"[TelegramLogHandler] ERROR: Failed to send log to Telegram: {e}")
        self.buffer = []
        self.first_prefix = None 