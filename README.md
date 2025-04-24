# HLTV Проект

Это базовый Python проект для работы с HLTV.

## Установка

1. Клонируйте репозиторий:
```bash
git clone [url-репозитория]
cd HLTV
```

2. Создайте виртуальное окружение и активируйте его:
```bash
python -m venv venv

# Для Windows:
venv\Scripts\activate

# Для Unix или MacOS:
source venv/bin/activate
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

## Использование

Запустите основной скрипт:
```bash
python src/main.py
```

## Структура проекта

```
HLTV/
├── src/               # Исходный код
│   ├── __init__.py
│   └── main.py
├── tests/             # Тесты
│   └── __init__.py
├── requirements.txt   # Зависимости проекта
├── README.md         # Документация
└── venv/             # Виртуальное окружение (не включается в репозиторий)
```