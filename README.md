1. Установите зависимости:

```bash
pip install -r requirements.txt
```

## Структура базы данных

С последнего обновления структура базы данных изменилась:

- `url_upcoming` - таблица предстоящих матчей
- `url_result` - таблица прошедших матчей

Для миграции со старой структуры выполните:

```bash
python src/check/migrate_db.py
```

## Использование

# Запустить только парсер матчей:

```bash
python src/main.py --parse-matches
```

# Запустить только парсер результатов:

```bash
python -m src.main --parse-results
```

# Запустить только сборщик данных:

```bash
python -m src.main --collect

# Запустить парсер деталей матчей:

```bash
python src/main.py --parse-details
```

# Запустить парсер деталей матчей в тестовом режиме (только 3 матча):

```bash
python src/main.py --parse-details --test
```

# Запустить парсер только для прошедших матчей:

```bash
python src/main.py --parse-details --parse-past
```

# Запустить парсер только для предстоящих матчей:

```bash
python src/main.py --parse-details --parse-upcoming
```

# Запустить парсер для обоих типов матчей:

```bash
python src/main.py --parse-details --parse-past --parse-upcoming
```

# Запустить все операции:

```bash
python src/main.py --all
```
