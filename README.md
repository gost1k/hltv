1. Установите зависимости:

```bash
pip install -r requirements.txt
```

## Использование

# Запустить только парсер матчей:

```bash
python src/main.py --parse-matches
```

# Запустить только парсер результатов:

```bash
python src/main.py --parse-results
```

# Запустить только сборщик данных:

```bash
python src/main.py --collect
```

# Запустить парсер деталей матчей:

```bash
python src/main.py --parse-details
```

# Запустить парсер деталей матчей в тестовом режиме (только 3 матча):

```bash
python src/main.py --parse-details --test
```

# Запустить все операции:

```bash
python src/main.py --all
```
