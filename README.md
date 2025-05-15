## HLTV Parser

A Python tool for parsing and collecting data from HLTV.org.

### Features

- Parse results, matches, and match details pages from HLTV.org
- Collect and store data from parsed pages
- Store data in SQLite database

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/hltv-parser.git
   cd hltv-parser
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Usage

The HLTV Parser provides several command-line options for different operations:

#### Basic Commands

| Command                     | Description                                               |
| --------------------------- | --------------------------------------------------------- |
| `--parse-results`           | Parse the results page from HLTV.org                      |
| `--parse-matches`           | Parse the matches page from HLTV.org                      |
| `--parse-details`           | Parse match details pages from IDs stored in the database |
| `--collect-results-list`    | Collect data from parsed matches and results HTML files   |
| `--collect-results-details` | Collect data from parsed match details HTML files         |

#### Legacy Commands (Deprecated)

| Command             | Description                                      |
| ------------------- | ------------------------------------------------ |
| `--collect-lists`   | Alias for --collect-results-list (deprecated)    |
| `--collect-details` | Alias for --collect-results-details (deprecated) |

#### Optional Parameters

| Parameter           | Description                                                                 |
| ------------------- | --------------------------------------------------------------------------- |
| `--details-limit N` | Limit the number of match details to parse                                  |
| `--test`            | Test mode: parse only 3 matches                                             |
| `--past`            | Parse past matches (default if neither `--past` nor `--upcoming` specified) |
| `--upcoming`        | Parse upcoming matches                                                      |

### Examples

Parse results page:

```bash
python -m src.main --parse-results
```

Parse matches page:

```bash
python -m src.main --parse-matches
```

Collect data from parsed matches and results:

```bash
python -m src.main --collect-results-list
```

Parse match details for 10 past matches:

```bash
python -m src.main --parse-details --details-limit 10 --past
```

Collect data from parsed match details:

```bash
python -m src.main --collect-results-details
```

### Project Structure

```
hltv-parser/
├── src/                      # Source code
│   ├── __init__.py           # Package initialization
│   ├── main.py               # Main entry point
│   ├── config/               # Configuration
│   │   └── constants.py      # Constants and configuration values
│   ├── db/                   # Database operations
│   │   └── database.py       # Database service
│   ├── models/               # Data models
│   │   └── match.py          # Match-related models
│   ├── parser/               # Parser modules
│   │   ├── base.py           # Base parser class
│   │   ├── matches.py        # Matches page parser
│   │   ├── match_details.py  # Match details parser
│   │   └── results.py        # Results page parser
│   ├── collector/            # Data collectors
│   │   ├── matches.py        # Matches collector
│   │   └── match_details.py  # Match details collector
│   ├── parsers/              # Parser managers
│   │   └── manager.py        # Parser manager
│   ├── collectors/           # Collector managers
│   │   └── manager.py        # Collector manager
│   └── utils/                # Utility functions
│       ├── __init__.py       # Utils package initialization
│       └── helpers.py        # Helper functions
├── logs/                     # Log files
├── storage/                  # Storage for HTML files
│   └── html/                 # HTML storage
│       └── match_details/    # Match details HTML files
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

### License

This project is licensed under the MIT License - see the LICENSE file for details.

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

# Запустить сборщик данных списков матчей:

```bash
python -m src.main --collect-results-list
```

# Запустить сборщик данных деталей матчей:

```bash
python -m src.main --collect-results-details
```

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
python src/main.py --parse-details --past
```

# Запустить парсер только для предстоящих матчей:

```bash
python src/main.py --parse-details --upcoming
```

# Запустить парсер для обоих типов матчей:

```bash
python src/main.py --parse-details --past --upcoming
```

## Новая функциональность

### Сохранение данных в JSON

Теперь данные сначала сохраняются в JSON файлы для возможности их проверки перед загрузкой в базу данных:
- Сведения о матчах сохраняются в `storage/json/match_details/`
- Статистика игроков сохраняется в `storage/json/player_stats/`
- Информация о составах предстоящих матчей сохраняется в `storage/json/upcoming_players/`

> **Примечание:** Основные команды `--collect-results-list` и `--collect-matches-list` записывают данные напрямую в базу данных, а также дублируют их в JSON-файлы для совместимости. Только детализированные данные о матчах (детали матчей и статистика игроков) сохраняются исключительно в JSON-формате и требуют отдельной загрузки.

### Загрузка данных из JSON в базу данных

Для загрузки данных из JSON в базу данных добавлены новые команды:

```bash
# Загрузка списков матчей (предстоящих и прошедших)
python -m src.main --load-matches-from-json

# Загрузка деталей матчей и статистики игроков
python -m src.main --load-match-details-from-json
```

Также добавлены отдельные скрипты для более гибкой загрузки данных:

```bash
# Загрузка только предстоящих матчей и информации о игроках
python src/scripts/load_upcoming_matches.py

# Загрузка прошедших матчей, деталей матчей и статистики игроков
python src/scripts/load_past_matches.py

# Загрузка только прошедших матчей без деталей и статистики
python src/scripts/load_past_matches.py --skip-match-details --skip-player-stats
```

Это позволяет проверить и, при необходимости, отредактировать данные перед их загрузкой в базу.
