@echo off
REM Запуск live-парсера (будет работать в цикле)
start "Live Parser" python -m src.scripts.live_matches_parser

REM Запуск загрузки предстоящих матчей (разово, можно добавить в планировщик)
python -m src.scripts.load_upcoming_matches

REM Можно добавить запуск других скриптов по необходимости
REM python -m src.scripts.load_past_matches 