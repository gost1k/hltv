@echo off

:loop
cd /d C:\projects\hltv-2

@REM Загружаем RESULT матчи
python -m src.main --download-results-page
python -m src.main --write-db-results-list

@REM Загружаем UPCOMING матчи
python -m src.main --download-upcoming-page
python -m src.main --write-db-upcoming-list

@REM Качаем страницы RESULT матчей
python -m src.main --download-result-match-page
python -m src.main --write-json-match-page

@REM Качаем страницы UPCOMING матчей
python -m src.main --download-upcoming-match-page
python -m src.main --write-json-upcoming-match-page

@REM Загружаем данные в базу
python -m src.scripts.load_past_matches
python -m src.scripts.load_upcoming_matches

@REM Ждем 10 минут (600 секунд)
ping -n 1801 127.0.0.1 >nul
goto loop