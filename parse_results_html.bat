@echo off
:loop
cd /d C:\projects\hltv-2

@REM Parsing past matches
python -m src.main --download-results-page
python -m src.main --write-db-results-list
python -m src.main --download-result-match-page
python -m src.main --write-json-match-page
python -m src.scripts.load_past_matches

@REM Ждем 10 минут (600 секунд)
ping -n 601 127.0.0.1 >nul
goto loop