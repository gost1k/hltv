@echo off
:loop
cd /d C:\projects\hltv-2

@REM Cleaning up old matches
python -m src.scripts.cleanup_old_upcoming_matches

@REM Parsing upcoming matches
python -m src.main --download-upcoming-page
python -m src.main --write-db-upcoming-list
python -m src.main --download-upcoming-match-page
python -m src.main --write-json-upcoming-match-page
python -m src.scripts.load_upcoming_matches

@REM Ждем 10 минут (600 секунд)
ping -n 1801 127.0.0.1 >nul
goto loop
