@echo off

:loop
@REM Парсим игроков
python -m src.scripts.download_players_html
python -m src.scripts.parse_players_html_to_json
python -m src.scripts.load_players_json_to_db

@REM Ждем 60 минут (3600 секунд)
ping -n 3601 127.0.0.1 >nul
goto loop