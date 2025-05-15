@echo off
cd /d C:\projects\hltv-2

@REM Парсинг прошедших матчей
python -m src.main --parse-results
python -m src.main --collect-results-list
python -m src.main --parse-details
python -m src.main --collect-results-matches
python src/scripts/load_past_matches.py