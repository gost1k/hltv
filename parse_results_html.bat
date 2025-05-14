@echo off
cd /d C:\projects\hltv-2

@REM Парсинг прошедших матчей
python -m src.main --parse-results
python -m src.main --collect-results-list
python -m src.main --parse-details
python -m src.main --collect-results-matches

@REM Парсинг будущих матчей
@REM python -m src.main --parse-matches
