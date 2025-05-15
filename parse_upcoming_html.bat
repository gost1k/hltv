@echo off
cd /d C:\projects\hltv-2
@REM Parsing upcoming matches
python -m src.main --parse-matches
python -m src.main --collect-matches-list
python -m src.main --parse-details --upcoming
python -m src.collector.match_upcoming
python src/scripts/load_upcoming_matches.py
