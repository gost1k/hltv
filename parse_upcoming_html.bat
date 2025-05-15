@echo off
cd /d %~dp0
set PYTHONPATH=%~dp0
python -m src.main --parse-matches
python -m src.main --collect-matches-list
python -m src.main --parse-details --upcoming
python -m src.collector.match_upcoming
python -m src.scripts.load_upcoming_matches
