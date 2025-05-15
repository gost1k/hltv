@echo off
cd /d %~dp0
set PYTHONPATH=%~dp0
python -m src.main --parse-results
python -m src.main --collect-results-list
python -m src.main --parse-details
python -m src.main --collect-results-matches
python src/scripts/load_past_matches.py