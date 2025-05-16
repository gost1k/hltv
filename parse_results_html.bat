@echo off
cd /d C:\projects\hltv-2

@REM Parsing past matches
python -m src.main --download-results-page
python -m src.main --write-db-results-list
python -m src.main --download-result-match-page
python -m src.main --write-json-match-page
python src/scripts/load_past_matches.py