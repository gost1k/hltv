@echo off
cd /d C:\projects\hltv-2

@REM Cleaning up old matches
python -m src.scripts.cleanup_old_upcoming_matches

@REM Parsing upcoming matches
python -m src.main --parse-matches
python -m src.main --collect-matches-list
python -m src.main --parse-details --upcoming
python -m src.collector.match_upcoming
python -m src.scripts.load_upcoming_matches
