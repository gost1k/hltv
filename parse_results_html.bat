@echo off
cd /d C:\projects\hltv-2
python -m src.main --parse-results
python -m src.main --collect-lists
python -m src.main --parse-details
python -m src.main --collect-details