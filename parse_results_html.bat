@echo off
cd /d C:\projects\hltv-2
python -m src.main --parse-results
python -m src.main --collect
python -m src.main --parse-details
