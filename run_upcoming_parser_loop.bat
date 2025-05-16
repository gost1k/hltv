@echo off
cd /d %~dp0

echo ===========================================
echo [%date% %time%] Starting upcoming matches parser loop
echo Press Ctrl+C to stop the script
echo ===========================================

:loop
echo ===========================================
echo [%date% %time%] Starting upcoming matches parsing cycle
echo ===========================================

echo ===========================================
echo [%date% %time%] Running upcoming matches parser...
echo ===========================================
call parse_upcoming_html.bat
if errorlevel 1 (
    echo [%date% %time%] Error in upcoming matches parser!
    timeout /t 60 /nobreak
    goto loop
)

echo ===========================================
echo [%date% %time%] Upcoming matches parser completed
echo Waiting 10 minutes before next cycle...
echo Next cycle will start at: %date% %time%
echo ===========================================
timeout /t 600 /nobreak

goto loop 