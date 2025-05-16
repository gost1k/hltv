@echo off
cd /d %~dp0

echo ===========================================
echo [%date% %time%] Starting results parser loop
echo Press Ctrl+C to stop the script
echo ===========================================

:loop
echo ===========================================
echo [%date% %time%] Starting results parsing cycle
echo ===========================================

echo ===========================================
echo [%date% %time%] Running results parser...
echo ===========================================
call parse_results_html.bat
if errorlevel 1 (
    echo [%date% %time%] Error in results parser!
    timeout /t 60 /nobreak
    goto loop
)

echo ===========================================
echo [%date% %time%] Results parser completed
echo Waiting 10 minutes before next cycle...
echo Next cycle will start at: %date% %time%
echo ===========================================
timeout /t 600 /nobreak

goto loop 