@echo off
cd /d %~dp0

echo ===========================================
echo [%date% %time%] Starting parsers loop
echo Press Ctrl+C to stop the script
echo ===========================================

:loop
echo ===========================================
echo [%date% %time%] Starting new parsing cycle
echo ===========================================

echo ===========================================
echo [%date% %time%] Starting results parser...
echo ===========================================
call parser_results_html.bat
if errorlevel 1 (
    echo [%date% %time%] Error in results parser!
    timeout /t 60 /nobreak
    goto loop
)

echo ===========================================
echo [%date% %time%] Waiting 5 minutes before starting upcoming matches parser...
echo Next parser will start at: %date% %time%
echo ===========================================
timeout /t 300 /nobreak

echo ===========================================
echo [%date% %time%] Starting upcoming matches parser...
echo ===========================================
call parse_upcoming_html.bat
if errorlevel 1 (
    echo [%date% %time%] Error in upcoming matches parser!
    timeout /t 60 /nobreak
    goto loop
)

echo ===========================================
echo [%date% %time%] All parsers completed
echo Waiting 25 minutes before next cycle...
echo Next cycle will start at: %date% %time%
echo ===========================================
timeout /t 1500 /nobreak

goto loop 