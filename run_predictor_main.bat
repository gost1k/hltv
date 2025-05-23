@echo off
echo ======================================
echo Starting CS2 Match Predictor...
echo ======================================

REM Активируем виртуальное окружение если есть
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM Запускаем предиктор
python src/scripts/predictor_main.py

echo.
echo ======================================
echo Predictor finished!
echo ======================================
pause 