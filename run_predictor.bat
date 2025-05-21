python -m src.scripts.predictor --mode train
python -m src.scripts.predictor --mode predict


@REM Делаем формулу для предиктов
python src/scripts/calibrate_postprocess_formula.py
@REM Проверить точность обычных предиктов
python .\check_predict_accuracy.py

@REM Формируем CSV для предиктов по прошедшим матчам
python -m src.scripts.predictor --mode predict_past
@REM Проверить прошлые придикты
python analyze_past_predictions.py

@REM Удаляем не состоявшиеся предикты
python delete_unplayed_predictions.py

