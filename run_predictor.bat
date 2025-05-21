python -m src.scripts.predictor --mode train
python -m src.scripts.predictor --mode predict

Формируем CSV для предиктов по прошедшим матчам
python -m src.scripts.predictor --mode predict_past
Делаем формулу для предиктов
python src/scripts/calibrate_postprocess_formula.py