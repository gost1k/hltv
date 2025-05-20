python -m src.scripts.predictor --mode train
python -m src.scripts.predictor --mode predict
python -m src.scripts.evaluate_predictions --period all
python -m src.scripts.evaluate_predictions --period week