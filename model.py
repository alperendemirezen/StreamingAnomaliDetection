# model.py
from river import linear_model
from river import metrics

class OnlineFarePredictor:
    def __init__(self, error_threshold=0.01):
        # Basit lineer regresyon, manuel one-hot feature ile
        self.model = linear_model.LinearRegression()
        self.threshold = error_threshold
        self.metric = metrics.MAE()
# OnlineFarePredictor sınıfı, çevrimiçi öğrenme ve anomali tespiti için kullanılır.
    def predict_one(self, x):
        y_pred = self.model.predict_one(x)
        return y_pred if y_pred is not None else 0.0

    def learn_one(self, x, y):
        # Model öğrenir
        self.model.learn_one(x, y)
        # Skor güncelle
        self.metric.update(y, self.predict_one(x))

    def is_anomaly(self, x, y):
        y_pred = self.predict_one(x)
        error = abs(y - y_pred)
        return error > self.threshold, error, y_pred
