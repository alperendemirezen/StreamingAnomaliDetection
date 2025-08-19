import os
import pickle

from river import linear_model, metrics, optim, preprocessing
from datetime import datetime
import logging

class AmountPredictor:
    def __init__(self, error_threshold=0.01, learning_rate=0.01):
        self.model = linear_model.LinearRegression(optimizer=optim.SGD(learning_rate))

        self.threshold = error_threshold

        self.metrics = {
            'mae': metrics.MAE(),
            'rmse': metrics.RMSE(),
            'r2': metrics.R2(),
        }

        self.stats = {
            'total_processed': 0,
            'anomaly_count': 0,
            'validation_errors': 0,
            'last_performance_check': datetime.now()
        }

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)


    def validate_data(self, data):
        validation_errors = []

        required_fields = ['route_code', 'customer_flag', 'usage_amt']
        for field in required_fields:
            if field not in data:
                validation_errors.append(f"Missing required field: {field}")
            elif data[field] is None:
                validation_errors.append(f"Required field {field} is null")

        if validation_errors:
            raise ValueError("; ".join(validation_errors))

        cleaned_data = data.copy()

        if data.get('rider') is None:
            cleaned_data['rider'] = '0'

        if data.get('tariff_number') is None:
            cleaned_data['tariff_number'] = 0

        if data.get('transmit_cnt') is None:
            cleaned_data['transmit_cnt'] = 0

        if data.get('stage') is None:
            cleaned_data['stage'] = '0'

        if data.get('vehicle_type') is None:
            cleaned_data['vehicle_type'] = '0'

        if data.get('old_route_code') is None:
            cleaned_data['old_route_code'] = '0'


        try:
            amount = float(str(data['usage_amt']).lstrip("0") or 0)
            cleaned_data['amount'] = amount
        except (ValueError, TypeError):
            raise ValueError(f"Invalid amount format: {data['usage_amt']}")

        customer_cnt = 1
        if 'customer_cnt' in data and data['customer_cnt'] is not None:
            try:
                customer_cnt = int(data['customer_cnt'])
                if customer_cnt <= 0:
                    raise ValueError(f"customer_cnt must be > 0, got: {customer_cnt}")
            except (ValueError, TypeError):
                raise ValueError(f"Invalid customer_cnt format: {data.get('customer_cnt')}")

        cleaned_data['customer_cnt'] = customer_cnt

        usage_amount = amount / customer_cnt
        cleaned_data['usage_amount'] = usage_amount


        if usage_amount < 0:
            raise ValueError(f"Usage amount cannot be negative: {usage_amount}")

        route_code = str(data['route_code']).strip()
        if not route_code or route_code == '':
            raise ValueError("route_code cannot be empty")
        cleaned_data['route_code'] = route_code

        # old_route_code = str(data['old_route_code']).strip()
        # if not old_route_code or old_route_code == '':
        #     raise ValueError("old_route_code cannot be empty")
        # cleaned_data['old_route_code'] = old_route_code

        customer_flag = str(data['customer_flag']).strip()
        if not customer_flag or customer_flag == '':
            raise ValueError("customer_flag cannot be empty")
        cleaned_data['customer_flag'] = customer_flag

        return cleaned_data

    def predict_one(self, x):
        y_pred = self.model.predict_one(x)
        return y_pred if y_pred is not None else 0.0

    def learn_one(self, x, y):
        y_pred = self.predict_one(x)

        for metric in self.metrics.values():
            metric.update(y, y_pred)

        self.model.learn_one(x, y)

    def is_anomaly(self, x, y):
        y_pred = self.predict_one(x)
        error = abs(y - y_pred)

        if y > 0:
            error_percentage = (error / y) * 100
        else:
            error_percentage = 0 if error == 0 else float('inf')

        dynamic_threshold = y_pred * self.threshold

        if y == 0:
            return error > 10, error, y_pred, 10, error_percentage
        else:
            return error > dynamic_threshold, error, y_pred, dynamic_threshold, error_percentage

    def get_performance_report(self):
        anomaly_rate = (self.stats['anomaly_count'] / max(self.stats['total_processed'], 1)) * 100

        report = {
            'performance_metrics': {
                'mae': self.metrics['mae'].get(),
                'rmse': self.metrics['rmse'].get(),
                'r2': self.metrics['r2'].get()
            },
            'processing_stats': {
                'total_processed': self.stats['total_processed'],
                'anomaly_count': self.stats['anomaly_count'],
                'anomaly_rate_percent': round(anomaly_rate, 2),
                'validation_errors': self.stats['validation_errors']
            },
            'system_health': self._assess_system_health()
        }
        return report

    def _assess_system_health(self):
        mae = self.metrics['mae'].get()
        r2 = self.metrics['r2'].get()
        anomaly_rate = (self.stats['anomaly_count'] / max(self.stats['total_processed'], 1)) * 100

        health_status = "GOOD"
        issues = []

        if mae > 1.0:
            health_status = "WARNING"
            issues.append(f"High MAE: {mae:.3f} TL")

        if r2 < 0.5:
            health_status = "CRITICAL"
            issues.append(f"Low R²: {r2:.3f}")

        if anomaly_rate > 5:
            health_status = "WARNING" if health_status == "GOOD" else health_status
            issues.append(f"High anomaly rate: {anomaly_rate:.1f}%")

        return {
            'status': health_status,
            'issues': issues
        }

    def log_performance(self, check_performance=1000):
        if self.stats['total_processed'] % check_performance == 0:
            report = self.get_performance_report()

            self.logger.info("=" * 50)
            self.logger.info("[PERFORMANCE REPORT]")
            self.logger.info(f"MAE: {report['performance_metrics']['mae']:.4f} TL")
            self.logger.info(f"RMSE: {report['performance_metrics']['rmse']:.4f} TL")
            self.logger.info(f"R²: {report['performance_metrics']['r2']:.4f}")
            self.logger.info(f"Total Processed: {report['processing_stats']['total_processed']}")
            self.logger.info(f"Anomaly Rate: {report['processing_stats']['anomaly_rate_percent']}%")
            self.logger.info(f"System Health: {report['system_health']['status']}")

            if report['system_health']['issues']:
                self.logger.warning(" Issues detected:")
                for issue in report['system_health']['issues']:
                    self.logger.warning(f"  - {issue}")

            self.logger.info("=" * 50)

    def save_model(self, file_path="amount_predictor.pkl"):
        snapshot = {
            "model": self.model,
            "metrics": self.metrics,
            "stats": self.stats
        }
        with open(file_path, "wb") as f:
            pickle.dump(snapshot, f, protocol=pickle.HIGHEST_PROTOCOL)
        self.logger.info(f"Model + metrics + stats saved to {file_path}")

    def load_model(self, file_path="amount_predictor.pkl"):
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                snapshot = pickle.load(f)

            self.model = snapshot.get("model", self.model)
            self.metrics = snapshot.get("metrics", self.metrics)
            self.stats = snapshot.get("stats", self.stats)

            self.logger.info(f"Model + metrics + stats loaded from {file_path}")
            return True
        else:
            self.logger.warning(f"No saved model found at {file_path}, starting fresh.")
            return False