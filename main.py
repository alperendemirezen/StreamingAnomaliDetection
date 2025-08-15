import json
import logging

from kafka import KafkaConsumer
from river import optim

from config_loader import load_config
from db_writer import OracleAnomalyWriter
from model import AmountPredictor


def setup_logging(log_level="INFO", log_file="app.log"):

    level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("kafka.consumer.fetcher").setLevel(logging.ERROR)
    logging.getLogger("oracledb").setLevel(logging.WARNING)

def main():

    try:
        cfg = load_config("config.ini")

        log_level = cfg["logging"]["level"]
        log_file = cfg["logging"]["file"]

        setup_logging(log_level,log_file)
        logger = logging.getLogger(__name__)

        logger.info("Starting anomaly detection system...")

        topic = cfg["kafka"]["topic"]
        bootstrap_servers = cfg["kafka"]["bootstrap_servers"]
        group_id = cfg["kafka"]["group_id"]
        timeout = int(cfg["kafka"]["timeout"])

        threshold = float(cfg["app"]["error_threshold_percentage"])
        check_performance = int(cfg["app"]["check_performance"])
        warmup_count = int(cfg["app"]["warmup_count"])
        save_model = int(cfg["app"]["save_model"])
        learning_rate = float(cfg["app"]["learning_rate"])

        user = cfg["database"]["user"]
        password = cfg["database"]["password"]
        host = cfg["database"]["host"]
        port = cfg["database"]["port"]
        service = cfg["database"]["service"]

        dsn = f"{host}:{port}/{service}"

        logger.info(f"Configuration loaded - Topic: {topic}, Threshold: {threshold}, Warmup: {warmup_count}")

        db_writer = OracleAnomalyWriter(user, password, dsn)

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            request_timeout_ms=timeout,
        )

        logger.info(f"Kafka consumer initialized for topic: {topic}")

        model = AmountPredictor(error_threshold=threshold, learning_rate=learning_rate)
        original_optimizer = model.model.optimizer

        anomaly_learning_rate = learning_rate * 0.1
        anomaly_optimizer = optim.SGD(anomaly_learning_rate)
        if model.load_model("amount_predictor.pkl"):
            warmup_count = 0

        logger.info("Enhanced model initialized. Starting detection...")
        logger.info(f"Config: threshold={threshold}, check_performance={check_performance}")

        for msg in consumer:
            try:
                raw_data = msg.value

                if not (raw_data['type'] == 'T') or not (raw_data['record_id'] == 'D'):
                    continue

                cleaned_data = model.validate_data(raw_data)

                route_code = cleaned_data['route_code']
                # old_route_code = cleaned_data.get('old_route_code')
                customer_flag = cleaned_data['customer_flag']
                tariff_number = cleaned_data['tariff_number']
                rider = cleaned_data['rider']
                transmit_cnt = cleaned_data.get('transmit_cnt')

                stage = cleaned_data['stage']
                vehicle_type = cleaned_data['vehicle_type']

                customer_cnt = cleaned_data['customer_cnt']
                total_amount = cleaned_data['amount']
                usage_amt = cleaned_data['usage_amount']

                features = {
                    f"route_{route_code}": 1,
                    f"customer_{customer_flag}": 1,
                    f"tariff_{tariff_number}": 1,
                    f"rider_{rider}": 1,
                    f"transmit_{transmit_cnt}": 1,

                    f"route_customer_{route_code}_{customer_flag}": 1,
                    f"route_tariff_{route_code}_{tariff_number}": 1,
                    f"customer_tariff_{customer_flag}_{tariff_number}": 1,

                    f"full_combo_{route_code}_{customer_flag}_{tariff_number}_{rider}_{transmit_cnt}": 1 #{old_route_code}
                }
                offset = msg.offset

                model.stats['total_processed'] += 1

                if model.stats['total_processed'] % save_model == 0:
                    model.save_model("amount_predictor.pkl")

                if model.stats['total_processed'] <= warmup_count:
                    model.learn_one(features, usage_amt)
                    logger.info(
                        f"[WARMUP] Offset={offset} | Route={route_code} | Flag={customer_flag} | "
                        f"Tariff={tariff_number} | Rider={rider} | Transmit={transmit_cnt} | "  
                        f"Total={total_amount:.2f} | "
                        f"Per Person={usage_amt:.2f} | Processed={model.stats['total_processed']}")
                else:
                    anomaly, error, y_pred, threshold_used, error_pct = model.is_anomaly(features, usage_amt)

                    customer_info = f"({customer_cnt} person)" if customer_cnt > 1 else ""

                    if anomaly:


                        if error >= threshold_used * 6:
                            model.model.optimizer = anomaly_optimizer
                            model.learn_one(features, usage_amt)
                            model.model.optimizer = original_optimizer
                            state = "critical"

                        elif error >= threshold_used * 3:
                            model.model.optimizer = anomaly_optimizer
                            model.learn_one(features, usage_amt)
                            model.model.optimizer = original_optimizer
                            state = "major"

                        else:
                            model.learn_one(features, usage_amt)
                            state = "minor"

                        model.stats['anomaly_count'] += 1
                        db_writer.insert_anomaly(cleaned_data, msg.offset, state, y_pred, error, error_pct)

                        logger.warning(
                            f"[ANOMALY-{state.upper()}] Offset={offset} | Route={route_code} | Flag={customer_flag} | "
                            f"Tariff={tariff_number} | Rider={rider} | Transmit={transmit_cnt} | Total={total_amount:.2f} | "
                            f"Per Person={usage_amt:.2f} {customer_info} | Predicted={y_pred:.2f} | Error={error:.2f}({error_pct:.1f}%) | Processed={model.stats['total_processed']}")


                    if not anomaly:
                        model.learn_one(features, usage_amt)
                        logger.debug(f"[NORMAL] Offset={offset} | Route={route_code} | Flag={customer_flag} | "
                                     f"Tariff={tariff_number} | Rider={rider} | Transmit={transmit_cnt} | Total={total_amount:.2f} | "
                                     f"Per Person={usage_amt:.2f} {customer_info} | "
                                     f"Predicted={y_pred:.2f} | Error={error:.2f} | Processed={model.stats['total_processed']}")

                model.log_performance(check_performance)

            except ValueError as e:
                model.stats['validation_errors'] += 1
                db_writer.insert_anomaly(raw_data, msg.offset, "invalid", 0, 0, 0)
                logger.error(f"[VALIDATION ERROR] Offset={msg.offset} | Error: {e}")
                continue

            except Exception as e:
                logger.critical(f"[SYSTEM ERROR] Offset={msg.offset} | Error: {e}", exc_info=True)
                continue

    except Exception as e:
        logger.critical(f"Critical error in main function: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()