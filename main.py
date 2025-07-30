# main.py
import json
from kafka import KafkaConsumer
from model import OnlineFarePredictor
from config_loader import load_config

def main():
    cfg = load_config("config.ini")

    topic = cfg["kafka"]["topic"]
    bootstrap_servers = cfg["kafka"]["bootstrap_servers"]
    group_id = cfg["kafka"]["group_id"]
    threshold = float(cfg["app"]["error_threshold"])

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    model = OnlineFarePredictor(error_threshold=threshold)

    print("✅ Model initialized. Starting detection...\n")

    for msg in consumer:
        data = msg.value
        route = str(data['route_code'])
        flag = str(data['customer_flag'])
        amount = float(str(data['usage_amt']).lstrip("0") or 0)

        combo = f"{route}_{flag}"
        x = {f"combo_{combo}": 1}
        offset = msg.offset

        anomaly, error, y_pred = model.is_anomaly(x, amount)
        model.learn_one(x, amount)

        if anomaly:
            print(f"🚨 [ANOMALY] Offset={offset} | Route={route} | Flag={flag} | "
                  f"Actual={amount} | Predicted={y_pred:.2f} | Error={error:.2f}")
        else:
            print(f"✅ [NORMAL]  Offset={offset} | Route={route} | Flag={flag} | "
                  f"Actual={amount} | Predicted={y_pred:.2f} | Error={error:.2f}")

if __name__ == "__main__":
    main()
