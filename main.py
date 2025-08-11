import json
import oracledb

from kafka import KafkaConsumer
from config_loader import load_config
from db_writer import OracleAnomalyWriter
from model import AmountPredictor




def main():

    cfg = load_config("config.ini")
    topic = cfg["kafka"]["topic"]
    bootstrap_servers = cfg["kafka"]["bootstrap_servers"]
    group_id = cfg["kafka"]["group_id"]
    threshold = float(cfg["app"]["error_threshold"])
    check_performance = int(cfg["app"]["check_performance"])
    warmup_count = int(cfg["app"]["warmup_count"])

    user = cfg["database"]["user"]
    password = cfg["database"]["password"]
    host = cfg["database"]["host"]
    port = cfg["database"]["port"]
    service = cfg["database"]["service"]

    dsn = f"{host}:{port}/{service}"

    db_writer = OracleAnomalyWriter(user, password, dsn)

    cur = db_writer.conn.cursor()
    cur.execute("select user, sys_context('USERENV','CON_NAME') from dual")
    print("LOGIN:", cur.fetchone())
    cur.execute("select table_name from user_tables where table_name='DETECTED_ANOMALIES'")
    print("USER_TABLES:", cur.fetchall())
    cur.execute("select owner, table_name from all_tables where table_name='DETECTED_ANOMALIES'")
    print("ALL_TABLES:", cur.fetchall())
    cur.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    model = AmountPredictor(error_threshold=threshold)

    print("✅ Enhanced model initialized. Starting detection...\n")
    print(f"🔧 Config: threshold={threshold}, check_performance={check_performance}")

    for msg in consumer:
        try:
            raw_data = msg.value
            cleaned_data = model.validate_data(raw_data)

            route_code = cleaned_data['route_code']
            customer_flag = cleaned_data['customer_flag']
            tariff_number = cleaned_data['tariff_number']
            rider = cleaned_data['rider']
            customer_cnt = cleaned_data['customer_cnt']
            total_amount = cleaned_data['amount']
            usage_amt = cleaned_data['usage_amount']


            combo = f"{route_code}_{customer_flag}_{tariff_number}_{rider}"
            x = {f"combo_{combo}": 1}
            offset = msg.offset

            model.stats['total_processed'] += 1

            if model.stats['total_processed'] <= warmup_count:
                model.learn_one(x, usage_amt)
                print(
                    f"🔥 [WARMUP] Offset={offset} | Combo={combo} | Amount={total_amount:.2f} | Per Person={usage_amt:.2f}")
            else:
                anomaly, error, y_pred, threshold_used = model.is_anomaly(x, usage_amt)
                model.learn_one(x, usage_amt)

                if anomaly:
                    model.stats['anomaly_count'] += 1

                    if error >= threshold_used * 10:
                        state = "critical"
                    elif error >= threshold_used * 5:
                        state = "major"
                    else:
                        state = "minor"

                    db_writer.insert_anomaly(cleaned_data, offset, state)

                status = "🚨 [ANOMALY]" if anomaly else "✅ [NORMAL] "
                customer_info = f"({customer_cnt} person)" if customer_cnt > 1 else ""
                print(f"{status} Offset={offset} | Route={route_code} | Flag={customer_flag} | "
                      f"Tariff={tariff_number} | Rider={rider} | Total={total_amount:.2f} | "
                      f"Per Person={usage_amt:.2f} {customer_info} | "
                      f"Predicted={y_pred:.2f} | Error={error:.2f} | Threshold={threshold_used:.2f}")

            model.log_performance(check_performance)

        except ValueError as e:
            model.stats['validation_errors'] += 1
            db_writer.insert_anomaly(raw_data, msg.offset, "invalid")
            print(f"❌ [VALIDATION ERROR] Offset={msg.offset} | Error: {e}")
            continue

        except Exception as e:
            print(f"💥 [SYSTEM ERROR] Offset={msg.offset} | Error: {e}")
            continue

if __name__ == "__main__":
    main()