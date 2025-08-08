
import datetime as dt
from decimal import Decimal
import oracledb

def write_anomaly(
    row: dict,
    *,
    user: str,
    password: str,
    host: str = "localhost",
    port: int = 1521,
    service_name: str = "XEPDB1",
) -> int:


    bdt = row.get("boarding_date_time")
    if isinstance(bdt, str):

        bdt = dt.datetime.strptime(bdt, "%Y-%m-%d %H:%M:%S")
    if isinstance(bdt, dt.datetime):
        bdt = bdt.replace(microsecond=0)  # TIMESTAMP(0)

    bind = {
        "kafka_offset": int(row["kafka_offset"]),
        "route_code": str(row["route_code"]),
        "customer_flag": str(row["customer_flag"]),
        "usage_amt": Decimal(str(row["usage_amt"])),
        "card_no": str(row["card_no"]),
        "sam_seq_no": int(row["sam_seq_no"]),
        "trans_flag": str(row["trans_flag"]),
        "boarding_date_time": bdt,
        "tap_id": row.get("tap_id"),  # None olabilir
    }

    dsn = f"{host}:{port}/{service_name}"
    with oracledb.connect(user=user, password=password, dsn=dsn, encoding="UTF-8") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO anomalies (
                  kafka_offset, route_code, customer_flag, usage_amt,
                  card_no, sam_seq_no, trans_flag, boarding_date_time, tap_id
                ) VALUES (
                  :kafka_offset, :route_code, :customer_flag, :usage_amt,
                  :card_no, :sam_seq_no, :trans_flag, :boarding_date_time, :tap_id
                )
                """,
                bind,
            )
        conn.commit()
        return 1


# Mini örnek kullanım (dosyayı direkt çalıştırırsan):
if __name__ == "__main__":
    example = {
        "kafka_offset": 202507310001,
        "route_code": "34A",
        "customer_flag": "student",
        "usage_amt": 12.50,
        "card_no": "6578123412341234",
        "sam_seq_no": 555001,
        "trans_flag": "OK",
        "boarding_date_time": "2025-07-31 21:05:10",  # sen veriyorsun
        "tap_id": None,
    }

    inserted = write_anomaly(
        example,
        user="app_user",
        password="StrongPass123",
        host="localhost",
        port=1521,
        service_name="XEPDB1",
    )
    print("Inserted:", inserted)
