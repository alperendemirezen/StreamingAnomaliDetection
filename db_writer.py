import oracledb
import logging


class OracleAnomalyWriter:
    def __init__(self, user, password, dsn):
        self.logger = logging.getLogger(__name__)

        try:
            self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
            self.cursor = self.conn.cursor()
            self.logger.info(f"Oracle database connection established successfully to {dsn}")
        except oracledb.DatabaseError as e:
            self.logger.error(f"Failed to connect to Oracle database: {e}")
            raise

    def insert_anomaly(self, data: dict, msg_offset: int, state: str):
        try:
            sql = """
                       INSERT INTO detected_anomalies (
                           msg_offset, state, route_code, customer_flag, tariff_number, rider,
                           usage_amount, card_no, sam_seq_no, trans_flag, tap_id, boarding_date_time
                       ) VALUES (:msg_offset, :state, :route_code, :customer_flag, :tariff_number, :rider,
                                 :usage_amount, :card_no, :sam_seq_no, :trans_flag, :tap_id, :boarding_date_time)
                   """
            params = {
                "msg_offset": msg_offset,
                "state": state,
                "route_code": data.get("route_code"),
                "customer_flag": data.get("customer_flag"),
                "tariff_number": data.get("tariff_number"),
                "rider": data.get("rider"),
                "usage_amount": data.get("usage_amount"),
                "card_no": data.get("card_no"),
                "sam_seq_no": data.get("sam_seq_no"),
                "trans_flag": data.get("trans_flag"),
                "tap_id": data.get("tap_id"),
                "boarding_date_time": data.get("boarding_date_time"),
            }
            self.cursor.execute(sql, params)
            self.conn.commit()
            self.logger.debug(
                f"Anomaly inserted successfully: offset={msg_offset}, state={state}, route={data.get('route_code')}")

        except oracledb.DatabaseError as e:
            self.logger.error(f"Database insert error for offset {msg_offset}: {e}")
            self.conn.rollback()
            raise

