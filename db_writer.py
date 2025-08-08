import oracledb

class OracleAnomalyWriter:
    def __init__(self, user, password, dsn):
        self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        self.cursor = self.conn.cursor()

    def insert_anomaly(self, anomaly_data):
        sql = """
            INSERT INTO detected_anomalies (
                offset, route_code, customer_flag, tariff_number, rider,
                usage_amount, card_no, sam_seq_no, trans_flag, tap_id, boarding_date_time
            ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
        """

        values = (
            anomaly_data.get('offset'),
            anomaly_data.get('route_code'),
            anomaly_data.get('customer_flag'),
            anomaly_data.get('tariff_number', None),
            anomaly_data.get('rider', None),
            anomaly_data.get('usage_amt'),
            anomaly_data.get('card_no', None),
            anomaly_data.get('sam_seq_no', None),
            anomaly_data.get('trans_flag', None),
            anomaly_data.get('tap_id', None),
            anomaly_data.get('boarding_date_time', None),
        )

        self.cursor.execute(sql, values)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()
