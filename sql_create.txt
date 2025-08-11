CREATE TABLE detected_anomalies (
  id                  NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  msg_offset          NUMBER(19),
  state               VARCHAR2(20),
  route_code          VARCHAR2(32),
  customer_flag       NUMBER(10),
  tariff_number       NUMBER(10),
  rider               NUMBER(10),
  usage_amount        NUMBER(19,2),
  card_no             VARCHAR2(64),
  sam_seq_no          VARCHAR2(64),
  trans_flag          VARCHAR2(16),
  tap_id              VARCHAR2(64),
  boarding_date_time  VARCHAR2(50)
);