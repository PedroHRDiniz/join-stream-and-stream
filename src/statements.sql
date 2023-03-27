CREATE STREAM orders (ID INT KEY, order_ts VARCHAR, total_amount DOUBLE, customer_name VARCHAR)
    WITH (KAFKA_TOPIC='_orders',
          VALUE_FORMAT='AVRO',
          TIMESTAMP='order_ts',
          TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ssX',
          PARTITIONS=4);

CREATE STREAM shipments (ID VARCHAR KEY, ship_ts VARCHAR, order_id INT, warehouse VARCHAR)
    WITH (KAFKA_TOPIC='_shipments',
          VALUE_FORMAT='AVRO',
          TIMESTAMP='ship_ts',
          TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ssX',
          PARTITIONS=4);

CREATE STREAM SHIPPED_ORDERS AS
SELECT O.ID AS ORDER_ID,
       TIMESTAMPTOSTRING(O.ROWTIME, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS ORDER_TS,
       O.TOTAL_AMOUNT,
       O.CUSTOMER_NAME,
       S.ID AS SHIPMENT_ID,
       TIMESTAMPTOSTRING(S.ROWTIME, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS SHIPMENT_TS,
       S.WAREHOUSE,
       (S.ROWTIME - O.ROWTIME) / 1000 / 60 AS SHIP_TIME
FROM ORDERS O INNER JOIN SHIPMENTS S
    WITHIN 7 DAYS
ON O.ID = S.ORDER_ID;

INSERT INTO orders (id, order_ts, total_amount, customer_name) VALUES (1, '2019-03-29T06:01:18Z', 133548.84, 'Ricardo Ferreira');
INSERT INTO orders (id, order_ts, total_amount, customer_name) VALUES (2, '2019-03-29T17:02:20Z', 164839.31, 'Tim Berglund');
INSERT INTO orders (id, order_ts, total_amount, customer_name) VALUES (3, '2019-03-29T13:44:10Z', 90427.66, 'Robin Moffatt');
INSERT INTO orders (id, order_ts, total_amount, customer_name) VALUES (4, '2019-03-29T11:58:25Z', 33462.11, 'Viktor Gamov');

INSERT INTO shipments (id, ship_ts, order_id, warehouse) VALUES ('ship-ch83360', '2019-03-31T18:13:39Z', 1, 'UPS');
INSERT INTO shipments (id, ship_ts, order_id, warehouse) VALUES ('ship-xf72808', '2019-03-31T02:04:13Z', 2, 'UPS');
INSERT INTO shipments (id, ship_ts, order_id, warehouse) VALUES ('ship-kr47454', '2019-03-31T20:47:09Z', 3, 'DHL');

SET 'auto.offset.reset' = 'earliest';