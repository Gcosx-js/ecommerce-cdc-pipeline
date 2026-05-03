CREATE TABLE f_events (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMP,
    event_type TEXT,
    product_id BIGINT,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT,
    price NUMERIC(10,2),
    user_id BIGINT,
    user_session TEXT
);

CREATE TABLE offset_metadata (
    id INT PRIMARY KEY DEFAULT 1,
    last_offset BIGINT NOT NULL
);


truncate f_events,offset_metadata restart identity

SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

CREATE PUBLICATION debezium_pub FOR ALL TABLES;


-- Run this periodically or hook into your monitoring
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots
WHERE pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) > 104857600;
-- alerts if lag exceeds 100MB





curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-schema-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "errors.log.include.messages": "true",
        "transforms": "unwrap",
        "topic.prefix": "cdc",
        "decimal.handling.mode": "double",
        "transforms.unwrap.drop.tombstones": "true",
        "confluent.topic.replication.factor": "1",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "errors.log.enable": "true",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "database.user": "myuser",
        "database.dbname": "ecommerce_events",
        "confluent.topic.bootstrap.servers": "kafka:29092",
        "plugin.name": "pgoutput",
        "database.port": "5432",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "key.converter.schemas.enable": "true",
        "database.hostname": "postgres",
        "database.password": "mypass",
        "value.converter.schemas.enable": "true",
        "value.converter.auto.register.schemas": "true",
        "name": "postgres-schema-connector",
        "transforms.unwrap.add.fields": "op,ts_ms",
        "errors.tolerance": "all",
        "table.include.list": "public.f_events",
        "key.converter.schema.registry.url": "http://schema-registry:8081"
}
  }'




docker exec -it spark_master /opt/spark/bin/spark-submit   --master spark://spark-master:7077   --conf spark.jars.ivy=/tmp/.ivy2   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0   /opt/spark/work-dir/spark-jobs/spark_streaming_job.py



docker exec -it spark_master bash

spark@ddc2927b5da1:/opt/spark/work-dir$ ls /opt/spark/jars | grep hadoop

sudo chmod -R 755 /home/elmir/de-project/data-binds/postgres

docker exec -it --user airflow airflow python -m pip install apache-airflow-providers-apache-spark

docker-compose --profile side --profile core up -d hue