from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.avro.functions import from_avro

from airflow import DAG
from airflow.operators.python import PythonOperator

from common_utils.spark_session import get_spark
from common_utils.kafka_utils import get_start_offsets, extract_schema_id_udf, extract_payload_udf
from common_utils.schema_utils import get_schema_str
from common_utils.hive_utils import create_database, create_external_table, repair_table
from common_utils.config import KAFKA_BROKER, MINIO_WAREHOUSE


APP_NAME        = "ecommerce-cdc-batch"
TOPIC           = "cdc.public.f_events"
DLQ_TOPIC       = "cdc.public.f_events.dlq"
VALID_SCHEMA_ID = 7
NUM_PARTITIONS  = 6
HIVE_DB         = "ecommerce"
HIVE_TABLE      = "f_events"
MINIO_PATH      = f"{MINIO_WAREHOUSE}/ecommerce/f_events"
STAGING_PATH    = f"{MINIO_WAREHOUSE}/staging/f_events"

HIVE_COLUMNS = """
    id              BIGINT,
    event_time      TIMESTAMP,
    event_type      STRING,
    product_id      BIGINT,
    category_id     BIGINT,
    category_code   STRING,
    brand           STRING,
    price           DOUBLE,
    user_id         BIGINT,
    user_session    STRING,
    op              STRING,
    ts_ms           BIGINT,
    kafka_offset    BIGINT,
    kafka_partition INT
"""


def task_retrieve_data(**_) -> None:
    spark = get_spark(APP_NAME)
    offsets = get_start_offsets(spark, TOPIC, HIVE_DB, HIVE_TABLE, NUM_PARTITIONS)
    print(f"Starting offsets: {offsets}")

    raw_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", offsets)
        .option("endingOffsets", "latest")
        .load()
    )

    print(f"Total messages read: {raw_df.count()}")
    raw_df.write.mode("overwrite").parquet(f"{STAGING_PATH}/raw")
    spark.stop()


def task_validate_schema(**_) -> None:
    spark = get_spark(APP_NAME)
    raw_df = spark.read.parquet(f"{STAGING_PATH}/raw")

    tagged_df = (
        raw_df
        .withColumn("schema_id", extract_schema_id_udf()(F.col("value")))
        .withColumn("avro_payload", extract_payload_udf()(F.col("value")))
        .withColumn("kafka_offset", F.col("offset"))
        .withColumn("kafka_partition", F.col("partition"))
    )

    valid_df = tagged_df.filter(F.col("schema_id") == VALID_SCHEMA_ID)
    dlq_df   = tagged_df.filter(F.col("schema_id") != VALID_SCHEMA_ID)

    print(f"Valid: {valid_df.count()} | DLQ: {dlq_df.count()}")

    if dlq_df.count() > 0:
        (
            dlq_df.select("key", "value").write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("topic", DLQ_TOPIC)
            .save()
        )
        print("DLQ messages sent.")

    valid_df.write.mode("overwrite").parquet(f"{STAGING_PATH}/valid")
    spark.stop()


def task_create_hive_table(**_) -> None:
    spark = get_spark(APP_NAME)
    create_database(spark, HIVE_DB)
    create_external_table(spark, HIVE_DB, HIVE_TABLE, HIVE_COLUMNS, MINIO_PATH)
    print("Hive external table ready.")
    spark.stop()

from common_utils.spark_session import get_spark

def task_cleanup(**_) -> None:
    spark = get_spark(APP_NAME)
    uri = spark._jvm.java.net.URI(STAGING_PATH)
    spark._jvm.org.apache.hadoop.fs.FileSystem \
        .get(uri, spark._jsc.hadoopConfiguration()) \
        .delete(spark._jvm.org.apache.hadoop.fs.Path(STAGING_PATH), True)
    print("Staging cleaned.")
    spark.stop()

def task_populate_data(**_) -> None:
    spark = get_spark(APP_NAME)
    schema_str = get_schema_str(VALID_SCHEMA_ID)
    valid_df   = spark.read.parquet(f"{STAGING_PATH}/valid")

    flat_df = (
        valid_df
        .select(
            from_avro(F.col("avro_payload"), schema_str).alias("data"),
            F.col("kafka_offset"),
            F.col("kafka_partition"),
        )
        .select(
            F.col("data.id"),
            (F.col("data.event_time") / 1_000_000).cast(TimestampType()).alias("event_time"),
            F.col("data.event_type").alias("event_type"),
            F.col("data.product_id").alias("product_id"),
            F.col("data.category_id").alias("category_id"),
            F.col("data.category_code").alias("category_code"),
            F.col("data.brand").alias("brand"),
            F.col("data.price").alias("price"),
            F.col("data.user_id").alias("user_id"),
            F.col("data.user_session").alias("user_session"),
            F.col("data.__op").alias("op"),
            F.col("data.__ts_ms").alias("ts_ms"),
            F.col("kafka_offset"),
            F.col("kafka_partition"),
            F.to_date((F.col("data.event_time") / 1_000_000).cast(TimestampType())).alias("date"),
        )
    )

    flat_df.write.mode("append").partitionBy("date").parquet(MINIO_PATH)
    repair_table(spark, HIVE_DB, HIVE_TABLE)
    print("Pipeline complete.")
    task_cleanup()

with DAG(
    dag_id="streaming_layer_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    t1 = PythonOperator(task_id="retrieve_data",     python_callable=task_retrieve_data)
    t2 = PythonOperator(task_id="validate_schema",   python_callable=task_validate_schema)
    t3 = PythonOperator(task_id="create_hive_table", python_callable=task_create_hive_table)
    t4 = PythonOperator(task_id="populate_data",     python_callable=task_populate_data)

    t1 >> t2 >> t3 >> t4