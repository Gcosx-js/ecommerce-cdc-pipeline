from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, to_json, struct
from pyspark.sql.avro.functions import from_avro
import requests
import struct as pystruct

# Fetch current valid schema and its ID
schema_response = requests.get(
    "http://schema-registry:8081/subjects/cdc.public.f_events-value/versions/latest"
)
schema_json      = schema_response.json()
avro_schema      = schema_json["schema"]
VALID_SCHEMA_ID  = 7

print(f"=== Accepting schema ID: {VALID_SCHEMA_ID} ===")

spark = SparkSession.builder \
    .appName("debug-f-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "cdc.public.f_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract schema ID from bytes 2-5 (big-endian int)
# byte 0 = magic byte, bytes 1-4 = schema id
df_with_schema_id = raw_df.select(
    col("value"),
    col("offset"),
    col("timestamp"),
    expr("cast(conv(hex(substring(value, 2, 4)), 16, 10) as int)").alias("schema_id")
)

# Split based on schema ID match
good_df = df_with_schema_id \
    .filter(col("schema_id") == VALID_SCHEMA_ID) \
    .select(
        from_avro(
            expr("substring(value, 6)"),  # skip 5 bytes (magic + schema id)
            avro_schema
        ).alias("data"),
        col("offset"),
        col("timestamp")
    ) \
    .select("data.*", "offset", "timestamp")

dlq_df = df_with_schema_id \
    .filter(col("schema_id") != VALID_SCHEMA_ID) \
    .select(
        lit(None).cast("string").alias("key"),
        to_json(struct(
            col("offset"),
            col("timestamp"),
            col("schema_id"),
            col("value")
        )).alias("value")
    )

# Good → console for now
good_query = good_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .queryName("good_records") \
    .outputMode("append") \
    .start()

# Bad → DLQ topic
dlq_query = dlq_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "cdc.public.f_events.dlq") \
    .option("checkpointLocation", "/tmp/checkpoint/dlq") \
    .queryName("dlq_records") \
    .outputMode("append") \
    .start()

good_query.awaitTermination()