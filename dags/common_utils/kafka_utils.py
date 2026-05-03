import json
import struct

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, BinaryType


def get_start_offsets(spark: SparkSession, topic: str, db: str, table: str, num_partitions: int) -> str:
    try:
        rows = spark.sql(f"""
            SELECT kafka_partition, MAX(kafka_offset) AS max_offset
            FROM {db}.{table}
            GROUP BY kafka_partition
        """).collect()

        if not rows:
            return "earliest"

        offsets = {str(r["kafka_partition"]): r["max_offset"] + 1 for r in rows}
        for i in range(num_partitions):
            offsets.setdefault(str(i), -2)

        return json.dumps({topic: offsets})
    except Exception:
        return "earliest"


def extract_schema_id_udf():
    return udf(
        lambda b: struct.unpack(">I", bytes(b[1:5]))[0] if b and len(b) >= 5 else None,
        IntegerType(),
    )


def extract_payload_udf():
    return udf(
        lambda b: bytes(b[5:]) if b and len(b) >= 5 else None,
        BinaryType(),
    )