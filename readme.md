# Ecommerce CDC Data Pipeline

A full end-to-end DataOps pipeline built on a single machine using Docker Compose. Simulates a real ecommerce event flow — from raw data ingestion through Change Data Capture, stream processing, data lake storage, and ad-hoc querying.

---

## Architecture

```
CSV / JSON
    │
    ▼
Python Insert Service
    │  Batch inserts via psycopg2
    ▼
PostgreSQL (WAL enabled)
    │  Debezium reads WAL (wal_level = logical)
    ▼
Kafka  ◄──── Schema Registry (Avro contract enforcement)
    │  Topic: cdc.public.f_events (6 partitions)
    │  DLQ:   cdc.public.f_events.dlq
    ▼
Airflow DAG (every 2 min, max_active_runs=1)
    │
    ├── retrieve_data       → reads Kafka by offset, writes raw Parquet to staging
    ├── validate_schema     → checks Confluent schema ID, routes invalid msgs to DLQ
    ├── create_hive_table   → creates external table in Hive Metastore if not exists
    └── populate_data       → deserializes Avro, writes partitioned Parquet to MinIO,
                              runs MSCK REPAIR TABLE, cleans staging
    ▼
MinIO (S3-compatible data lake)
    │  s3a://hive-warehouse/ecommerce/f_events/date=YYYY-MM-DD/
    │  Format: Snappy-compressed Parquet, partitioned by date
    ▼
Hive Metastore
    │  External table: ecommerce.f_events
    ▼
Trino ──► Hue (SQL interface)
```

---

## Key Design Decisions

**Offset-based deduplication** — Kafka offsets are stored alongside each row (`kafka_offset`, `kafka_partition`). On each DAG run, `get_start_offsets()` queries Hive for `MAX(kafka_offset)` per partition and resumes from `offset + 1`. Re-triggering the DAG on the same data produces zero duplicates.

**Schema contract enforcement** — Every Kafka message carries a 5-byte Confluent wire format header. The pipeline extracts the schema ID from bytes `[1:5]`, validates it against `VALID_SCHEMA_ID = 7` (fetched from Schema Registry), and routes non-conforming messages to a DLQ topic before any processing begins. Spark will not deserialize data that fails the contract.

**Staging layer** — Raw Kafka messages are written to a staging path in MinIO before schema validation. This decouples reading from Kafka and processing, makes reruns cheaper, and gives a recoverable checkpoint if any downstream task fails.

**External Hive table** — The table is `CREATE EXTERNAL TABLE IF NOT EXISTS`, partitioned by `date`. Partition discovery runs via `MSCK REPAIR TABLE` after each write. Trino queries the table directly through Hive Metastore without any additional catalog configuration.

**Single PostgreSQL instance** — One PostgreSQL container serves the application data (`f_events`), Airflow metadata, and Hive Metastore. WAL logical replication is enabled only on the application schema.

**Kafka KRaft mode** — Kafka runs without ZooKeeper using KRaft consensus (`KAFKA_PROCESS_ROLES: broker,controller`). Simplifies the stack and removes a dependency.

**Docker Compose profiles** — Services are split into `core` (pipeline) and `side` (tooling). This allows running the full pipeline with minimal resources, and adding UI tools only when needed.

**Jupyter PySpark** — A `quay.io/jupyter/pyspark-notebook` container is included for ad-hoc exploration and prototyping directly against the Spark cluster. Shares the same extra JARs volume as the workers.

---

## Stack

| Service | Image | Version              | Profile    | Port(s) |
|---|---|----------------------|------------|---|
| PostgreSQL | `postgres` | 15                   | core, side | 5432 |
| pgAdmin | `dpage/pgadmin4` | 9.9               | side       | 8080 |
| Apache Kafka (KRaft) | `confluentinc/cp-kafka` | 7.6.1                | core       | 9092 |
| Schema Registry | `confluentinc/cp-schema-registry` | 7.6.1                | core       | 8081 |
| Kafka Connect + Debezium | `confluentinc/cp-kafka-connect` | 7.6.1                | side       | 8083 |
| Kafka UI | `provectuslabs/kafka-ui` | 0.7.2                | side       | 8090 |
| Spark Master | custom build | 3.5.3                | core       | 7077, 8085 |
| Spark Worker 1 | custom build | 3.5.3                | core       | 8086 |
| Spark Worker 2 | custom build | 3.5.3                | core       | 8087 |
| Jupyter PySpark | `quay.io/jupyter/pyspark-notebook` | spark-3.5.3          | side       | 8888 |
| MinIO | `minio/minio` | 2025-09-07           | core       | 9000, 9001 |
| Hive Metastore | `apache/hive` | 4.0.0                | core       | 9083 |
| Trino | `trinodb/trino` | 435                  | core       | 8088 |
| Hue | `gethue/hue` | tag: 20251008-140101 | side       | 8889 |
| Airflow | custom build | standalone           | core       | 8091 |

**Docker Compose profiles:**
- `core` — minimum set to run the full pipeline (PostgreSQL, Kafka, Spark, MinIO, Hive, Trino, Airflow, Jupyter)
- `side` — optional tooling (pgAdmin, Kafka Connect UI, Kafka UI, Hue)

```bash
# Run full pipeline only
docker compose --profile core up -d

# Run everything including UI tools
docker compose --profile core --profile side up -d
```

All services run on a single machine via Docker Compose.

---

## Project Structure

```
de-project/
├── dags/
│   ├── streaming_layer_dag.py       # Airflow DAG definition (4 tasks)
│   └── common_utils/
│       ├── config.py                # All service endpoints and credentials
│       ├── kafka_utils.py           # Offset resolution + Avro wire format UDFs
│       ├── schema_utils.py          # Schema Registry client
│       ├── hive_utils.py            # DDL helpers (CREATE DATABASE, CREATE TABLE, REPAIR)
│       └── spark_session.py         # SparkSession factory with S3A + Hive config
├── spark-jobs/
│   └── spark_streaming_job.py       # Standalone Spark job (alternative entrypoint)
├── py_services/
│   ├── pg_insert_service.py         # Batch insert service for stress testing
│   └── pg_upsert_service.py         # Upsert variant
├── data-binds/                      # Docker volume mounts
│   ├── kafka/                       # Kafka log segments
│   ├── minio/                       # Parquet files, partitioned by date
│   ├── spark/jars/                  # Extra JARs (Kafka, Avro, S3A, Hadoop)
│   ├── hive/                        # Hive config + PostgreSQL JDBC driver
│   └── hue/                         # Hue configuration
├── trino-catalog/
│   └── hive.properties              # Trino → Hive Metastore connector config
├── docker-compose.yml
├── Dockerfile                       # Base image
├── Dockerfile.airflow               # Airflow image with Python deps
├── setup.sh                         # First-time environment setup
└── requirements.txt
```

---

## DAG: `streaming_layer_dag`

Schedule: `*/2 * * * *` | `max_active_runs=1` | `catchup=False`

```
retrieve_data → validate_schema → create_hive_table → populate_data
```

| Task | What it does |
|---|---|
| `retrieve_data` | Resolves start offsets from Hive, reads Kafka batch, writes raw Parquet to staging |
| `validate_schema` | Extracts Confluent schema ID from wire format, filters valid/invalid, sends DLQ |
| `create_hive_table` | Creates `ecommerce.f_events` external table if not exists |
| `populate_data` | Deserializes Avro payload, flattens schema, appends partitioned Parquet, repairs table, cleans staging |

---

## Data Contract

Kafka messages use Confluent Avro wire format:

```
[0x00] [schema_id: 4 bytes big-endian] [avro payload]
```

Schema ID `7` is the registered contract for `f_events`. Any message with a different schema ID is routed to `cdc.public.f_events.dlq` and excluded from processing.

Schema is fetched at runtime from Schema Registry:
```
GET http://schema-registry:8081/schemas/ids/7
```

---

## Output Schema

Table: `ecommerce.f_events` — partitioned by `date`

| Column | Type | Source |
|---|---|---|
| `id` | BIGINT | Avro payload |
| `event_time` | TIMESTAMP | Avro (microseconds → timestamp) |
| `event_type` | STRING | Avro payload |
| `product_id` | BIGINT | Avro payload |
| `category_id` | BIGINT | Avro payload |
| `category_code` | STRING | Avro payload |
| `brand` | STRING | Avro payload |
| `price` | DOUBLE | Avro payload |
| `user_id` | BIGINT | Avro payload |
| `user_session` | STRING | Avro payload |
| `op` | STRING | Debezium `__op` (c/u/d/r) |
| `ts_ms` | BIGINT | Debezium `__ts_ms` |
| `kafka_offset` | BIGINT | Kafka metadata |
| `kafka_partition` | INT | Kafka metadata |
| `date` | DATE | Partition column |

---

## Getting Started

**Prerequisites:** Docker, Docker Compose, 16GB RAM recommended

```bash
# Clone the repo
git clone <repo-url>
cd de-project

# First-time setup (downloads JARs for Spark dependencies + dataset)
chmod +x setup.sh && ./setup.sh
 
# Dataset will be downloaded automatically to py_services/2019-Dec.csv
# Source: https://data.rees46.com/datasets/marketplace/2019-Dec.csv.gz

# Start all services
docker-compose --profile side --profile core up -d

# Trigger the DAG manually or wait for the 2-minute schedule
# Access points:
#   Airflow:         http://localhost:8091  (admin / admin)
#   Jupyter:         http://localhost:8888  (token: dev)
#   Spark Master UI: http://localhost:8085
#   Spark Worker 1:  http://localhost:8086
#   Spark Worker 2:  http://localhost:8087
#   MinIO Console:   http://localhost:9001  (minioadmin / minioadmin)
#   Trino:           http://localhost:8088
#   Hue:             http://localhost:8889  (side profile only)
#   Kafka UI:        http://localhost:8090  (side profile only)
#   pgAdmin:         http://localhost:8080  (side profile only)
#   Schema Registry: http://localhost:8081
#   Kafka Connect:   http://localhost:8083  (side profile only)
```

---

## Stress Test Results

Tested on a single local machine with 10 million ecommerce rows (clean run, zero pre-existing data):

| Stage | Result |
|---|---|
| Python → PostgreSQL (10M rows, batch=2.5M) | 156 seconds |
| Debezium CDC lag (first 2.5M batch) | 30 seconds |
| Airflow DAG — Kafka → MinIO → Hive | 2 min 22 sec |
| Trino `COUNT(*)` on 10M rows | 2.7 seconds |
| Duplicate rows | 0 |
| Peak RAM across all containers | ~11 GB |

DAG task breakdown for 10M rows:

| Task | Duration |
|---|---|
| `retrieve_data` | 32 sec |
| `validate_schema` | 1 min 16 sec |
| `create_hive_table` | 3 sec |
| `populate_data` | 27 sec |

> `validate_schema` is the current bottleneck — it performs two `count()` actions on the full dataset for valid/DLQ reporting. This will be optimized in a future iteration.

---

## Known Limitations

- `validate_schema` triggers two Spark actions (`valid_df.count()` and `dlq_df.count()`) which accounts for ~53% of total DAG time. Can be replaced with a single pass accumulator pattern.
- Single PostgreSQL instance shared across app, Airflow, and Hive Metastore — not suitable for production isolation.
- No retention policy on MinIO — partitions accumulate indefinitely.
