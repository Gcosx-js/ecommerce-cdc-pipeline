#!/bin/bash
set -e

JARS_DIR="./data-binds/spark/jars"
MAVEN="https://repo1.maven.org/maven2"
CONFLUENT="https://packages.confluent.io/maven"

mkdir -p "$JARS_DIR"

echo "Downloading JARs into $JARS_DIR..."

download() {
  local url=$1
  local filename=$(basename "$url")
  if [ -f "$JARS_DIR/$filename" ]; then
    echo "  [skip] $filename already exists"
  else
    echo "  [download] $filename"
    wget -q --show-progress -O "$JARS_DIR/$filename" "$url"
  fi
}

# Maven Central JARs
download "$MAVEN/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
download "$MAVEN/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar"
download "$MAVEN/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar"
download "$MAVEN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
download "$MAVEN/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar"
download "$MAVEN/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar"
download "$MAVEN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
download "$MAVEN/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar"
download "$MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar"
download "$MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar"
download "$MAVEN/org/apache/spark/spark-avro_2.12/3.5.3/spark-avro_2.12-3.5.3.jar"
download "$MAVEN/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar"
download "$MAVEN/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar"
download "$MAVEN/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar"

# Confluent JARs
download "$CONFLUENT/io/confluent/kafka-avro-serializer/8.2.0/kafka-avro-serializer-8.2.0.jar"
download "$CONFLUENT/io/confluent/kafka-schema-registry-client/8.2.0/kafka-schema-registry-client-8.2.0.jar"
download "$CONFLUENT/io/confluent/common-config/8.2.0/common-config-8.2.0.jar"
download "$CONFLUENT/io/confluent/common-utils/8.2.0/common-utils-8.2.0.jar"
#Required source CSV
echo "Downloading 2019-Dec dataset..."
wget -q --show-progress "https://data.rees46.com/datasets/marketplace/2019-Dec.csv.gz" -O py_services/2019-Dec.csv.gz
gunzip py_services/2019-Dec.csv.gz
echo "Dataset ready at py_services/2019-Dec.csv.gz"
echo ""
echo "Done. All JARs are in $JARS_DIR"
