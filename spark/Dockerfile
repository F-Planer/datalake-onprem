# POC Spark image: official image + Iceberg, Kafka and Avro jars and the
# SQL Server JDBC driver (MIT).
# The jars come from Maven Central and are verified against the SHA-1 published there.
FROM apache/spark:4.1.4-java21-python3

ARG SPARK_VERSION=4.1.4
ARG ICEBERG_VERSION=1.12.0
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.1
ARG MSSQL_JDBC_VERSION=13.6.2.jre11

USER root
RUN set -eu; \
    cd /opt/spark/jars; \
    M=https://repo1.maven.org/maven2; \
    for path in \
      org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
      org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_VERSION}.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_VERSION}.jar \
      org/apache/spark/spark-avro_2.13/${SPARK_VERSION}/spark-avro_2.13-${SPARK_VERSION}.jar \
      org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
      org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
      com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar; \
    do \
      file=$(basename "$path"); \
      curl -fsSL -o "$file" "$M/$path"; \
      echo "$(curl -fsSL "$M/$path.sha1" | cut -c1-40)  $file" | sha1sum -c -; \
    done; \
    mkdir -p /checkpoints /opt/spark/jobs; \
    chown spark:spark /checkpoints /opt/spark/jobs

COPY --chown=spark:spark conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY --chown=spark:spark conf/log4j2.properties /opt/spark/conf/log4j2.properties
COPY --chown=spark:spark --chmod=755 submit-job.sh /opt/spark/submit-job.sh

USER spark
WORKDIR /opt/spark/work-dir
