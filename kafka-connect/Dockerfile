# Kafka Connect with Debezium (official image) + the Confluent Avro converter.
# Confluent's converter and schema registry client are Apache 2.0; their
# registry server is not, which is why we use Karapace.

FROM maven:3.10.0-eclipse-temurin-21 AS converter
WORKDIR /build
COPY pom.xml .
# Libraries that Kafka Connect already ships are excluded to avoid conflicts.
RUN mvn -q -B dependency:copy-dependencies \
      -DoutputDirectory=/build/jars \
      -DincludeScope=runtime \
      -DexcludeGroupIds=org.apache.kafka,org.slf4j \
    && ls /build/jars

FROM quay.io/debezium/connect:3.7.1.Final
COPY --from=converter /build/jars /kafka/connect/confluent-avro-converter/
