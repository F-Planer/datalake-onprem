# Airflow with the Spark client: tasks run spark-submit (client mode)
# against the cluster at spark://spark-master:7077.
#
# Spark, jars (Iceberg, JDBC...) and Java come from our Spark image, so the
# versions are identical. Airflow's Python (3.10) matches the Spark image's:
# PySpark requires the same version on the driver and on the executors.
FROM datalake/spark:4.1.4 AS spark

FROM apache/airflow:3.3.2-python3.10
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH="${PATH}:/opt/spark/bin"
