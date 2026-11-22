FROM python:3.13-slim

# confluent-kafka (Apache 2.0) with Avro + Schema Registry support
RUN pip install --no-cache-dir --root-user-action=ignore "confluent-kafka[avro]==2.16.*"

RUN useradd --create-home --uid 10001 producer
WORKDIR /app
COPY --chown=producer order.avsc producer.py ./
USER producer

ENTRYPOINT ["python", "-u", "producer.py"]
