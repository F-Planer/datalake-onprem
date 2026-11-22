#!/bin/sh
# Runs only on the first start of the Postgres volume.
# Creates one database + user for each service that shares this Postgres.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE polaris LOGIN PASSWORD '${POLARIS_DB_PASSWORD}';
CREATE DATABASE polaris OWNER polaris;
CREATE ROLE airflow LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
CREATE DATABASE airflow OWNER airflow;
SQL

# Polaris (relational-jdbc persistence) expects the schema to exist already.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname polaris <<SQL
CREATE SCHEMA polaris_schema AUTHORIZATION polaris;
SQL
