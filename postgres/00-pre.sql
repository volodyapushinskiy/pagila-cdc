-- pagila-schema.sql references the postgres role, create it if missing
CREATE ROLE postgres SUPERUSER LOGIN;
SELECT 'CREATE DATABASE airflow_meta' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_meta')\gexec
