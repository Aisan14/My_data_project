-- Создаем схемы для сырых данных и для dbt
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- Даем права (если будете подключать Airflow под другим пользователем)
-- GRANT ALL PRIVILEGES ON DATABASE analytics_db TO airflow_user;