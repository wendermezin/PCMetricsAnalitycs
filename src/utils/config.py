# src/utils/config.py
import os

COLLECTION_FREQUENCY_SECONDS = 30

METRICS_LOG_FILE = "metrics_data.jsonl"

ICEBERG_DB_HOST = "localhost"
ICEBERG_DB_PORT = 5432
ICEBERG_DB_USER = "user"
ICEBERG_DB_PASSWORD = "password"
ICEBERG_DB_NAME = "pc_metrics_db"

# Para usar as variáveis de ambiente, você pode fazer:
# import os
# ICEBERG_DB_PASSWORD = os.getenv("ICEBERG_DB_PASSWORD", "default_password")

class Config:
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "your_minio_access_key")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "your_minio_secret_key")
    MINIO_ENDPOINT = "http://localhost:9000"
    BUCKET_NAME = "pc-metrics"
    ICEBERG_WAREHOUSE = f"s3a://{BUCKET_NAME}"