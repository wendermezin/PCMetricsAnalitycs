# src/tasks/data_tasks.py
import psutil
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from minio import Minio
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, TimestampType, DoubleType, IntegerType
from pyiceberg.transforms import DayTransform
import os


def get_minio_client() -> Minio:
    """Creates and returns a MinIO client."""
    return Minio(
        endpoint=Variable.get("MINIO_HOST_URI"),
        access_key=Variable.get("MINIO_ACCESS_KEY"),
        secret_key=Variable.get("MINIO_SECRET_KEY"),
        secure=False
    )


def collect_system_metrics() -> dict:
    """Collects system metrics and returns them as a dictionary."""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        mem_info = psutil.virtual_memory()
        disk_usage = psutil.disk_usage('/')
        net_io = psutil.net_io_counters()

        metrics = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cpu_percent": float(cpu_percent),
            "mem_percent": float(mem_info.percent),
            "disk_percent": float(disk_usage.percent),
            "network_sent_bytes": int(net_io.bytes_sent),
            "network_recv_bytes": int(net_io.bytes_recv),
            "cpu_freq_ghz": round(psutil.cpu_freq().current / 1000, 2)
        }
        return metrics

    except Exception as e:
        print(f"Error collecting metrics: {e}")
        # Return default values in case of failure
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cpu_percent": 0.0,
            "mem_percent": 0.0,
            "disk_percent": 0.0,
            "network_sent_bytes": 0,
            "network_recv_bytes": 0,
            "cpu_freq_ghz": 0.0
        }


def save_data_to_storage(metrics_data: dict) -> str:
    """Saves metrics as Parquet to MinIO and returns the full path."""
    if not metrics_data:
        raise AirflowFailException("Empty metrics data. Cannot save to MinIO.")

    # Ensure all fields exist
    fields_defaults = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cpu_percent": 0.0,
        "mem_percent": 0.0,
        "disk_percent": 0.0,
        "network_sent_bytes": 0,
        "network_recv_bytes": 0,
        "cpu_freq_ghz": 0.0
    }
    for field, default in fields_defaults.items():
        metrics_data.setdefault(field, default)

    try:
        minio_client = get_minio_client()
        bucket_name = Variable.get("MINIO_BUCKET")
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        df = pd.DataFrame([metrics_data])

        # Adjust data types
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['cpu_percent'] = df['cpu_percent'].astype(float)
        df['mem_percent'] = df['mem_percent'].astype(float)
        df['disk_percent'] = df['disk_percent'].astype(float)
        df['network_sent_bytes'] = df['network_sent_bytes'].astype(int)
        df['network_recv_bytes'] = df['network_recv_bytes'].astype(int)
        df['cpu_freq_ghz'] = df['cpu_freq_ghz'].astype(float)

        now = datetime.now(timezone.utc)
        file_path = now.strftime("landing/%Y/%m/%d/%H/%M/%S.parquet")

        # Write Parquet to a buffer
        table = pa.Table.from_pandas(df)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)
        data_stream = io.BytesIO(buffer.getvalue().to_pybytes())

        minio_client.put_object(
            bucket_name,
            file_path,
            data_stream,
            length=data_stream.getbuffer().nbytes,
            content_type='application/parquet'
        )

        full_s3_path = f"s3a://{bucket_name}/{file_path}"
        print(f"Data saved to {full_s3_path}")
        return full_s3_path

    except Exception as e:
        print(f"Error saving data to MinIO: {e}")
        raise AirflowFailException(f"Failed to save data to MinIO: {e}")


def process_to_bronze_iceberg(parquet_path: str):
    """Processes Parquet from landing and inserts it into the Iceberg bronze table."""
    try:
        # Load the catalog using environment variables from Docker Compose
        catalog = load_catalog(
            name='pc_metrics_catalog',
            type='rest',
            uri=os.environ.get("ICEBERG_CATALOG_URI"),
            # Ensure the warehouse path is correct for the pyiceberg client
            warehouse=os.environ.get("ICEBERG_WAREHOUSE"),
            s3_endpoint=os.environ.get("AWS_ENDPOINT_URL_S3"),
            s3_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            s3_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            s3_path_style_access=os.environ.get("AWS_S3_PATH_STYLE_ACCESS")
        )
        
        minio_client = get_minio_client()
        bucket_name = Variable.get("MINIO_BUCKET")
        
        # Read the Parquet file from MinIO
        file_object = minio_client.get_object(bucket_name, parquet_path.replace(f's3a://{bucket_name}/', ''))
        table_from_parquet = pq.read_table(io.BytesIO(file_object.read()))
        df = table_from_parquet.to_pandas()
        
        # Define the schema for the Iceberg table
        schema = Schema(
            NestedField(1, "timestamp", TimestampType(), required=True),
            NestedField(2, "cpu_percent", DoubleType()),
            NestedField(3, "mem_percent", DoubleType()),
            NestedField(4, "disk_percent", DoubleType()),
            NestedField(5, "network_sent_bytes", IntegerType()),
            NestedField(6, "network_recv_bytes", IntegerType()),
            NestedField(7, "cpu_freq_ghz", DoubleType()),
        )

        table_identifier = "bronze.pcmetrics"

        # Create namespace and table if they don't exist
        if not catalog.namespace_exists("bronze"):
            catalog.create_namespace("bronze")

        if not catalog.table_exists(table_identifier):
            table = catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=[("timestamp", DayTransform())],
            )
        else:
            table = catalog.load_table(table_identifier)
            
        # Overwrite the table with the new data
        table.overwrite(table_from_parquet)
        print("✅ Data successfully inserted into pcmetrics.bronze table (Full Overwrite).")

    except Exception as e:
        print(f"Error processing and saving to Iceberg table: {e}")
        raise AirflowFailException(f"Failed to process Iceberg table: {e}")