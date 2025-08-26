# src/tasks/data_tasks.py
from prefect import task
import psutil
from datetime import datetime
import json
import pandas as pd
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
import io
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestampType, DoubleType, IntegerType
from pyiceberg.transforms import DayTransform
import pandas as pd

@task
def collect_system_metrics():
    """
    Coleta métricas do sistema (CPU, memória, disco, rede).
    """
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        mem_info = psutil.virtual_memory()
        disk_usage = psutil.disk_usage('/')
        net_io = psutil.net_io_counters()

        metrics = {
            "timestamp": datetime.now().isoformat(),
            "cpu_percent": cpu_percent,
            "mem_percent": mem_info.percent,
            "disk_percent": disk_usage.percent,
            "network_sent_bytes": net_io.bytes_sent,
            "network_recv_bytes": net_io.bytes_recv,
            "cpu_freq_ghz": round(psutil.cpu_freq().current / 1000, 2)
        }
        
        print(f"Métricas coletadas: {json.dumps(metrics, indent=2)}")
        
        return metrics

    except Exception as e:
        print(f"Erro ao coletar métricas: {e}")
        return None

@task
def save_data_to_storage(metrics_data: dict):
    """
    Salva os dados de métricas no MinIO como um arquivo Parquet.
    """
    try:
        minio_client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        bucket_name = "landing"
        
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        df = pd.DataFrame([metrics_data])

        now = datetime.now()
        file_path = now.strftime("pc_metrics/%Y/%m/%d/%H/%M/%S.parquet")

        # Cria a tabela Arrow a partir do DataFrame
        table = pa.Table.from_pandas(df)

        # Escreve em memória (Parquet)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)

        # Converte para BytesIO (objeto legível pelo MinIO)
        import io
        data = buffer.getvalue().to_pybytes()
        data_stream = io.BytesIO(data)

        # Envia para o MinIO
        minio_client.put_object(
            bucket_name,
            file_path,
            data_stream,
            length=len(data),
            content_type='application/parquet'
        )
        
        print(f"Dados salvos em s3://{bucket_name}/{file_path}")
        return True

    except Exception as e:
        print(f"Erro ao salvar dados no MinIO: {e}")
        return False


@task
def process_to_bronze_iceberg(landing_path: str):
    """
    Lê o arquivo Parquet da landing (no MinIO) e insere de forma incremental na tabela bronze Iceberg.
    """
    try:
        # Configurar catálogo Iceberg (REST)
        catalog = load_catalog(
            "default_catalog",
            **{
                "uri": "http://localhost:8181",  # REST Catalog do Iceberg
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "minioadmin",
                "s3.secret-access-key": "minioadmin",
                "warehouse": "s3://warehouse/",
            }
        )

        # Ler parquet direto do MinIO
        df = pd.read_parquet(
            landing_path,
            storage_options={
                "key": "minioadmin",
                "secret": "minioadmin",
                "client_kwargs": {"endpoint_url": "http://localhost:9000"},
            },
        )

        # Garantir que a tabela existe
        try:
            table = catalog.load_table("bronze.pc_metrics")
        except:
            schema = Schema(
                NestedField(1, "timestamp", TimestampType(), required=True),
                NestedField(2, "cpu_percent", DoubleType()),
                NestedField(3, "mem_percent", DoubleType()),
                NestedField(4, "disk_percent", DoubleType()),
                NestedField(5, "network_sent_bytes", IntegerType()),
                NestedField(6, "network_recv_bytes", IntegerType()),
                NestedField(7, "cpu_freq_ghz", DoubleType()),
            )

            table = catalog.create_table(
                identifier="bronze.pc_metrics",
                schema=schema,
                partition_spec=[("timestamp", DayTransform())],
            )

        # Append no Iceberg (DataFrame → Arrow Table)
        import pyarrow as pa
        table.append(pa.Table.from_pandas(df))

        print("✅ Dados inseridos incrementalmente na tabela bronze.pc_metrics")
        return True

    except Exception as e:
        print(f"Erro ao processar e salvar na tabela Iceberg: {e}")
        return False