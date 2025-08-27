from airflow.hooks.base_hook import BaseHook
from minio import Minio

def get_minio_client(connection_id: str = 'minio_default'):
    conn = BaseHook.get_connection(connection_id)
    return Minio(
        endpoint=conn.host,
        access_key=conn.login,
        secret_key=conn.password,
        secure=False
    )