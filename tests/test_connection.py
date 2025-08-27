import os
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, TimestampType, DoubleType, IntegerType
from pyiceberg.transforms import DayTransform

# 1. Defina as variáveis de ambiente para a conexão com o MinIO
#    Essas variáveis precisam ser as mesmas que o seu serviço Airflow usa
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'
os.environ['AWS_ENDPOINT_URL'] = 'http://minio:9000'
os.environ['AWS_S3_ALLOW_UNSAFE_REUSE'] = 'true' # Necessário para o fsspec

# 2. Defina as variáveis para o Catálogo Iceberg
ICEBERG_CATALOG_URI = "http://localhost:8181" # Corrigido para acessar via localhost
ICEBERG_WAREHOUSE = "s3a://bronze/warehouse"
TABLE_IDENTIFIER = "bronze.pc_metrics_test"

def test_iceberg_connection():
    """Testa a conexão e a escrita em uma tabela Iceberg."""
    print("Iniciando teste de conexão com o Iceberg...")
    try:
        # Carregar o catálogo do Iceberg
        catalog = load_catalog(
            "default_catalog",
            **{
                "uri": ICEBERG_CATALOG_URI,
                "warehouse": ICEBERG_WAREHOUSE,
                "s3.endpoint": os.environ['AWS_ENDPOINT_URL'],
                "s3.access-key-id": os.environ['AWS_ACCESS_KEY_ID'],
                "s3.secret-access-key": os.environ['AWS_SECRET_ACCESS_KEY'],
            }
        )
        print("✅ Catálogo Iceberg carregado com sucesso.")

        # Criar um esquema de teste
        schema = Schema(
            NestedField(1, "timestamp", TimestampType(), required=True),
            NestedField(2, "cpu_percent", DoubleType()),
            NestedField(3, "mem_percent", DoubleType()),
        )
        
        # Verificar e criar a tabela de teste
        if not catalog.table_exists(TABLE_IDENTIFIER):
            print(f"Tabela '{TABLE_IDENTIFIER}' não encontrada. Criando...")
            table = catalog.create_table(
                identifier=TABLE_IDENTIFIER,
                schema=schema,
                partition_spec=[("timestamp", DayTransform())],
            )
            print("✅ Tabela criada com sucesso.")
        else:
            print(f"Tabela '{TABLE_IDENTIFIER}' já existe. Carregando...")
            table = catalog.load_table(TABLE_IDENTIFIER)
            print("✅ Tabela carregada com sucesso.")

        # Criar um DataFrame de teste
        data = {
            'timestamp': [pd.Timestamp('2025-08-27 12:00:00', tz='UTC')],
            'cpu_percent': [25.5],
            'mem_percent': [60.2],
        }
        df = pd.DataFrame(data)

        # Escrever o DataFrame na tabela Iceberg
        table.overwrite(pa.Table.from_pandas(df))
        print("✅ Dados escritos na tabela com sucesso.")
        print(f"Teste de conexão concluído. Verifique o bucket '{ICEBERG_WAREHOUSE}' no seu MinIO.")

    except Exception as e:
        print(f"❌ Erro durante o teste de conexão: {e}")
        print("Por favor, verifique se os seus contêineres Docker estão em execução e as variáveis de ambiente estão corretas.")

if __name__ == "__main__":
    test_iceberg_connection()