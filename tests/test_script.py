import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.tasks.data_tasks import collect_system_metrics, save_data_to_storage, process_to_bronze_iceberg
import os

os.environ["MINIO_HOST_URI"] = "localhost:9000"
os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
os.environ["MINIO_SECRET_KEY"] = "minioadmin"
os.environ["MINIO_BUCKET"] = "landing"
os.environ["ICEBERG_CATALOG_URI"] = "http://localhost:8181"
os.environ["ICEBERG_WAREHOUSE"] = "s3a://pc-metrics/warehouse"

def run_local_test():
    """Simulates the data collection, save, and processing pipeline."""
    print("--- 1. Collecting system metrics ---")
    metrics_data = collect_system_metrics()
    if metrics_data is None:
        print("Test failed at the metric collection step.")
        return

    print("\n--- 2. Saving data to MinIO (Landing Zone) ---")
    landing_path = save_data_to_storage(metrics_data)
    if landing_path is None:
        print("Test failed at the data saving step.")
        return

    print(f"\n--- 3. Processing data from Landing Zone to the Bronze table ---")
    success = process_to_bronze_iceberg(landing_path)
    if not success:
        print("Test failed at the Iceberg processing step.")
        return

    print("\n✅ Local test completed successfully!")

# Run the test
if __name__ == "__main__":
    run_local_test()