from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from src.tasks.data_tasks import (
    collect_system_metrics,
    save_data_to_storage,
    process_to_bronze_iceberg
)

with DAG(
    dag_id="dag_pc_metrics_30s",
    start_date=pendulum.yesterday('UTC'),
    schedule="*/1 * * * *",
    catchup=False,
    tags=["pc_metrics", "data_ingestion"],
) as dag:
    
    collect_metrics_task = PythonOperator(
        task_id="collect_metrics",
        python_callable=collect_system_metrics,
    )

    save_data_task = PythonOperator(
        task_id="save_data",
        python_callable=save_data_to_storage,
        op_args=[collect_metrics_task.output],
    )

    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_to_bronze_iceberg,
        op_args=[save_data_task.output],
    )
    
    collect_metrics_task >> save_data_task >> process_data_task