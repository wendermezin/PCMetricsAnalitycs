# src/flows/monitor_flow.py
from prefect import flow
from src.tasks.data_tasks import collect_system_metrics, save_data_to_storage, process_to_bronze_iceberg
import time
import json
import time

@flow(name="PC System Monitoring Flow")
def pc_monitoring_flow():
    print("Iniciando a coleta de métricas...")
    
    metrics_data = collect_system_metrics()
    
    if metrics_data:
        print("Coleta de métricas concluída com sucesso.")
        
        # Salva o arquivo na landing
        landing_path = save_data_to_storage(metrics_data)
        
        if landing_path:
            print("Dados salvos na landing com sucesso.")
            
            # Processa e salva na tabela bronze Iceberg
            process_success = process_to_bronze_iceberg(landing_path)
            
            if process_success:
                print("Dados processados e salvos na camada bronze Iceberg com sucesso.")
            else:
                print("Falha ao salvar os dados na camada bronze Iceberg.")
        else:
            print("Falha ao salvar os dados na landing.")
    else:
        print("Falha na coleta de métricas.")

if __name__ == "__main__":
    while True:
        pc_monitoring_flow()
        time.sleep(30)