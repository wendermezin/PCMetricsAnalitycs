#!/bin/bash
set -e

# Inicializa DB
airflow db init

# Cria usuário admin (se não existir)
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

# Inicia scheduler em background
airflow scheduler &

# Inicia webserver em foreground
exec airflow webserver
