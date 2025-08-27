#!/bin/bash

# Inicia o Iceberg REST em background
/start-iceberg.sh &

# Função para verificar se o REST está pronto
check_iceberg_ready() {
  until curl -s http://localhost:8181/v1/health | grep -q "OK"; do
    echo "Esperando Iceberg REST subir..."
    sleep 3
  done
}

# Chama a função de espera
check_iceberg_ready

# Cria o namespace 'bronze'
echo "Criando namespace 'bronze'..."
curl -X POST http://localhost:8181/v1/namespaces \
     -H "Content-Type: application/json" \
     -d '{"namespace": ["bronze"]}'

echo "Namespace 'bronze' criado com sucesso."

# Mantém o container ativo (Iceberg REST já está rodando em background)
wait
