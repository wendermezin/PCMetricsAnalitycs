# PC Monitoring and Analytics Platform

Bem-vindo ao meu projeto pessoal de engenharia de dados, focado em monitorar e analisar métricas de um computador local. Esta plataforma utiliza uma arquitetura de dados moderna e open-source para coletar, orquestrar, armazenar e visualizar dados de desempenho do PC.

## Arquitetura

O projeto é baseado em uma arquitetura de dados sem contêineres, com os seguintes componentes:

- **ETL:** Python com `psutil` para coleta de dados e Apache Iceberg para formato de tabela.
- **Orchestration:** Prefect para agendamento e execução de fluxos de trabalho.
- **BI - Analytics:** Apache Superset para análise e dashboards históricos.
- **Observability:** Grafana para visualização em tempo real.
- **Catalog:** DataHub para descoberta de dados e metadados.

## Como Rodar o Projeto

### Pré-requisitos

- Python 3.8+
- Os serviços de backend (Prefect, Superset, Grafana, DataHub) devem ser instalados e rodados localmente.

### 1. Configurar o Ambiente

1. Clone este repositório.
2. Abra o projeto no VS Code.
3. Abra o terminal integrado e crie um ambiente virtual:
   ```bash
   python -m venv venv