PC Metrics Analytics with a Data Lakehouse
This project implements a complete data pipeline to collect, process, and store PC system metrics. It's built on a Data Lakehouse architecture, using modern tools to demonstrate robust and scalable data ingestion across different layers.

🚀 Technologies Used
Prefect: Orchestrates the data pipeline, ensuring that data collection and processing tasks run reliably and on a schedule.

MinIO: An S3-compatible object storage server that acts as both the Landing Zone for raw data and the Warehouse for the Iceberg tables.

Apache Iceberg: An open-source table format for data lakes, providing ACID transactions and evolving schemas, ideal for the Bronze layer.

PyArrow/Pandas: Libraries for manipulating and processing data in Parquet format.

Docker Compose: A tool to orchestrate all services (MinIO, Prefect, and the Iceberg REST Catalog) in an isolated and easy-to-configure local environment.

🏗️ Pipeline Architecture
The pipeline operates in three main stages:

Data Collection: A Prefect task collects system metrics (CPU, memory, disk, network) using the psutil library. The data is formatted into a dictionary.

Landing Zone (MinIO): The next task receives the raw data, converts it to the Parquet format, and stores the file in MinIO under the landing/pc_metrics/ path.

Bronze Layer (Iceberg): A final task reads the Parquet file from the Landing Zone and appends it to an Iceberg table named bronze.pc_metrics. This table is managed by the Iceberg REST Catalog and stored within MinIO.

This layered approach allows for data validation and transformation before moving to subsequent stages (Silver and Gold), ensuring data quality and reliability.

⚙️ How to Run the Project
This guide will help you set up and run the pipeline using docker-compose.

Prerequisites
Docker Desktop installed and running.

1. Environment Setup
Open your terminal in the root of the project (where the docker-compose.yml file is located) and run the following command:

docker-compose up -d

This command will download the necessary images and start the following services:

minio_server: MinIO on port 9000 (Console on port 9001).

rest_catalog: The Iceberg REST Catalog on port 8181.

prefect_server: The Prefect UI server on port 4200.

prefect_worker: The Prefect worker, ready to execute tasks.

2. Verify Services
You can access the user interfaces to check if everything is running correctly:

MinIO Console: http://localhost:9001 (User/Password: minioadmin/minioadmin)

Prefect UI: http://localhost:4200

3. Run the Workflow
With the services running, you can now execute the Prefect workflow. The main flow is defined in src/flows/monitor_flow.py.

First, make sure your Python dependencies are installed:

pip install -r requirements.txt

prefect deploy --name "pc-metrics-pipeline" --flow-name "monitor-flow" --work-pool "default-agent-pool" --entrypoint "src/flows/monitor_flow.py:monitor_flow" --override "is_schedule=False" --no-prompt

After deployment, the Prefect worker will start executing the task. You can monitor the progress in real-time on the Prefect UI.
