# Pipeline for Black Desert Mobile Hub

This project contains pipelines for scraping and processing item information from the Black Desert Mobile website. It uses Apache Airflow with Docker Compose for orchestration and task management.

## Project Overview

The repository includes the following DAGs:

### 1. `scrape_and_store_content_dag`
- **Purpose**: Scrapes the latest item information from the Black Desert Mobile website and stores it.
- **Workflow**:
  1. Scrapes data from the website.
  2. Stores data in local storage as Parquet and CSV files.
  3. Uploads the files to Google Cloud Storage (GCS).
  4. Loads the data into BigQuery for analysis.

### 2. `backup_historical_data_dag`
- **Purpose**: Backs up historical item information to ensure data availability.
- **Workflow**:
  1. Scrapes historical data.
  2. Saves the data locally as Parquet and CSV files.
  3. Uploads the backup data to GCS.
  4. Loads the backup data into BigQuery.

## Setup and Usage

### Prerequisites
- Docker and Docker Compose installed.
- GCP service account with the necessary permissions.
- BigQuery and GCS configured with appropriate datasets and buckets.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/leewr9/pipeline.blackdesertm.hub.git
   cd pipeline.blackdesertm.hub
   ```

2. Set up the service account JSON file:
   - Place your GCP service account JSON file as `key.json` in the `/opt/airflow/config/` directory.

3. Set the environment variables in the `docker-compose.yml` file:
   ```yaml
    environment:
      - AIRFLOW_CONN_GCP_CONN=google-cloud-platform://?key_path=<key_file_path>&scope=https://www.googleapis.com/auth/cloud-platform&project=<project_id>&num_retries=5
      - AIRFLOW_VAR_PROJECT_ID=<your_project_id>
      - AIRFLOW_VAR_BUCKET_NAME=<your_bucket_name>
   ```

### Running Airflow
1. Start the Airflow services using Docker Compose:
   ```bash
   docker-compose up -d
   ```

2. Access the Airflow web interface at [http://localhost:8080](http://localhost:8080).

3. Log in with the default credentials:
   - Username: `airflow`
   - Password: `airflow`

### DAG Execution
- Trigger the desired DAG (`scrape_and_store_content_dag` or `backup_historical_data_dag`) manually or via a schedule.

## Repository Structure
```plaintext
pipeline.blackdesertm.hub/
├── dags/
│   ├── scrape_and_store_content_dag.py
│   ├── backup_historical_data_dag.py
├── modules/
│   ├── scraper.py
│   ├── storage.py
└── docker-compose.yaml
```

## Troubleshooting
- Ensure Airflow connections and variables are correctly configured.
- Check logs in the Airflow web interface for task-specific errors.

For further assistance, refer to the [Apache Airflow documentation](https://airflow.apache.org/docs/).
