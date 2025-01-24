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
2. Set up environment variables in `.env` (if required):
   - Example `.env` file:
     ```env
     AIRFLOW_UID=50000
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

### DAG Configuration
- Airflow Connections:
  - `blackdesert-mobile-hub-key`: Connection to Google Cloud Platform.

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

## Key Points
- **Data Storage**: Parquet and CSV files are stored locally before uploading to GCS.
- **GCP Project**: `blackdesert-mobile-hub`.
- **GCS Bucket**: `blackdesert-mobile-hub-scraping-data-bucket`.
- **BigQuery Dataset**: `blackdesert_mobile_hub_data`.

## Troubleshooting
- Ensure Airflow connections and variables are correctly configured.
- Check logs in the Airflow web interface for task-specific errors.

For further assistance, refer to the [Apache Airflow documentation](https://airflow.apache.org/docs/).

