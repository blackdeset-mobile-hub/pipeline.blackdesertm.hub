import logging
import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def upload_files_to_gcs(files_to_upload: list, bucket_name: str) -> None:
    """
    Upload scraped data files to Google Cloud Storage.
    """
    gcs_hook = GCSHook(gcp_conn_id="blackdesert-mobile-hub-key")
    data_dir = Variable.get("data_dir")

    for file in files_to_upload:
        gcs_destination_path = f"raw/{file}"
        local_file_path = f"{data_dir}/{file}"

        gcs_hook.upload(bucket_name, gcs_destination_path, local_file_path)
        logger.info(f"Uploaded {file} to GCS bucket {bucket_name} at {gcs_destination_path}")

def load_files_to_bigquery(bucket_name: str, dataset_id: str, table_id: str) -> BigQueryInsertJobOperator:
    """
    Load data from GCS into BigQuery for analysis.
    """
    current_time = pendulum.now()
    gcs_source_uri = f"gs://{bucket_name}/raw/{current_time.year}/{current_time.month}/{current_time.day}/*.parquet"
    
    logger.info(f"Preparing to load data from GCS URI: {gcs_source_uri} into BigQuery table: {dataset_id}.{table_id}")

    load_operator = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        configuration={  # BigQuery load configuration
            "load": {
                "sourceUris": [gcs_source_uri],
                "destinationTable": {
                    "projectId": "blackdesert-mobile-hub",
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "sourceFormat": "PARQUET",
                "autodetect": True
            }
        },
        gcp_conn_id="blackdesert-mobile-hub-key",
    )

    return load_operator
