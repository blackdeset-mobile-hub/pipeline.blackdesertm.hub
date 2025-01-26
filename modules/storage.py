import logging
import pandas as pd

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def save_dataframe_to_files(data: dict, parquet_file_name: str, csv_file_name: str) -> None:
    """
    Save a dictionary as a DataFrame into Parquet and CSV files.
    """
    df = pd.DataFrame(data)
    data_dir = Variable.get("data_dir")

    if "date" in df:
        df["date"] = pd.to_datetime(df["date"])

    df.to_parquet(f"{data_dir}/{parquet_file_name}", engine="pyarrow", compression="snappy")
    df.to_csv(f"{data_dir}/{csv_file_name}", index=False)

def upload_files_to_gcs(files_to_upload: list, bucket_name: str, bucket_path: str) -> None:
    """
    Upload scraped data files to Google Cloud Storage.
    """
    gcs_hook = GCSHook(gcp_conn_id="gcp_conn")
    data_dir = Variable.get("data_dir")

    for file in files_to_upload:
        gcs_destination_path = f"{bucket_path}/{file}"
        local_file_path = f"{data_dir}/{file}"

        gcs_hook.upload(bucket_name, gcs_destination_path, local_file_path)
        logger.info(f"Uploaded {file} to GCS bucket {bucket_name} at {gcs_destination_path}")

def load_files_to_bigquery(gcs_source_uri: str, dataset_id: str, table_id: str) -> BigQueryInsertJobOperator:
    """
    Load data from GCS into BigQuery for analysis.
    """
    logger.info(f"Preparing to load data from GCS URI: {gcs_source_uri} into BigQuery table: {dataset_id}.{table_id}")

    load_operator = BigQueryInsertJobOperator(
        task_id="load_files_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [gcs_source_uri],
                "destinationTable": {
                    "projectId": Variable.get("project_id"),
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "sourceFormat": "PARQUET",
                "autodetect": True
            }
        },
        gcp_conn_id="gcp_conn",
    )

    return load_operator
