import os
import logging
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

from modules import scraper, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@dag(
    description="DAG for scraping content using Airflow Variables",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 24),
    catchup=False
)
def scrape_and_store_content_dag():

    @task
    def scrape_content() -> list:
        current_content_no = int(Variable.get("content_no", default_var=385))
        saved_file_paths = []
        current_time = pendulum.now()

        logger.info(f"Starting scraping from content_no: {current_content_no}")
        failure_count = 0
        while failure_count < 3:
            post = scraper.fetch_blackdesert_post(current_content_no)
            current_content_no += 1
            if not post:
                failure_count += 1
                continue
            post_datetime = pendulum.parse(post["date"])

            if post_datetime > current_time:
                logger.info(f"Post date {post_datetime} is greater than current date. Stopping scraping.")
                break

            data_dir = Variable.get("data_dir")
            parquet_file_path = f"{current_time.year}/{current_time.month}/{current_time.day}/{current_content_no}.parquet"
            csv_file_path = f"{current_time.year}/{current_time.month}/{current_time.day}/{current_content_no}.csv"
            parquet_folder = os.path.join(data_dir, os.path.dirname(parquet_file_path))
            os.makedirs(parquet_folder, exist_ok=True)

            df = pd.DataFrame([post])
            df.to_parquet(f"{data_dir}/{parquet_file_path}", engine="pyarrow", compression="snappy")
            df.to_csv(f"{data_dir}/{csv_file_path}", index=False)
            saved_file_paths.append(parquet_file_path)

            logger.info(f"Saved post {current_content_no} to {parquet_file_path} and {csv_file_path}")

        current_content_no -= failure_count
        Variable.set("content_no", current_content_no)
        logger.info(f"Updated content_no to {current_content_no}")

        return saved_file_paths

    @task
    def upload_to_gcs(files: list, bucket_name: str) -> None:
        storage.upload_files_to_gcs(files, bucket_name, "raw")

    @task
    def load_to_bq(bucket_name: str, dataset_id: str, table_id: str, **kwargs) -> None:
        current_time = pendulum.now()
        gcs_source_uri = f"gs://{bucket_name}/raw/{current_time.year}/{current_time.month}/{current_time.day}/*.parquet"
        operator = storage.load_files_to_bigquery(gcs_source_uri, dataset_id, table_id)
        operator.execute(context=kwargs)

    bucket_name = "blackdesert-mobile-hub-scraping-data-bucket"
    dataset_id = "blackdesert_mobile_hub_data"
    table_id = "blackdesert_mobile_hub_scraping"

    files = scrape_content()
    upload_to_gcs(files, bucket_name) >> load_to_bq(bucket_name, dataset_id, table_id)

scrape_and_store_content_dag()
