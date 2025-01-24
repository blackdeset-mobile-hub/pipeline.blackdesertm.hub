import os
import logging
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime

from modules import scraper, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@dag(
    description="DAG for backing up historical data using Airflow Variables",
    schedule_interval="@once",
    start_date=datetime(2019, 2, 14),
    catchup=False
)
def backup_historical_data_dag():

    @task
    def backup_past_data() -> list:
        start_content_no = Variable.get("start_content_no", default_var=385)
        current_content_no = int(start_content_no)
        
        backed_up_posts = []
        current_time = pendulum.now()

        logger.info(f"Starting backup from content_no: {current_content_no}")
        while True:
            post = scraper.fetch_blackdesert_post(current_content_no)
            current_content_no += 1
            if not post:
                continue
            post_datetime = pendulum.parse(post["date"])

            if post_datetime >= current_time:
                logger.info(f"Post date {post_datetime} is greater than current date. Stopping backup.")
                break
            
            backed_up_posts.append(post)
            logger.info(f"Backed up post {current_content_no}")

        current_content_no -= 1
        data_dir = Variable.get("data_dir")
        df = pd.DataFrame(backed_up_posts)
        
        parquet_file_path = f"{start_content_no}_{current_content_no}.parquet"
        csv_file_path = f"{start_content_no}_{current_content_no}.csv"

        df.to_parquet(f'{data_dir}/{parquet_file_path}', engine="pyarrow", compression="snappy")
        df.to_csv(f'{data_dir}/{csv_file_path}', index=False)
        
        Variable.set("content_no", current_content_no)
        logger.info(f"Updated content_no to {current_content_no}")

        return [parquet_file_path]

    @task
    def upload_backup_to_gcs(files: list, bucket_name: str) -> None:
        storage.upload_files_to_gcs(files, bucket_name, "archive/post")

    @task
    def load_backup_to_bq(bucket_name: str, dataset_id: str, table_id: str, **kwargs) -> None:
        gcs_source_uri = f"gs://{bucket_name}/archive/post/*.parquet"
        operator = storage.load_files_to_bigquery(gcs_source_uri, dataset_id, table_id)
        operator.execute(context=kwargs)

    bucket_name = "blackdesert-mobile-hub-scraping-data-bucket"
    dataset_id = "blackdesert_mobile_hub_data"
    table_id = "blackdesert_mobile_hub_scraping"

    files = backup_past_data()
    upload_backup_to_gcs(files, bucket_name) >> load_backup_to_bq(bucket_name, dataset_id, table_id)

backup_historical_data_dag()
