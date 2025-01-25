import os
import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime

from modules import scraper, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@dag(
    description="DAG for scraping content using Airflow Variables",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
)
def scrape_and_store_content_dag():

    @task
    def scrape_content() -> list:
        last_content_no = Variable.get("last_content_no", default_var=617749)
        current_content_no = int(last_content_no)
        
        scraped_posts = {}
        saved_file_paths = []
        current_time = pendulum.now()

        logger.info(f"Starting scraping from content_no: {current_content_no}")
        failure_count = 0
        while failure_count < 5:
            post = scraper.fetch_blackdesert_post(current_content_no)
            current_content_no += 1
            if not post:
                failure_count += 1
                continue

            failure_count = 0
            created_time = pendulum.parse(post["created_at"])
            if created_time > current_time:
                logger.info(f"Post date {created_time} is greater than current date. Stopping scraping.")
                break
            
            scraped_posts.setdefault(created_time.year, [])
            scraped_posts[created_time.year].append(post)

        data_dir = Variable.get("data_dir")
        for year, posts in scraped_posts.items():
            if posts:
                post_id = f"{posts[0]["post_id"]}_{posts[-1]["post_id"]}"
                parquet_file_path = f"{year}/{post_id}.parquet"
                csv_file_path = f"{year}/{post_id}.csv"
                
                output_folder  = os.path.join(data_dir, os.path.dirname(parquet_file_path))
                os.makedirs(output_folder , exist_ok=True)
                
                storage.save_dataframe_to_files(posts, parquet_file_path, csv_file_path)
                saved_file_paths.append(parquet_file_path)

                logger.info(f"Saved post {post_id} to {csv_file_path}")

        current_content_no -= failure_count
        Variable.set("last_content_no", current_content_no)
        logger.info(f"Updated content_no to {current_content_no}")

        return saved_file_paths

    @task
    def upload_to_gcs(files: list, bucket_name: str) -> None:
        storage.upload_files_to_gcs(files, bucket_name, "raw")

    @task
    def load_to_bq(files: list, bucket_name: str, dataset_id: str, table_id: str, **kwargs) -> None:
        for file in files:
            gcs_source_uri = f"gs://{bucket_name}/{file}"
            operator = storage.load_files_to_bigquery(gcs_source_uri, dataset_id, table_id)
            operator.execute(context=kwargs)

    bucket_name = "blackdesert-mobile-hub-scraping-data-bucket"
    dataset_id = "blackdesert_mobile_hub_data"
    table_id = "blackdesert_mobile_hub_scraping"

    files = scrape_content()
    upload_to_gcs(files, bucket_name) >> load_to_bq(files, bucket_name, dataset_id, table_id)

scrape_and_store_content_dag()
