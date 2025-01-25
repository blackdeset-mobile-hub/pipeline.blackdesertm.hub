import logging
import pendulum

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
        
        backed_up_posts = {}
        backed_file_paths = []
        current_time = pendulum.now()

        logger.info(f"Starting backup from content_no: {current_content_no}")
        while current_content_no < 500:
            post = scraper.fetch_blackdesert_post(current_content_no)
            current_content_no += 1
            if not post:
                continue

            created_time = pendulum.parse(post["created_at"])
            if created_time >= current_time:
                logger.info(f"Post date {created_time} is greater than current date. Stopping backup.")
                break

            backed_up_posts.setdefault(created_time.year, [])
            backed_up_posts[created_time.year].append(post)

        for year, posts in backed_up_posts.items():
            if posts:
                post_id = f"{posts[0]["post_id"]}_{posts[-1]["post_id"]}"
                parquet_file_path = f"{year}_{post_id}.parquet"
                csv_file_path = f"{year}_{post_id}.csv"
                
                storage.save_dataframe_to_files(posts, parquet_file_path, csv_file_path)
                backed_file_paths.append(parquet_file_path)

                logger.info(f"Backup post {post_id} to {csv_file_path}")

        Variable.set("last_content_no", current_content_no)
        logger.info(f"Updated content_no to {current_content_no}")

        return backed_file_paths

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
    #upload_backup_to_gcs(files, bucket_name) >> load_backup_to_bq(bucket_name, dataset_id, table_id)

backup_historical_data_dag()
