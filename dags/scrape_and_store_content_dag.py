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

    def _save_scraped_data(scrape_dict: dict, is_reply: bool = False) -> list:
        """Save scraped data to Parquet and CSV files."""
        data_dir = Variable.get("data_dir")
        file_paths_saved = []
        
        for year, posts in scrape_dict.items():
            if posts:
                post_id = f"{posts[0]['post_id']}_{posts[-1]['post_id']}"
                file_suffix = "reply" if is_reply else "post"

                parquet_file_path = f"{year}/{post_id}_{file_suffix}.parquet"
                csv_file_path = f"{year}/{post_id}_{file_suffix}.csv"

                output_folder = os.path.join(data_dir, os.path.dirname(parquet_file_path))
                os.makedirs(output_folder, exist_ok=True)

                storage.save_dataframe_to_files(posts, parquet_file_path, csv_file_path)
                file_paths_saved.append(parquet_file_path)

                logger.info(f"Saved {file_suffix} {post_id} to {csv_file_path}")

        return file_paths_saved

    @task
    def scrape_content() -> dict:
        """Scrape content and save to files."""
        last_content_no = 617749#Variable.get("last_content_no", default_var=617749)
        current_content_no = int(last_content_no)

        posts_by_year = {}
        replies_by_year = {}
        current_time = pendulum.now()

        logger.info(f"Starting scraping from content_no: {current_content_no}")
        failure_count = 0

        while failure_count < 1:
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

            year = created_time.year
            replies_by_year.setdefault(year, []).extend(post.pop("replies"))
            posts_by_year.setdefault(year, []).append(post)

        post_file_list = _save_scraped_data(posts_by_year)
        reply_file_list = _save_scraped_data(replies_by_year, is_reply=True)

        current_content_no -= failure_count
        Variable.set("last_content_no", current_content_no)
        logger.info(f"Updated content_no to {current_content_no}")

        return {
            "post": post_file_list,
            "reply": reply_file_list,
        }

    @task
    def upload_to_gcs(contents: dict, bucket_name: str) -> None:
        """Upload files to Google Cloud Storage."""
        storage.upload_files_to_gcs(contents["post"], bucket_name, "raw")
        storage.upload_files_to_gcs(contents["reply"], bucket_name, "raw")

    @task
    def load_to_bq(contents: dict, bucket_name: str, **kwargs) -> None:
        """Load files from GCS to BigQuery."""
        dataset_id = "blackdesert_mobile_hub_data"
        posts_table = "blackdesert_mobile_hub_scraping_post"
        replies_table = "blackdesert_mobile_hub_scraping_reply"

        for post in contents["post"]:
            gcs_source_uri = f"gs://{bucket_name}/raw/{post}"
            operator = storage.load_files_to_bigquery(gcs_source_uri, dataset_id, posts_table)
            operator.execute(context=kwargs)

        for reply in contents["reply"]:
            gcs_source_uri = f"gs://{bucket_name}/raw/{reply}"
            operator = storage.load_files_to_bigquery(gcs_source_uri, dataset_id, replies_table)
            operator.execute(context=kwargs)

    bucket_name = "blackdesert-mobile-hub-scraping-data-bucket"

    contents = scrape_content()
    upload_to_gcs(contents, bucket_name) >> load_to_bq(contents, bucket_name)

scrape_and_store_content_dag()