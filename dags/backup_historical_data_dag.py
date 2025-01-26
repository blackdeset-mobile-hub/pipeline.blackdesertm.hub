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

    def _save_scraped_data(scrape_dict: dict, is_reply: bool = False) -> list:
        """Save scraped data to Parquet and CSV files."""
        file_paths_saved = []
        for year, posts in scrape_dict.items():
            if posts:
                post_id = f"{posts[0]['post_id']}_{posts[-1]['post_id']}"
                file_suffix = "reply" if is_reply else "post"

                parquet_file_path = f"{year}_{post_id}_{file_suffix}.parquet"
                csv_file_path = f"{year}_{post_id}_{file_suffix}.csv"

                storage.save_dataframe_to_files(posts, parquet_file_path, csv_file_path)
                file_paths_saved.append(parquet_file_path)

                logger.info(f"Backup {file_suffix} {post_id} to {csv_file_path}")

        return file_paths_saved

    @task
    def scrape_content() -> dict:
        """Scrape content and save to files."""
        start_content_no = Variable.get("start_content_no", default_var=385)
        current_content_no = int(start_content_no)

        posts_by_year = {}
        replies_by_year = {}
        current_time = pendulum.now()

        logger.info(f"Starting backup from content_no: {current_content_no}")
        while current_content_no < 617749:
            post = scraper.fetch_blackdesert_post(current_content_no)
            current_content_no += 1

            if not post:
                continue

            created_time = pendulum.parse(post["created_at"])

            if created_time > current_time:
                logger.info(f"Post date {created_time} is greater than current date. Stopping scraping.")
                break

            year = created_time.year
            replies_by_year.setdefault(year, []).extend(post.pop("replies"))
            posts_by_year.setdefault(year, []).append(post)

        post_file_list = _save_scraped_data(posts_by_year)
        reply_file_list = _save_scraped_data(replies_by_year, is_reply=True)

        Variable.set("last_content_no", current_content_no)
        logger.info(f"Updated content_no to {current_content_no}")

        return {
            "post": post_file_list,
            "reply": reply_file_list,
        }

    @task
    def upload_to_gcs(contents: dict, ) -> None:
        """Upload files to Google Cloud Storage."""
        bucket_name = Variable.get("bucket_name")

        storage.upload_files_to_gcs(contents["post"], bucket_name, "archive")
        storage.upload_files_to_gcs(contents["reply"], bucket_name, "archive")

    @task
    def load_to_bq(contents: dict, **kwargs) -> None:
        """Load files from GCS to BigQuery."""
        bucket_name = Variable.get("bucket_name")
        posts_table = "scraping_post"
        replies_table = "scraping_reply"

        for post in contents["post"]:
            gcs_source_uri = f"gs://{bucket_name}/archive/{post}"
            operator = storage.load_files_to_bigquery(gcs_source_uri, "raw_data", posts_table)
            operator.execute(context=kwargs)

        for reply in contents["reply"]:
            gcs_source_uri = f"gs://{bucket_name}/archive/{reply}"
            operator = storage.load_files_to_bigquery(gcs_source_uri, "raw_data", replies_table)
            operator.execute(context=kwargs)

    contents = scrape_content()
    upload_to_gcs(contents) >> load_to_bq(contents)

backup_historical_data_dag()