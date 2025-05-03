from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models.baseoperator import BaseOperator
from google.cloud import storage
from airflow.utils.context import Context
import boto3
from google.oauth2 import service_account
from datetime import datetime, timedelta
import requests
import json
import os

# Config
SUBREDDIT = "ValorantCompetitive"
POST_LIMIT = 10
S3_BUCKET = "testing47"
S3_PREFIX = "project1"
HEADERS = {"User-Agent": "airflow-script/0.1"}

# Task 1: Fetch Reddit posts
def fetch_reddit_posts(**context):
    url = f"https://www.reddit.com/r/{SUBREDDIT}/new.json?limit={POST_LIMIT}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    execution_date = context['ds']  # 'YYYY-MM-DD'
    local_file = f"/tmp/valorant_new_posts_{execution_date}.json"

    posts = []
    for post in response.json()["data"]["children"]:
        p = post["data"]
        posts.append({
            "id": p["id"],
            "title": p["title"],
            "url": p["url"],
            "score": p["score"],
            "num_comments": p["num_comments"],
            "created_utc": datetime.utcfromtimestamp(p["created_utc"]).strftime('%Y-%m-%d %H:%M')
        })

    with open(local_file, "w") as f:
        for post in posts:
            f.write(json.dumps(post) + "\n")

    context['ti'].xcom_push(key="file_path", value=local_file)

# Task 2: Upload to S3
def upload_to_s3(**context):
    file_path = context['ti'].xcom_pull(task_ids="fetch_reddit_posts", key="file_path")
    file_name = os.path.basename(file_path)
    s3_key = f"{S3_PREFIX}/{file_name}"

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )

def copy_s3_to_gcs(**context):
    execution_date = context['ds']
    filename = f"valorant_new_posts_{execution_date}.json"

    # --- Download from S3 ---
    s3_bucket = "testing47"
    s3_key = f"project1/{filename}"
    local_file = f"/tmp/{filename}"

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.get_conn().download_file(s3_bucket, s3_key, local_file)

    # --- Upload to GCS ---
    gcs_bucket_name = "testing48"  #  GCS bucket
    gcs_blob_path = f"{filename}"

    credentials = service_account.Credentials.from_service_account_file(
        "/opt/airflow/credentials/fit-accumulator-458615-u3-f4c215044155.json"
    )
    gcs_client = storage.Client(credentials=credentials)
    bucket = gcs_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_blob_path)
    blob.upload_from_filename(local_file)

    # Push the GCS path to XCom (optional if needed)
    context['ti'].xcom_push(key='gcs_path', value=gcs_blob_path)

def run_bigquery_insert(**context: Context):


    ds = context["ds"]
    uri = f"gs://testing48/valorant_new_posts_{ds}.json"

    bq_operator = BigQueryInsertJobOperator(
    task_id="gcs_to_bigquery",
    configuration={
        "load": {
            "sourceUris": [uri],
            "destinationTable": {
                "projectId": "fit-accumulator-458615-u3",
                "datasetId": "Valorant",
                "tableId": "valorant_raw"
            },
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "autodetect": True,
            "writeDisposition": "WRITE_APPEND",
            "createDisposition": "CREATE_IF_NEEDED"   
        }
    },
    gcp_conn_id="google_cloud_default"
    )
    return bq_operator.execute(context=context)


# DAG definition
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="reddit_valorant_http_to_s3",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["reddit", "requests", "s3"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_reddit_posts",
        python_callable=fetch_reddit_posts,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    copy_to_gcs = PythonOperator(
    task_id="copy_s3_to_gcs",
    python_callable=copy_s3_to_gcs,
    provide_context=True,
    )

    bq_task = PythonOperator(
    task_id="gcs_to_bigquery",
    python_callable=run_bigquery_insert,
    provide_context=True,
    )

    
    fetch_task >> upload_task >> copy_to_gcs >> bq_task
