import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

import datetime as dt
import pandas as pd
from pmaw import PushshiftAPI

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'reddit_data')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def reddit_scrap(before, after, subreddit, limit, output_file):
    api = PushshiftAPI()

    submissions = api.search_submissions(subreddit=subreddit, limit=limit, before=before, after=after)

    submissions_df = pd.DataFrame(submissions, columns=['author', 'author_fullname', 'domain', 'full_link', 'id',
                                                        'link_flair_text', 'locked', 'num_comments', 'permalink',
                                                        'pinned', 'send_replies', 'title',
                                                        'total_awards_received', 'upvote_ratio', 'url'])
    return submissions_df.to_csv(output_file, header=True, index=False)


def format_to_parquet(input_file, output_file):
    if not input_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(input_file)
    pq.write_table(table, output_file)


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


local_csv_path_template = AIRFLOW_HOME + '/data_reddit.csv'
local_parquet_path_template = AIRFLOW_HOME + '/data_reddit.parquet'
gcs_path_template_tier1 = "tier1/reddit_data.csv"
gcs_path_template_tier2 = "tier2/reddit_data.parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
        dag_id='elt_reddit_pipeline',
        description='Loading Reddit API Data into Redshift',
        schedule_interval="@once",
        default_args=default_args,
        catchup=True,
        max_active_runs=1,
        tags=['Reddit'],
) as dag:
    extract_reddit_data_task = PythonOperator(
        task_id="extract_reddit_data_task",
        python_callable=reddit_scrap,
        op_kwargs={
            "before": int(dt.datetime(2022, 1, 1, 0, 0).timestamp()),
            "after": int(dt.datetime(2021, 12, 1, 0, 0).timestamp()),
            "subreddit": "science",
            "limit": 100,
            "output_file": local_csv_path_template
        },
    )
    extract_reddit_data_task.doc_md = 'Extract Reddit data using PMAW and store as CSV'

    local_to_gcs_task1 = PythonOperator(
        task_id="local_to_gcs_task1",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": gcs_path_template_tier1,
            "local_file": local_csv_path_template,
        },
    )
    local_to_gcs_task1.doc_md = 'Transfer the CSV file to the first GCS bucket'

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "input_file": local_csv_path_template,
            "output_file": local_parquet_path_template,
        },
    )
    format_to_parquet_task.doc_md = 'Reformat the CSV file into a Parquet file'

    local_to_gcs_task2 = PythonOperator(
        task_id="local_to_gcs_task2",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": gcs_path_template_tier2,
            "local_file": local_parquet_path_template,
        },
    )
    local_to_gcs_task2.doc_md = 'Transfer the Parquet to the second GCS bucket'

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "reddit_external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/tier2/*"],
            },
        },
    )
    bigquery_external_table_task.doc_md = 'Create a DW and infuse the data'

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.reddit_data \
                AS \
                SELECT * FROM {BIGQUERY_DATASET}.reddit_external_table;"
    )

    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_reddit_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )
    remove_files = BashOperator(
        task_id='remove_files',
        bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}",
        dag=dag
    )
    remove_files.doc_md = 'Delete local CSV file or files stored in directory'

extract_reddit_data_task >> local_to_gcs_task1 >> format_to_parquet_task >> local_to_gcs_task2 >> bigquery_external_table_task >> bq_create_partitioned_table_job >> remove_files
