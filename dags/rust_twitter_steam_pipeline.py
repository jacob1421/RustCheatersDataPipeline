import json
from datetime import datetime, timedelta

from airflow import macros
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator, get_current_context
from airflow.models import Variable, DAG
from twitter_plugin.operators.TwitterToS3Operator import TwitterToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 18)
}

with DAG("rust_twitter_steam_pipeline", schedule_interval=timedelta(hours=1), catchup=True, default_args=default_args) as dag:
    twitter_api = TwitterToS3Operator(
        task_id="extract_twitter_timeline",
        twitter_user_id=3243246400,
        endpoint="users/:id/tweets",
        http_conn_id="twitter_api",
        bearer_token="{{ var.value.TWITTER_BEARER_TOKEN }}",
        request_params={
            "max_results": 100,
            "exclude": "replies,retweets",
            "tweet.fields": "id,text,author_id,entities,created_at",
            "start_time": "{{ data_interval_start | ts }}",
            "end_time": "{{ data_interval_end | ts}}"
        },
        response_check=lambda response: response.status_code == 200,
        aws_conn_id="s3",
        bucket_name="rust-cheaters",
        key=f"data-lake/raw/twitter/timeline/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
        log_response=True
    )

    #SteamAPItoS3Operator




    twitter_api