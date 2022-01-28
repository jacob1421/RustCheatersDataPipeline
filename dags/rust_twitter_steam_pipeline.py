import json
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import DAG
from steam_plugin.operators.SteamToS3Operator import SteamToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 27),
    "retries": 1,
    "max_retry_delay": timedelta(minutes=5)
}

def get_twitter_timeline(**kwargs):
    from datetime import datetime, timezone
    from tweepy import Paginator, Client
    import json
    kwargs["data_interval_start"] = kwargs["data_interval_start"]
    steam_profile_urls = {
        "steam_profile_urls": [],
        "debug": {
            "extract_start_datetime": kwargs["data_interval_start"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "extract_end_datetime": kwargs["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "logical_execution_datetime": kwargs["ts"]
        }
    }
    client = Client(bearer_token=kwargs["templates_dict"]["BEARER_TOKEN"])

    for tweet in Paginator(client.get_users_tweets, id=kwargs["user_id"],
                           exclude=["replies", "retweets"],
                           max_results=100,
                           tweet_fields=["id", "text", "author_id", "entities", "created_at"],
                           start_time=kwargs["data_interval_start"].isoformat(),
                           end_time=kwargs["data_interval_end"].isoformat(),
                           ).flatten():

        # Make sure the tweet being processed is between the start and end date and author id is only the target author id
        if (tweet.created_at >= kwargs["data_interval_start"]) and (
                tweet.created_at <= kwargs["data_interval_end"]) and (tweet.author_id == kwargs["user_id"]):
            steam_profile_urls["steam_profile_urls"].append(
                {
                    "profile_name_at_ban": tweet["text"].split(" was banned ")[0],
                    "profile_url": tweet["entities"]["urls"][0]["expanded_url"],
                    "tweet_created_time": tweet["created_at"].strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            )

    if kwargs["log_response"]:
        print("DATA: %s\n" % (steam_profile_urls))

    if len(steam_profile_urls["steam_profile_urls"]) > 0:
        file_name = ("%s_to_%s" % (kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
                                   kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ")))
        s3_bucket_key = ("%s%s%s%s" % (kwargs["templates_dict"]["bucket_key"], ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),file_name, ".json"))

        # Save profile urls to S3
        s3_hook = S3Hook(aws_conn_id=kwargs["aws_conn_id"])
        s3_hook.load_string(
            bucket_name=kwargs["bucket_name"],
            string_data=json.dumps(steam_profile_urls, indent=4),
            key=s3_bucket_key
        )
        # Push s3 key
        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_key)
        if kwargs["log_response"]:
            print("SAVED DATA TO BUCKET:%s\nSAVED DATA TO KEY: %s\n" % (kwargs["bucket_name"], s3_bucket_key))
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_timeline_data")

def do_nothing():
    pass

def check_timeline_data_exists(ti):
    timeline_data = ti.xcom_pull(key='s3_bucket_key', task_ids='extract_twitter_timeline_data')
    if timeline_data is None:
        return "no_timeline_data"
    return "twitter_data_file_sensor"


with DAG("rust_twitter_steam_pipeline", schedule_interval=timedelta(hours=1), catchup=True, default_args=default_args, max_active_runs=1) as dag:
    extract_twitter_timeline_data = PythonOperator(
        task_id="extract_twitter_timeline_data",
        python_callable=get_twitter_timeline,
        op_kwargs={
            "aws_conn_id": "s3",
            "user_id": 3243246400,
            "bucket_name": "rust-cheaters",
            "log_response": True
        },
        templates_dict={
            "bucket_key": f"data-lake/raw/twitter/timeline/",
            "BEARER_TOKEN": "{{ var.value.TWITTER_BEARER_TOKEN }}"
        }
    )

    timeline_data_exists_branch = BranchPythonOperator(
        task_id='timeline_data_exists_branch',
        python_callable=check_timeline_data_exists
    )

    no_accounts_exists_dummy_op = DummyOperator(
        task_id="no_timeline_data_exists"
    )

    twitter_data_file_sensor = S3KeySensor(
        aws_conn_id="s3",
        task_id="twitter_data_file_sensor",
        bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_twitter_timeline_data') }}",
        bucket_name="rust-cheaters",
    )

    with TaskGroup(group_id='extract_steam_data') as extract_steam_data:
        # SteamAPItoS3Operator
        player_summaries = SteamToS3Operator(
            task_id="player_summaries",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/player_summaries/",
            endpoint="ISteamUser/GetPlayerSummaries/v0002/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json"
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_friendlists = SteamToS3Operator(
            task_id="player_friendlists",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/player_friendlists/",
            endpoint="ISteamUser/GetFriendList/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
                "relationship": "all"
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_bans = SteamToS3Operator(
            task_id="player_bans",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/player_bans/",
            endpoint="ISteamUser/GetPlayerBans/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json"
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_associated_groups = SteamToS3Operator(
            task_id="player_associated_groups",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/player_subscribed_groups/",
            endpoint="ISteamUser/GetUserGroupList/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json"
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_achievements = SteamToS3Operator(
            task_id="player_achievements",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/player_achievements/",
            endpoint="ISteamUserStats/GetPlayerAchievements/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
                "appid": "252490",
                "l": "en"
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_stats = SteamToS3Operator(
            task_id="player_stats",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/player_stats/",
            endpoint="ISteamUserStats/GetUserStatsForGame/v2/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
                "appid": "252490"
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_recently_played_games = SteamToS3Operator(
            task_id="player_recently_played_games",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/recently_played_games/",
            endpoint="IPlayerService/GetRecentlyPlayedGames/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_owned_games = SteamToS3Operator(
            task_id="player_owned_games",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/owned_games/",
            endpoint="IPlayerService/GetOwnedGames/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
                "include_appinfo": "true",
                "include_played_free_games": "true"
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_steam_level = SteamToS3Operator(
            task_id="player_steam_level",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/steam_level/",
            endpoint="IPlayerService/GetSteamLevel/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_badges = SteamToS3Operator(
            task_id="player_badges",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/steam_badges/",
            endpoint="IPlayerService/GetBadges/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )

        player_community_badge_progress = SteamToS3Operator(
            task_id="player_community_badge_progress",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"data-lake/raw/steam/community_badge_progress/",
            endpoint="IPlayerService/GetCommunityBadgeProgress/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
            },
            headers={
                "Content-Type": "application/json"
            },
            log_response=False
        )
        player_summaries >> player_friendlists >> player_bans >> player_associated_groups
        player_associated_groups >> player_achievements >> player_stats >> player_recently_played_games
        player_recently_played_games >> player_owned_games >> player_steam_level >> player_badges
        player_badges >> player_community_badge_progress

    with TaskGroup(group_id='transform_steam_data') as transform_steam_data:
        #Generate file sensor and transform task
        steam_data_tasks = [task_id.split(".")[1] for task_id in dag.task_ids if "extract_steam_data" in task_id]
        for extract_data_task_id in steam_data_tasks:
            sensor_task_id = f"{extract_data_task_id}_file_sensor"
            file_sensor = S3KeySensor(
                aws_conn_id="s3",
                task_id=sensor_task_id,
                bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.%s') }}" % (extract_data_task_id),
                bucket_name="rust-cheaters",
                timeout=90,
                soft_fail=True
            )
            transform_task_id = f"transform_{extract_data_task_id}"
            transform_op = PythonOperator(
                task_id=transform_task_id,
                python_callable=do_nothing
            )
            file_sensor >> transform_op

    begin = DummyOperator(
        task_id="begin"
    )
    end = DummyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )


    begin >> extract_twitter_timeline_data >> timeline_data_exists_branch >> [no_accounts_exists_dummy_op, twitter_data_file_sensor]
    twitter_data_file_sensor >> extract_steam_data >> transform_steam_data >> end
    no_accounts_exists_dummy_op >> end