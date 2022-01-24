import json
from datetime import datetime, timedelta

from airflow import macros
from airflow.operators.python import PythonOperator, get_current_context
from airflow.models import DAG
from twitter_plugin.operators.TwitterToS3Operator import TwitterToS3Operator
from steam_plugin.operators.SteamToS3Operator import SteamToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 20)
}


def get_steam_profile_urls_from_twitter_data(**kwargs):
    context = get_current_context()
    twitter_data_key = context["ti"].xcom_pull(task_ids='extract_twitter_timeline', key='return_value')
    # Get S3 Profile Data
    hook = S3Hook(aws_conn_id=kwargs['aws_conn_id'])
    file_content = hook.read_key(
        key=twitter_data_key,
        bucket_name=kwargs['bucket_name']
    )
    twitter_player_data = json.loads(file_content)

    # Parse the data for steam profile urls
    steam_profiles = {"profile_urls": []}
    for responses in twitter_player_data["responses"]:
        for profile in responses["data"]:
            steam_profiles["profile_urls"].append(profile["entities"]["urls"][0]["expanded_url"])

    if len(steam_profiles["profile_urls"]) > 0:
        s3_hook = S3Hook(aws_conn_id=kwargs['aws_conn_id'])
        s3_hook.load_string(
            bucket_name=kwargs['bucket_name'],
            string_data=json.dumps(steam_profiles, indent=4),
            key=kwargs['save_key']
        )
        return kwargs['save_key']
    else:
        return None

def check_accts_exist(ti):
    profile_urls = ti.xcom_pull(key='return_value', task_ids='extract_steam_profile_urls_from_twitter_data')
    if profile_urls == "None":
        return "no_accounts_exist"
    return "extract_steam_data.extract_steam_player_summaries"

with DAG("rust_twitter_steam_pipeline", schedule_interval=timedelta(hours=1), catchup=False, default_args=default_args) as dag:
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
        log_response=False
    )

    extract_steam_profile_urls_from_twitter_data = PythonOperator(
        task_id="extract_steam_profile_urls_from_twitter_data",
        python_callable=get_steam_profile_urls_from_twitter_data,
        op_kwargs={
            "aws_conn_id": "s3",
            "bucket_name": "rust-cheaters",
            "save_key": f"temp_xcom_data/{macros.uuid.uuid4()}.json"
        }
    )

    accounts_exist_branch = BranchPythonOperator(
        task_id='accounts_exist_branch',
        python_callable=check_accts_exist
    )

    no_accounts_exist_dummy_op = DummyOperator(
        task_id="no_accounts_exist"
    )

    with TaskGroup(group_id='extract_steam_data') as extract_steam_data:
        # SteamAPItoS3Operator
        extract_steam_player_summaries = SteamToS3Operator(
            task_id="extract_steam_player_summaries",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/player_summaries/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_friendlists = SteamToS3Operator(
            task_id="extract_steam_player_friendlists",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/player_friendlists/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_bans = SteamToS3Operator(
            task_id="extract_steam_player_bans",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/player_bans/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_associated_groups = SteamToS3Operator(
            task_id="extract_steam_player_associated_groups",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/player_subscribed_groups/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_achievements = SteamToS3Operator(
            task_id="extract_steam_player_achievements",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/player_achievements/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_stats = SteamToS3Operator(
            task_id="extract_steam_player_stats",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/player_stats/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_recently_played_games = SteamToS3Operator(
            task_id="extract_steam_player_recently_played_games",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/recently_played_games/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_owned_games = SteamToS3Operator(
            task_id="extract_steam_player_owned_games",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/owned_games/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_steam_level = SteamToS3Operator(
            task_id="extract_steam_player_steam_level",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/steam_level/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_badges = SteamToS3Operator(
            task_id="extract_steam_player_badges",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/steam_badges/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

        extract_steam_player_community_badge_progress = SteamToS3Operator(
            task_id="extract_steam_player_community_badge_progress",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            load_key="{{ ti.xcom_pull(task_ids='extract_steam_profile_urls_from_twitter_data', key='return_value') }}",
            save_key=f"data-lake/raw/steam/community_badge_progress/{macros.datetime.today().year}/{macros.datetime.today().month}/{macros.datetime.today().day}/{macros.uuid.uuid4()}.json",
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

    merge_data = DummyOperator(
        task_id="merge_data"
    )
    #Merge Results
    #Transform
    #Save Curated Layer Parquet Snappy
    #Save Trusted Layer Parquet Snappy


    twitter_api >> extract_steam_profile_urls_from_twitter_data >> accounts_exist_branch >> [no_accounts_exist_dummy_op, extract_steam_player_summaries]
    extract_steam_player_summaries >> extract_steam_player_friendlists >> extract_steam_player_bans >> extract_steam_player_associated_groups
    extract_steam_player_associated_groups >> extract_steam_player_achievements >> extract_steam_player_stats >> extract_steam_player_recently_played_games
    extract_steam_player_recently_played_games >> extract_steam_player_owned_games >> extract_steam_player_steam_level >> extract_steam_player_badges
    extract_steam_player_badges >> extract_steam_player_community_badge_progress >> merge_data

