from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import DAG
from custom_operators.SteamToS3Operator import SteamToS3Operator
from custom_operators.LoadDimsOperator import LoadDimsOperator
from custom_operators.LoadFactsOperator import LoadFactsOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from scripts.helpers import get_twitter_timeline
from scripts.rust_twitter_steam_dims import (
    transform_achievement_dim,
    transform_badge_dim,
    transform_relationship_dim,
    transform_game_dim,
    transform_stats_dim,
    transform_group_dim,
    transform_player_dim,
    transform_friend_dim,
)

from scripts.rust_twitter_steam_facts import (
    transform_achievement_fact,
    transform_game_playtime_fact,
    transform_bans_fact,
    transform_friends_fact,
    transform_groups_fact,
    transform_stats_fact,
    transform_game_playing_banned_fact,
    transform_badges_fact,
)

from scripts.sql_queries import sql_queries
from scripts.config import config


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 2, 3),
    "retries": 1,
    "max_retry_delay": timedelta(minutes=5),
}

with DAG(
    "rust_twitter_steam_pipeline",
    schedule_interval=timedelta(hours=1),
    catchup=False,
    default_args=default_args,
    concurrency=3,
    max_active_runs=1,
) as dag:
    extract_twitter_timeline_data = PythonOperator(
        task_id="extract_twitter_timeline_data",
        python_callable=get_twitter_timeline,
        op_kwargs={
            "aws_conn_id": config.AWS_CONN_ID,
            "user_id": config.TWITTER_USER_ID,
            "bucket_name": config.BUCKET_NAME,
        },
        templates_dict={
            "bucket_key": f"{config.LANDING_RAW}twitter/timeline/",
            "BEARER_TOKEN": "{{ var.value.TWITTER_BEARER_TOKEN }}",
        },
    )

    twitter_data_file_sensor = S3KeySensor(
        aws_conn_id=config.AWS_CONN_ID,
        task_id="twitter_data_file_sensor",
        bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_twitter_timeline_data') }}",
        bucket_name=config.BUCKET_NAME,
    )

    with TaskGroup(group_id="extract_steam_data") as extract_steam_data:
        # SteamAPItoS3Operator
        player_summaries = SteamToS3Operator(
            task_id="player_summaries",
            aws_conn_id=config.AWS_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"{config.LANDING_RAW}steam/player_summaries/",
            endpoint="ISteamUser/GetPlayerSummaries/v0002/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
            },
            headers={"Content-Type": "application/json"},
        )

        player_friendlists = SteamToS3Operator(
            task_id="player_friendlists",
            aws_conn_id=config.AWS_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"{config.LANDING_RAW}steam/player_friendlists/",
            endpoint="ISteamUser/GetFriendList/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
                "relationship": "all",
            },
            headers={"Content-Type": "application/json"},
        )

        player_bans = SteamToS3Operator(
            task_id="player_bans",
            aws_conn_id=config.AWS_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"{config.LANDING_RAW}steam/player_bans/",
            endpoint="ISteamUser/GetPlayerBans/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
            },
            headers={"Content-Type": "application/json"},
        )

        player_associated_groups = SteamToS3Operator(
            task_id="player_associated_groups",
            aws_conn_id=config.AWS_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"{config.LANDING_RAW}steam/player_subscribed_groups/",
            endpoint="ISteamUser/GetUserGroupList/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
            },
            headers={"Content-Type": "application/json"},
        )

        player_achievements = SteamToS3Operator(
            task_id="player_achievements",
            aws_conn_id=config.AWS_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"{config.LANDING_RAW}steam/player_achievements/",
            endpoint="ISteamUserStats/GetPlayerAchievements/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
                "appid": "252490",
                "l": "en",
            },
            headers={"Content-Type": "application/json"},
        )

        player_stats = SteamToS3Operator(
            task_id="player_stats",
            aws_conn_id=config.AWS_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"{config.LANDING_RAW}steam/player_stats/",
            endpoint="ISteamUserStats/GetUserStatsForGame/v2/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
                "appid": "252490",
            },
            headers={"Content-Type": "application/json"},
        )

        player_owned_games = SteamToS3Operator(
            task_id="player_owned_games",
            aws_conn_id=config.AWS_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"{config.LANDING_RAW}steam/owned_games/",
            endpoint="IPlayerService/GetOwnedGames/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
                "include_appinfo": "true",
                "include_played_free_games": "true",
            },
            headers={"Content-Type": "application/json"},
        )

        player_badges = SteamToS3Operator(
            task_id="player_badges",
            aws_conn_id=config.AWS_CONN_ID,
            bucket_name=config.BUCKET_NAME,
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key=f"{config.LANDING_RAW}steam/steam_badges/",
            endpoint="IPlayerService/GetBadges/v1/",
            http_conn_id="steam_web_api",
            request_params={
                "key": "{{ var.value.STEAM_WEB_API_TOKEN }}",
                "format": "json",
            },
            headers={"Content-Type": "application/json"},
        )

        (
            player_summaries
            >> player_friendlists
            >> player_bans
            >> player_associated_groups
        )
        (
            player_associated_groups
            >> player_achievements
            >> player_stats
            >> player_owned_games
            >> player_badges
        )

    with TaskGroup(group_id="transform_steam_data") as transform_steam_data:
        # FILE SENSORS
        player_summaries_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_summaries_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_summaries') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        player_friendlists_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_friendlists_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_friendlists') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        player_bans_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_bans_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_bans') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        player_associated_groups_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_associated_groups_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_associated_groups') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        player_achievements_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_achievements_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_achievements') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        player_stats_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_stats_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_stats') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        player_owned_games_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_owned_games_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_owned_games') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        player_badges_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_badges_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_badges') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        # STAGE DIMS
        stage_achievements_dim = PythonOperator(
            task_id="stage_achievements_dim",
            python_callable=transform_achievement_dim,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/achievement_dim/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_achievements') }}",
            },
        )
        stage_badges_dim = PythonOperator(
            task_id="stage_badges_dim",
            python_callable=transform_badge_dim,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/badges_dim/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_badges') }}",
            },
        )
        stage_relationships_dim = PythonOperator(
            task_id="stage_relationships_dim",
            python_callable=transform_relationship_dim,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/relationship_dim/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_friendlists') }}",
            },
        )
        stage_games_dim = PythonOperator(
            task_id="stage_games_dim",
            python_callable=transform_game_dim,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/game_dim/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_owned_games') }}",
            },
        )
        stage_stats_dim = PythonOperator(
            task_id="stage_stats_dim",
            python_callable=transform_stats_dim,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/stats_dim/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_stats') }}",
            },
        )
        stage_group_dim = PythonOperator(
            task_id="stage_group_dim",
            python_callable=transform_group_dim,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/group_dim/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_associated_groups') }}",
            },
        )
        stage_player_dim = PythonOperator(
            task_id="stage_player_dim",
            python_callable=transform_player_dim,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/player_dim/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_summaries') }}",
            },
        )
        stage_friend_dim = PythonOperator(
            task_id="stage_friend_dim",
            python_callable=transform_friend_dim,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/friend_dim/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_friendlists') }}",
            },
        )

        player_summaries_f_sensor >> stage_player_dim
        player_friendlists_f_sensor >> stage_relationships_dim
        player_friendlists_f_sensor >> stage_friend_dim
        player_associated_groups_f_sensor >> stage_group_dim
        player_achievements_f_sensor >> stage_achievements_dim
        player_stats_f_sensor >> stage_stats_dim
        player_owned_games_f_sensor >> stage_games_dim
        player_badges_f_sensor >> stage_badges_dim

        # STAGE FACTS
        stage_achievements_fact = PythonOperator(
            task_id="stage_achievements_fact",
            python_callable=transform_achievement_fact,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/achievement_fact/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_achievements') }}",
            },
        )

        stage_friends_fact = PythonOperator(
            task_id="stage_friends_fact",
            python_callable=transform_friends_fact,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/friends_fact/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_friendlists') }}",
            },
        )

        stage_badges_fact = PythonOperator(
            task_id="stage_badges_facts",
            python_callable=transform_badges_fact,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/badges_fact/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_badges') }}",
            },
        )

        stage_bans_fact = PythonOperator(
            task_id="stage_bans_facts",
            python_callable=transform_bans_fact,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/bans_fact/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_bans') }}",
            },
        )

        stage_game_playtime_fact = PythonOperator(
            task_id="stage_game_playtime_fact",
            python_callable=transform_game_playtime_fact,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/game_playtime_fact/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_owned_games') }}",
            },
        )

        stage_groups_fact = PythonOperator(
            task_id="stage_groups_fact",
            python_callable=transform_groups_fact,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/groups_fact/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_associated_groups') }}",
            },
        )

        stage_game_playing_banned_fact = PythonOperator(
            task_id="stage_game_playing_banned_fact",
            python_callable=transform_game_playing_banned_fact,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/game_playing_banned_fact/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_summaries') }}",
            },
        )

        stage_stats_fact = PythonOperator(
            task_id="stage_stats_fact",
            python_callable=transform_stats_fact,
            op_kwargs={
                "aws_conn_id": config.AWS_CONN_ID,
                "bucket_name": config.BUCKET_NAME,
                "save_bucket_key": f"{config.LANDING_STAGED}steam/stats_fact/",
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_stats') }}",
            },
        )

        player_summaries_f_sensor >> stage_game_playing_banned_fact
        player_friendlists_f_sensor >> stage_friends_fact
        player_associated_groups_f_sensor >> stage_groups_fact
        player_achievements_f_sensor >> stage_achievements_fact
        player_stats_f_sensor >> stage_stats_fact
        player_owned_games_f_sensor >> stage_game_playtime_fact
        player_badges_f_sensor >> stage_badges_fact
        player_bans_f_sensor >> stage_bans_fact

    with TaskGroup(group_id="load_dims_steam_data") as load_dims_steam_data:
        # FILE SENSORS
        achievements_dim_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="achievements_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_achievements_dim') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        group_dim_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="group_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_group_dim') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        badges_dim_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="badges_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_badges_dim') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        friend_dim_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="friend_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_friend_dim') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        relationships_dim_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="relationships_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_relationships_dim') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        games_dim_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="games_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_games_dim') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        stats_dim_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="stats_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_stats_dim') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        player_dim_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="player_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_player_dim') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        # DIMS TO POSTGRESQL
        load_achievements_dim = LoadDimsOperator(
            task_id="load_achievements_dim",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            table="Achievements_Dim",
            columns="name, description",
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_achievements_dim') }}",
            region=config.BUCKET_REGION,
            conflict_columns="name",
            conflict_action="NOTHING",
        )

        load_badges_dim = LoadDimsOperator(
            task_id="load_badges_dim",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            table="Badges_Dim",
            columns="badge_id, app_id, community_item_id, xp, level",
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_badges_dim') }}",
            region=config.BUCKET_REGION,
            conflict_columns="badge_id,app_id,community_item_id,level",
            conflict_action="NOTHING",
        )

        load_friend_dim = LoadDimsOperator(
            task_id="load_friend_dim",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            table="Friend_Dim",
            columns="steam_id",
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_friend_dim') }}",
            region=config.BUCKET_REGION,
            conflict_columns="steam_id",
            conflict_action="NOTHING",
        )

        load_game_dim = LoadDimsOperator(
            task_id="load_game_dim",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            table="Game_Dim",
            columns="game_id, name, has_community_visible_stats",
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_games_dim') }}",
            region=config.BUCKET_REGION,
            conflict_columns="game_id",
            conflict_action="NOTHING",
        )

        load_group_dim = LoadDimsOperator(
            task_id="load_group_dim",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            table="Group_Dim",
            columns="group_id",
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_group_dim') }}",
            region=config.BUCKET_REGION,
            conflict_columns="group_id",
            conflict_action="NOTHING",
        )

        load_player_dim = LoadDimsOperator(
            task_id="load_player_dim",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            table="Player_Dim",
            columns="steam_id, created_at, community_vis_state, profile_state, persona_name, avatar_hash, persona_state, comment_permission, real_name, primary_clan_id, loc_country_code, loc_state_code, loc_city_id",
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_player_dim') }}",
            region=config.BUCKET_REGION,
            conflict_columns="steam_id",
            conflict_action=""" --Update Profile
                                UPDATE SET community_vis_state = EXCLUDED.community_vis_state,
                                profile_state = EXCLUDED.profile_state,
                                persona_name = EXCLUDED.persona_name,
                                avatar_hash = EXCLUDED.avatar_hash,
                                persona_state = EXCLUDED.persona_state,
                                comment_permission = EXCLUDED.comment_permission,
                                real_name = EXCLUDED.real_name,
                                primary_clan_id = EXCLUDED.primary_clan_id,
                                loc_country_code = EXCLUDED.loc_country_code,
                                loc_state_code = EXCLUDED.loc_state_code,
                                loc_city_id = EXCLUDED.loc_city_id""",
        )

        load_relationship_dim = LoadDimsOperator(
            task_id="load_relationship_dim",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            table="Relationship_Dim",
            columns="relationship",
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_relationships_dim') }}",
            region=config.BUCKET_REGION,
            conflict_columns="relationship",
            conflict_action="NOTHING",
        )

        load_stats_dim = LoadDimsOperator(
            task_id="load_stats_dim",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            table="Stats_Dim",
            columns="name",
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_stats_dim') }}",
            region=config.BUCKET_REGION,
            conflict_columns="name",
            conflict_action="NOTHING",
        )

        achievements_dim_f_sensor >> load_achievements_dim
        group_dim_f_sensor >> load_group_dim
        badges_dim_f_sensor >> load_badges_dim
        friend_dim_f_sensor >> load_friend_dim
        relationships_dim_f_sensor >> load_relationship_dim
        games_dim_f_sensor >> load_game_dim
        stats_dim_f_sensor >> load_stats_dim
        player_dim_f_sensor >> load_player_dim

    with TaskGroup(group_id="load_facts_steam_data") as load_facts_steam_data:
        # FILE SENSORS
        friends_fact_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="friends_fact_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_friends_fact') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        bans_fact_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="bans_fact_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_bans_facts') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        badges_fact_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="badges_fact_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_badges_facts') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        game_playtime_fact_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="game_playtime_fact_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_game_playtime_fact') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        groups_fact_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="groups_fact_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_groups_fact') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        achievement_fact_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="achievement_fact_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_achievements_fact') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        game_playing_banned_fact_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="game_playing_banned_fact_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_game_playing_banned_fact') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )
        stats_fact_f_sensor = S3KeySensor(
            aws_conn_id=config.AWS_CONN_ID,
            task_id="stats_fact_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_stats_fact') }}",
            bucket_name=config.BUCKET_NAME,
            timeout=90,
            soft_fail=True,
        )

        # FACTS TO POSTGRESQL
        load_achievement_fact = LoadFactsOperator(
            task_id="load_achievement_fact",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_achievements_fact') }}",
            region=config.BUCKET_REGION,
            sql=sql_queries.achievement_fact_insert,
        )

        load_friends_fact = LoadFactsOperator(
            task_id="load_friends_fact",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_friends_fact') }}",
            region=config.BUCKET_REGION,
            sql=sql_queries.friends_fact_insert,
        )

        load_badges_fact = LoadFactsOperator(
            task_id="load_badges_fact",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_badges_facts') }}",
            region=config.BUCKET_REGION,
            sql=sql_queries.badges_fact_insert,
        )

        load_game_playtime_fact = LoadFactsOperator(
            task_id="load_game_playtime_fact",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_game_playtime_fact') }}",
            region=config.BUCKET_REGION,
            sql=sql_queries.game_playtime_fact_insert,
        )

        load_group_fact = LoadFactsOperator(
            task_id="load_group_fact",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_groups_fact') }}",
            region=config.BUCKET_REGION,
            sql=sql_queries.group_fact_insert,
        )

        load_game_playing_banned_fact = LoadFactsOperator(
            task_id="load_game_playing_banned_fact",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_game_playing_banned_fact') }}",
            region=config.BUCKET_REGION,
            sql=sql_queries.game_playing_banned_fact_insert,
        )

        load_stats_fact = LoadFactsOperator(
            task_id="load_stats_fact",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_stats_fact') }}",
            region=config.BUCKET_REGION,
            sql=sql_queries.stats_fact_insert,
        )

        load_bans_fact = LoadFactsOperator(
            task_id="load_bans_fact",
            postgres_conn_id=config.POSTGRES_CONN,
            database=config.DATABASE,
            schema=config.SCHEMA,
            bucket=config.BUCKET_NAME,
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_bans_facts') }}",
            region=config.BUCKET_REGION,
            sql=sql_queries.bans_fact_insert,
        )

        friends_fact_f_sensor >> load_friends_fact
        bans_fact_f_sensor >> load_bans_fact
        game_playtime_fact_f_sensor >> load_game_playtime_fact
        groups_fact_f_sensor >> load_group_fact
        achievement_fact_f_sensor >> load_achievement_fact
        game_playing_banned_fact_f_sensor >> load_game_playing_banned_fact
        stats_fact_f_sensor >> load_stats_fact
        badges_fact_f_sensor >> load_badges_fact

    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    (
        begin
        >> extract_twitter_timeline_data
        >> twitter_data_file_sensor
        >> extract_steam_data
        >> transform_steam_data
        >> load_dims_steam_data
        >> load_facts_steam_data
        >> end
    )
