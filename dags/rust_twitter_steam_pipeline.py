from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import DAG
from custom_operators.SteamToS3Operator import SteamToS3Operator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from scripts.helpers import get_twitter_timeline, check_timeline_data_exists
from scripts.rust_twitter_steam_dims import transform_achievement_dim, transform_badge_dim, transform_relationship_dim, transform_game_dim, transform_stats_dim, transform_group_dim, transform_player_dim, transform_friend_dim
from scripts.rust_twitter_steam_facts import transform_stats_fact, transform_game_playing_banned_fact, transform_groups_fact, transform_game_playtime_fact, transform_bans_fact, transform_badges_fact, transform_friends_fact, transform_achievement_fact

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 27),
    "retries": 1,
    "max_retry_delay": timedelta(minutes=5)
}

with DAG("rust_twitter_steam_pipeline", schedule_interval=timedelta(hours=1), catchup=False, default_args=default_args, max_active_runs=1) as dag:
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
            bucket_save_key="data-lake/raw/steam/player_summaries/",
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
            bucket_save_key="data-lake/raw/steam/player_friendlists/",
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
            bucket_save_key="data-lake/raw/steam/player_bans/",
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
            bucket_save_key="data-lake/raw/steam/player_subscribed_groups/",
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
            bucket_save_key="data-lake/raw/steam/player_achievements/",
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
            bucket_save_key="data-lake/raw/steam/player_stats/",
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

        player_owned_games = SteamToS3Operator(
            task_id="player_owned_games",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key="data-lake/raw/steam/owned_games/",
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

        player_badges = SteamToS3Operator(
            task_id="player_badges",
            aws_conn_id="s3",
            bucket_name="rust-cheaters",
            bucket_load_key="{{ ti.xcom_pull(task_ids='extract_twitter_timeline_data', key='s3_bucket_key') }}",
            bucket_save_key="data-lake/raw/steam/steam_badges/",
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

        player_summaries >> player_friendlists >> player_bans >> player_associated_groups
        player_associated_groups >> player_achievements >> player_stats >> player_owned_games >> player_badges

    with TaskGroup(group_id='transform_steam_data') as transform_steam_data:
        #FILE SENSORS
        player_summaries_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_summaries_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_summaries') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )
        player_friendlists_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_friendlists_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_friendlists') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )
        player_bans_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_bans_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_bans') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )
        player_associated_groups_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_associated_groups_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_associated_groups') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )
        player_achievements_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_achievements_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_achievements') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )
        player_stats_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_stats_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_stats') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )
        player_owned_games_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_owned_games_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_owned_games') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )
        player_badges_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_badges_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_badges') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

        #STAGE DIMS
        stage_achievements_dim = PythonOperator(
            task_id="stage_achievements_dim",
            python_callable=transform_achievement_dim,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/achievement_dim/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_achievements') }}",
            }
        )
        stage_badges_dim = PythonOperator(
            task_id="stage_badges_dim",
            python_callable=transform_badge_dim,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/badges_dim/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_badges') }}",
            }
        )
        stage_relationships_dim = PythonOperator(
            task_id="stage_relationships_dim",
            python_callable=transform_relationship_dim,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/relationship_dim/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_friendlists') }}",
            }
        )
        stage_games_dim = PythonOperator(
            task_id="stage_games_dim",
            python_callable=transform_game_dim,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/game_dim/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_owned_games') }}",
            }
        )
        stage_stats_dim = PythonOperator(
            task_id="stage_stats_dim",
            python_callable=transform_stats_dim,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/stats_dim/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_stats') }}",
            }
        )
        stage_group_dim = PythonOperator(
            task_id="stage_group_dim",
            python_callable=transform_group_dim,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/group_dim/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_associated_groups') }}",
            }
        )
        stage_player_dim = PythonOperator(
            task_id="stage_player_dim",
            python_callable=transform_player_dim,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/player_dim/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_summaries') }}",
            }
        )
        stage_friend_dim = PythonOperator(
            task_id="stage_friend_dim",
            python_callable=transform_friend_dim,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/friend_dim/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_friendlists') }}",
            }
        )

        player_summaries_f_sensor >> stage_player_dim
        player_friendlists_f_sensor >> stage_relationships_dim
        player_friendlists_f_sensor >> stage_friend_dim
        player_associated_groups_f_sensor >> stage_group_dim
        player_achievements_f_sensor >> stage_achievements_dim
        player_stats_f_sensor >> stage_stats_dim
        player_owned_games_f_sensor >> stage_games_dim
        player_badges_f_sensor >> stage_badges_dim

        #STAGE FACTS
        stage_achievements_fact = PythonOperator(
            task_id="stage_achievements_fact",
            python_callable=transform_achievement_fact,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/achievement_fact/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_achievements') }}",
            }
        )

        stage_friends_fact = PythonOperator(
            task_id="stage_friends_fact",
            python_callable=transform_friends_fact,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/friends_fact/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_friendlists') }}",
            }
        )

        stage_badges_fact = PythonOperator(
            task_id="stage_badges_facts",
            python_callable=transform_badges_fact,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/badges_fact/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_badges') }}",
            }
        )

        stage_bans_fact = PythonOperator(
            task_id="stage_bans_facts",
            python_callable=transform_bans_fact,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/bans_fact/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_bans') }}",
            }
        )

        stage_game_playtime_fact = PythonOperator(
            task_id="stage_game_playtime_fact",
            python_callable=transform_game_playtime_fact,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/game_playtime_fact/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_owned_games') }}",
            }
        )

        stage_groups_fact = PythonOperator(
            task_id="stage_groups_fact",
            python_callable=transform_groups_fact,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/groups_fact/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_associated_groups') }}",
            }
        )

        stage_game_playing_banned_fact = PythonOperator(
            task_id="stage_game_playing_banned_fact",
            python_callable=transform_game_playing_banned_fact,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/game_playing_banned_fact/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_summaries') }}",
            }
        )

        stage_stats_fact = PythonOperator(
            task_id="stage_stats_fact",
            python_callable=transform_stats_fact,
            op_kwargs={
                "aws_conn_id": "s3",
                "bucket_name": "rust-cheaters",
                "save_bucket_key": "data-lake/staged/steam/stats_fact/",
                "log_response": True
            },
            templates_dict={
                "load_bucket_key": "{{ ti.xcom_pull(key='s3_bucket_key', task_ids='extract_steam_data.player_stats') }}",
            }
        )

        player_summaries_f_sensor >> stage_game_playing_banned_fact
        player_friendlists_f_sensor >> stage_friends_fact
        player_associated_groups_f_sensor >> stage_groups_fact
        player_achievements_f_sensor >> stage_achievements_fact
        player_stats_f_sensor >> stage_stats_fact
        player_owned_games_f_sensor >> stage_game_playtime_fact
        player_badges_f_sensor >> stage_badges_fact
        player_bans_f_sensor >> stage_bans_fact

    with TaskGroup(group_id='load_dims_steam_data') as load_steam_data:
        # FILE SENSORS
        achievements_dim_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="achievements_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_achievements_dim') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

        group_dim_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="group_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_group_dim') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

        badges_dim_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="badges_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_badges_dim') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

        friend_dim_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="friend_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_friend_dim') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

        relationships_dim_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="relationships_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_relationships_dim') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

        games_dim_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="games_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_games_dim') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

        stats_dim_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="stats_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_stats_dim') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

        player_dim_f_sensor = S3KeySensor(
            aws_conn_id="s3",
            task_id="player_dim_f_sensor",
            bucket_key="{{ ti.xcom_pull(key='s3_bucket_key', task_ids='transform_steam_data.stage_player_dim') }}",
            bucket_name="rust-cheaters",
            timeout=90,
            soft_fail=True
        )

    begin = DummyOperator(
        task_id="begin"
    )

    end = DummyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    begin >> extract_twitter_timeline_data >> timeline_data_exists_branch >> [no_accounts_exists_dummy_op, twitter_data_file_sensor]
    twitter_data_file_sensor >> extract_steam_data >> transform_steam_data >> load_steam_data >> end
    no_accounts_exists_dummy_op >> end