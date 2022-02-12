from scripts.helpers import get_json_object_s3, save_pandas_object_s3
import pandas as pd
from datetime import timedelta


def transform_achievement_fact(**kwargs):
    # Log number records at start
    print(
        "Loading data from Bucket: %s Key: %s"
        % (kwargs["bucket_name"], kwargs["templates_dict"]["load_bucket_key"])
    )

    # Get data from JSON Bucket/Key
    data = get_json_object_s3(
        kwargs["aws_conn_id"],
        kwargs["bucket_name"],
        kwargs["templates_dict"]["load_bucket_key"],
    )

    # Parse JSON into DataFrame
    df = pd.json_normalize(
        data["responses"],
        record_path=["playerstats", "achievements"],
        meta=["queried_steam_id", ["playerstats", "gameName"]],
    )

    # Only get achievements that have been achieved
    df = df[df["achieved"] == 1]

    # Format datetime
    df["unlocktime"] = pd.to_datetime(
        df["unlocktime"], unit="s", errors="coerce", utc=True
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # If description is empty copy the name over.
    df.loc[df["description"] == "", "description"] = df["name"]
    df = df.drop(columns=["name"])

    # Rename columns
    df = df.rename(
        columns={
            "queried_steam_id": "steam_id",
            "apiname": "name",
            "playerstats.gameName": "game_name",
            "unlocktime": "unlock_ts",
        }
    )

    # Data validation checks
    if df.loc[:, df.columns != "unlock_ts"].isna().any().sum() > 0:
        raise Exception("Data Contains Missing Data NaN/Null")
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    # Select columns
    df = df[["steam_id", "name", "game_name", "unlock_ts"]]

    # Only stage facts if they exist
    if not df.empty:
        file_name = "%s_to_%s" % (
            kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )

        s3_bucket_save_key = "%s%s%s%s" % (
            kwargs["save_bucket_key"],
            ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),
            file_name,
            ".csv",
        )

        save_pandas_object_s3(
            kwargs["aws_conn_id"], kwargs["bucket_name"], s3_bucket_save_key, df
        )

        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_facts")


def transform_game_playtime_fact(**kwargs):
    # Log number records at start
    print(
        "Loading data from Bucket: %s Key: %s"
        % (kwargs["bucket_name"], kwargs["templates_dict"]["load_bucket_key"])
    )

    # Get data from JSON Bucket/Key
    data = get_json_object_s3(
        kwargs["aws_conn_id"],
        kwargs["bucket_name"],
        kwargs["templates_dict"]["load_bucket_key"],
    )

    # Fix JSON for empty responses (Private Profiles)
    for response in data["responses"]:
        if response["response"] == {}:
            response["response"]["games"] = [{}]

    # Parse JSON into DataFrame
    df = pd.json_normalize(
        data["responses"], record_path=["response", "games"], meta=["queried_steam_id"]
    )

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Drop any records with NA game name
    df = df.dropna(axis="index", subset=["name"])

    # Fillna values
    df = df.fillna(
        value={
            "has_community_visible_stats": 0,
            "playtime_windows_forever": 0,
            "playtime_mac_forever": 0,
            "playtime_linux_forever": 0,
            "playtime_2weeks": 0,
        }
    )

    # Rename columns
    df = df.rename(
        columns={
            "appid": "game_id",
            "queried_steam_id": "steam_id",
            "playtime_windows_forever": "playtime_windows_mins",
            "playtime_mac_forever": "playtime_mac_mins",
            "playtime_linux_forever": "playtime_linux_mins",
            "playtime_2weeks": "playtime_two_weeks_mins",
        }
    )

    # Correct datatypes for columns
    df = df.astype(
        {
            "steam_id": "Int64",
            "game_id": "Int64",
            "playtime_windows_mins": "Int64",
            "playtime_mac_mins": "Int64",
            "playtime_linux_mins": "Int64",
            "playtime_two_weeks_mins": "Int64",
        },
        errors="ignore",
    )

    # Todays Date
    df["date"] = kwargs["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ")

    # Data validation checks
    if df.isna().any().sum() > 0:
        raise Exception("Data Contains Missing Data NaN/Null")
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    # #Select columns
    df = df[
        [
            "steam_id",
            "game_id",
            "date",
            "playtime_windows_mins",
            "playtime_mac_mins",
            "playtime_linux_mins",
            "playtime_two_weeks_mins",
        ]
    ]

    # Only stage facts if they exist
    if not df.empty:
        file_name = "%s_to_%s" % (
            kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )

        s3_bucket_save_key = "%s%s%s%s" % (
            kwargs["save_bucket_key"],
            ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),
            file_name,
            ".csv",
        )

        save_pandas_object_s3(
            kwargs["aws_conn_id"], kwargs["bucket_name"], s3_bucket_save_key, df
        )

        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_facts")


def transform_bans_fact(**kwargs):
    # Log number records at start
    print(
        "Loading data from Bucket: %s Key: %s"
        % (kwargs["bucket_name"], kwargs["templates_dict"]["load_bucket_key"])
    )

    # Get data from JSON Bucket/Key
    data = get_json_object_s3(
        kwargs["aws_conn_id"],
        kwargs["bucket_name"],
        kwargs["templates_dict"]["load_bucket_key"],
    )

    # Parse JSON into DataFrame
    df = pd.json_normalize(data["responses"], record_path=["players"])

    # Get the date of ban, instead of days since last ban
    df["DaysSinceLastBan"] = df["DaysSinceLastBan"].map(
        lambda days_since_ban: kwargs["data_interval_end"]
        - timedelta(days=days_since_ban)
    )

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Rename columns
    df = df.rename(
        columns={
            "SteamId": "steam_id",
            "DaysSinceLastBan": "last_ban_date",
            "CommunityBanned": "community_banned",
            "VACBanned": "vac_banned",
            "NumberOfVACBans": "num_vac_bans",
            "NumberOfGameBans": "num_game_bans",
            "EconomyBan": "economy_ban",
        }
    )

    # Data validation checks
    if df.isna().any().sum() > 0:
        raise Exception("Data Contains Missing Data NaN/Null")
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    # Select columns
    df = df[
        [
            "steam_id",
            "last_ban_date",
            "num_vac_bans",
            "num_game_bans",
            "community_banned",
            "economy_ban",
            "vac_banned",
        ]
    ]

    # Only stage facts if they exist
    if not df.empty:
        file_name = "%s_to_%s" % (
            kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )

        s3_bucket_save_key = "%s%s%s%s" % (
            kwargs["save_bucket_key"],
            ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),
            file_name,
            ".csv",
        )

        save_pandas_object_s3(
            kwargs["aws_conn_id"], kwargs["bucket_name"], s3_bucket_save_key, df
        )

        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_facts")


def transform_friends_fact(**kwargs):
    # Log number records at start
    print(
        "Loading data from Bucket: %s Key: %s"
        % (kwargs["bucket_name"], kwargs["templates_dict"]["load_bucket_key"])
    )

    # Get data from JSON Bucket/Key
    data = get_json_object_s3(
        kwargs["aws_conn_id"],
        kwargs["bucket_name"],
        kwargs["templates_dict"]["load_bucket_key"],
    )

    # Parse JSON into DataFrame
    df = pd.json_normalize(
        data["responses"],
        record_path=["friendslist", "friends"],
        meta=["queried_steam_id"],
    )

    # Format datetime
    df["friend_since"] = pd.to_datetime(
        df["friend_since"], unit="s", errors="coerce", utc=True
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Rename columns
    df = df.rename(
        columns={"steamid": "friend_steam_id", "queried_steam_id": "steam_id"}
    )

    # Data validation checks
    if df.isna().any().sum() > 0:
        raise Exception("Data Contains Missing Data NaN/Null")
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    # Select columns
    df = df[["steam_id", "friend_steam_id", "friend_since", "relationship"]]

    # Only stage facts if they exist
    if not df.empty:
        file_name = "%s_to_%s" % (
            kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )

        s3_bucket_save_key = "%s%s%s%s" % (
            kwargs["save_bucket_key"],
            ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),
            file_name,
            ".csv",
        )

        save_pandas_object_s3(
            kwargs["aws_conn_id"], kwargs["bucket_name"], s3_bucket_save_key, df
        )

        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_facts")


def transform_groups_fact(**kwargs):
    # Log number records at start
    print(
        "Loading data from Bucket: %s Key: %s"
        % (kwargs["bucket_name"], kwargs["templates_dict"]["load_bucket_key"])
    )

    # Get data from JSON Bucket/Key
    data = get_json_object_s3(
        kwargs["aws_conn_id"],
        kwargs["bucket_name"],
        kwargs["templates_dict"]["load_bucket_key"],
    )

    # Parse JSON into DataFrame
    df = pd.json_normalize(
        data["responses"], record_path=["response", "groups"], meta=["queried_steam_id"]
    )

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Rename columns
    df = df.rename(
        columns={
            "queried_steam_id": "steam_id",
            "gid": "group_id",
        }
    )

    # Todays Date
    df["date"] = kwargs["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ")

    # Data validation checks
    if df.isna().any().sum() > 0:
        raise Exception("Data Contains Missing Data NaN/Null")
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    # #Select columns
    df = df[["steam_id", "group_id", "date"]]

    # Only stage facts if they exist
    if not df.empty:
        file_name = "%s_to_%s" % (
            kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )

        s3_bucket_save_key = "%s%s%s%s" % (
            kwargs["save_bucket_key"],
            ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),
            file_name,
            ".csv",
        )

        save_pandas_object_s3(
            kwargs["aws_conn_id"], kwargs["bucket_name"], s3_bucket_save_key, df
        )

        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_facts")


def transform_stats_fact(**kwargs):
    # Log number records at start
    print(
        "Loading data from Bucket: %s Key: %s"
        % (kwargs["bucket_name"], kwargs["templates_dict"]["load_bucket_key"])
    )

    # Get data from JSON Bucket/Key
    data = get_json_object_s3(
        kwargs["aws_conn_id"],
        kwargs["bucket_name"],
        kwargs["templates_dict"]["load_bucket_key"],
    )

    # Fix JSON for empty responses (Private Profiles)
    for response in data["responses"]:
        if "stats" not in response["playerstats"]:
            response["playerstats"]["stats"] = [{}]

    # Parse JSON into DataFrame
    df = pd.json_normalize(
        data["responses"],
        record_path=["playerstats", "stats"],
        meta=["queried_steam_id"],
    )

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Drop any records with NA name and value
    df = df.dropna(axis="index", subset=["name", "value"])

    # Rename columns
    df = df.rename(
        columns={
            "queried_steam_id": "steam_id",
        }
    )

    # Todays Date
    df["date"] = kwargs["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ")

    # Todays Date
    df["game"] = "Rust"

    # Data validation checks
    if df.isna().any().sum() > 0:
        raise Exception("Data Contains Missing Data NaN/Null")
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    df = df[["name", "steam_id", "game", "date", "value"]]

    # Only stage facts if they exist
    if not df.empty:
        file_name = "%s_to_%s" % (
            kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )

        s3_bucket_save_key = "%s%s%s%s" % (
            kwargs["save_bucket_key"],
            ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),
            file_name,
            ".csv",
        )

        save_pandas_object_s3(
            kwargs["aws_conn_id"], kwargs["bucket_name"], s3_bucket_save_key, df
        )

        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_facts")


def transform_game_playing_banned_fact(**kwargs):
    # Log number records at start
    print(
        "Loading data from Bucket: %s Key: %s"
        % (kwargs["bucket_name"], kwargs["templates_dict"]["load_bucket_key"])
    )

    # Get data from JSON Bucket/Key
    data = get_json_object_s3(
        kwargs["aws_conn_id"],
        kwargs["bucket_name"],
        kwargs["templates_dict"]["load_bucket_key"],
    )

    # Parse JSON into DataFrame
    df = pd.json_normalize(data["responses"], record_path=["response", "players"])

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    df["timecreated"] = pd.to_datetime(
        df["timecreated"], unit="s", errors="coerce", utc=True
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Rename columns
    df = df.rename(
        columns={"steamid": "steam_id", "gameid": "game_id"}, errors="ignore"
    )

    # Todays Date
    df["date"] = kwargs["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ")

    # Data validation checks
    if df["steam_id"].isna().sum() > 0:
        raise Exception("Data Contains Missing Data NaN/Null")
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    if "game_id" in df.columns:
        # Drop any records with NA game ids
        df = df.dropna(axis="index", subset=["game_id"])

        # Only get achievements that have been achieved
        df = df[df["game_id"] != "NaN"]

        # Select columns
        df = df[["steam_id", "game_id", "date"]]

        # Only stage facts if they exist
        if not df.empty:
            file_name = "%s_to_%s" % (
                kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
                kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            )

            s3_bucket_save_key = "%s%s%s%s" % (
                kwargs["save_bucket_key"],
                (
                    "%s/%s/%s/"
                    % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])
                ),
                file_name,
                ".csv",
            )

            save_pandas_object_s3(
                kwargs["aws_conn_id"], kwargs["bucket_name"], s3_bucket_save_key, df
            )

            kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
        else:
            kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_facts")


def transform_badges_fact(**kwargs):
    # Log number records at start
    print(
        "Loading data from Bucket: %s Key: %s"
        % (kwargs["bucket_name"], kwargs["templates_dict"]["load_bucket_key"])
    )

    # Get data from JSON Bucket/Key
    data = get_json_object_s3(
        kwargs["aws_conn_id"],
        kwargs["bucket_name"],
        kwargs["templates_dict"]["load_bucket_key"],
    )

    # Fix JSON for empty responses (Private Profiles)
    for response in data["responses"]:
        if "badges" not in response["response"]:
            response["response"]["badges"] = [{}]

    # Parse JSON into DataFrame
    df = pd.json_normalize(
        data["responses"],
        record_path=["response", "badges"],
        meta=["queried_steam_id", ["badges", "player_level"]],
        errors="ignore",
    )

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Make sure these columns exist
    for column in ["appid", "communityitemid"]:
        if column not in df.columns:
            df[column] = -1

    # Drop any records with NA badgeid
    df = df.dropna(axis="index", subset=["badgeid"])

    # Nulls do not work for check constraints
    df["appid"] = df["appid"].fillna(-1)
    df["communityitemid"] = df["communityitemid"].fillna(-1)

    # Get date from unix time
    df["completion_time"] = pd.to_datetime(
        df["completion_time"], unit="s", errors="coerce", utc=True
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Rename columns
    df = df.rename(
        columns={
            "queried_steam_id": "steam_id",
            "badges.player_level": "steam_level",
            "badgeid": "badge_id",
            "appid": "app_id",
            "communityitemid": "community_item_id",
        }
    )

    # Correct datatypes for columns
    df = df.astype(
        {
            "steam_id": "Int64",
            "badge_id": "Int64",
            "app_id": "Int64",
            "community_item_id": "Int64",
            "xp": "Int64",
            "level": "Int64",
            "scarcity": "Int64",
            "steam_level": "Int64",
        },
        errors="ignore",
    )

    # Data validation checks
    if df["steam_id"].isna().sum() > 0:
        raise Exception("Data Contains Missing Data NaN/Null")
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    # Select columns
    df = df[
        [
            "steam_id",
            "badge_id",
            "app_id",
            "community_item_id",
            "xp",
            "level",
            "completion_time",
            "scarcity",
            "steam_level",
        ]
    ]

    # Only stage facts if they exist
    if not df.empty:
        file_name = "%s_to_%s" % (
            kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )

        s3_bucket_save_key = "%s%s%s%s" % (
            kwargs["save_bucket_key"],
            ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),
            file_name,
            ".csv",
        )

        save_pandas_object_s3(
            kwargs["aws_conn_id"], kwargs["bucket_name"], s3_bucket_save_key, df
        )

        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_facts")
