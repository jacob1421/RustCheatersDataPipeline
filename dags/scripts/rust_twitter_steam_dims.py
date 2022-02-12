from scripts.helpers import get_json_object_s3, save_pandas_object_s3


def transform_achievement_dim(**kwargs):
    import pandas as pd

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
        data["responses"], record_path=["playerstats", "achievements"]
    )

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # If description is empty copy the name over.
    df.loc[df["description"] == "", "description"] = df["name"]
    df = df.drop(columns=["name"])

    # Drop any NA records
    df = df.dropna(axis="index", subset=["apiname", "description"])

    # Rename columns
    df = df.rename(
        columns={
            "apiname": "name",
        }
    )

    # Drop duplicate dims
    df = df.drop_duplicates(subset=["name", "description"])

    # Select columns
    df = df[["name", "description"]]

    # Data validation checks
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    print("Dims dataframe with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Only stage dims if they exist
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
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_dims")


def transform_badge_dim(**kwargs):
    import pandas as pd

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
        data["responses"], record_path=["response", "badges"], errors="ignore"
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

    # Rename columns
    df = df.rename(
        columns={
            "badgeid": "badge_id",
            "appid": "app_id",
            "communityitemid": "community_item_id",
        }
    )

    # Correct datatypes for columns
    df = df.astype(
        {
            "badge_id": "Int64",
            "app_id": "Int64",
            "community_item_id": "Int64",
            "level": "Int64",
            "xp": "Int64",
        },
        errors="ignore",
    )

    # Drop duplicate dims
    df = df.drop_duplicates(
        subset=["badge_id", "app_id", "community_item_id", "xp", "level"]
    )

    df = df[["badge_id", "app_id", "community_item_id", "xp", "level"]]

    # Data validation checks
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    print("Dims dataframe with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Only stage dims if they exist
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
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_dims")


def transform_relationship_dim(**kwargs):
    import pandas as pd

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

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Drop duplicate dims
    df = df.drop_duplicates(subset=["relationship"])

    # Select columns
    df = df[["relationship"]]

    # Data validation checks
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    print("Dims dataframe with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Only stage dims if they exist
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
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_dims")


def transform_game_dim(**kwargs):
    import pandas as pd

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
    df = df.dropna(axis="index", subset=["name", "appid"])

    # Fillna values
    df = df.fillna(
        value={
            "has_community_visible_stats": 0,
        }
    )

    # Rename columns
    df = df.rename(
        columns={
            "appid": "game_id",
        }
    )

    # Correct datatypes for columns
    df = df.astype(
        {
            "game_id": "int64",
        },
        errors="ignore",
    )

    # Drop duplicate dims
    df = df.drop_duplicates(subset=["game_id", "name"])

    # Select columns
    df = df[["game_id", "name", "has_community_visible_stats"]]

    # Data validation checks
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    print("Dims dataframe with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Only stage dims if they exist
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
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_dims")


def transform_stats_dim(**kwargs):
    import pandas as pd

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
    df = df.dropna(axis="index", subset=["name"])

    # Drop duplicate dims
    df = df.drop_duplicates(subset=["name"])

    # Select columns
    df = df[["name"]]

    # Data validation checks
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    print("Dims dataframe with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Only stage dims if they exist
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
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_dims")


def transform_group_dim(**kwargs):
    import pandas as pd

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
            "gid": "group_id",
        }
    )

    # Drop duplicate dims
    df = df.drop_duplicates(subset=["group_id"])

    # Select columns
    df = df[["group_id"]]

    # Data validation checks
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    print("Dims dataframe with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Only stage dims if they exist
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
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_dims")


def transform_player_dim(**kwargs):
    import pandas as pd

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

    # Make sure these columns exist
    for column in [
        "commentpermission",
        "realname",
        "primaryclanid",
        "timecreated",
        "loccountrycode",
        "locstatecode",
        "loccityid",
    ]:
        if column not in df.columns:
            df[column] = pd.NA

    # Convert time
    df["timecreated"] = pd.to_datetime(
        df["timecreated"], unit="s", errors="coerce", utc=True
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Rename columns
    df = df.rename(
        columns={
            "steamid": "steam_id",
            "communityvisibilitystate": "community_vis_state",
            "timecreated": "created_at",
            "profilestate": "profile_state",
            "personaname": "persona_name",
            "commentpermission": "comment_permission",
            "avatarhash": "avatar_hash",
            "personastate": "persona_state",
            "primaryclanid": "primary_clan_id",
            "loccountrycode": "loc_country_code",
            "locstatecode": "loc_state_code",
            "loccityid": "loc_city_id",
            "realname": "real_name",
        }
    )

    # Correct datatypes for columns
    df = df.astype(
        {
            "steam_id": "Int64",
            "community_vis_state": "Int64",
            "profile_state": "Int64",
            "persona_state": "Int64",
            "comment_permission": "Int64",
            "primary_clan_id": "Int64",
            "loc_city_id": "Int64",
        },
        errors="ignore",
    )

    # Drop duplicate dims
    df = df.drop_duplicates(subset=["steam_id"])

    df = df[
        [
            "steam_id",
            "created_at",
            "community_vis_state",
            "profile_state",
            "persona_name",
            "avatar_hash",
            "persona_state",
            "comment_permission",
            "real_name",
            "primary_clan_id",
            "loc_country_code",
            "loc_state_code",
            "loc_city_id",
        ]
    ]

    # Data validation checks
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    print("Dims dataframe with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Only stage dims if they exist
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
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_dims")


def transform_friend_dim(**kwargs):
    import pandas as pd

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
    df = pd.json_normalize(data["responses"], record_path=["friendslist", "friends"])

    # Log number records at start
    print("Dataframe loaded with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Rename columns
    df = df.rename(
        columns={
            "steamid": "steam_id",
        }
    )

    # Drop duplicate dims
    df = df.drop_duplicates(subset=["steam_id"])

    # Select columns
    df = df[["steam_id"]]

    # Data validation checks
    if df.duplicated().any().sum() > 0:
        raise Exception("Data Contains Duplicate Rows")

    print("Dims dataframe with %i rows and %i columns" % (df.shape[0], df.shape[1]))

    # Only stage dims if they exist
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
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_dims")
