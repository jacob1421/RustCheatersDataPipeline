from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_twitter_timeline(**kwargs):
    from tweepy import Paginator, Client
    import json

    steam_profile_urls = {
        "steam_profile_urls": [],
        "debug": {
            "extract_start_datetime": kwargs["data_interval_start"].strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "extract_end_datetime": kwargs["data_interval_end"].strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "logical_execution_datetime": kwargs["ts"],
        },
    }

    client = Client(bearer_token=kwargs["templates_dict"]["BEARER_TOKEN"])

    for tweet in Paginator(
        client.get_users_tweets,
        id=kwargs["user_id"],
        exclude=["replies", "retweets"],
        max_results=100,
        tweet_fields=["id", "text", "author_id", "entities", "created_at"],
        start_time=kwargs["data_interval_start"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_time=kwargs["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ"),
    ).flatten():

        # Ensure the tweet being processed is between the start and end date
        # Ensure Author id is only the target author id
        if (
            (tweet.created_at >= kwargs["data_interval_start"])
            and (tweet.created_at <= kwargs["data_interval_end"])
            and (tweet.author_id == kwargs["user_id"])
        ):
            steam_profile_urls["steam_profile_urls"].append(
                {
                    "profile_name_at_ban": tweet["text"].split(" was banned ")[0],
                    "profile_url": tweet["entities"]["urls"][0]["expanded_url"],
                    "tweet_created_time": tweet["created_at"].strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                }
            )

    # if kwargs["log_response"]:
    #     print("DATA: %s\n" % (steam_profile_urls))

    if len(steam_profile_urls["steam_profile_urls"]) > 0:
        file_name = "%s_to_%s" % (
            kwargs["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            kwargs["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )
        s3_bucket_key = "%s%s%s%s" % (
            kwargs["templates_dict"]["bucket_key"],
            ("%s/%s/%s/" % (kwargs["ds"][0:4], kwargs["ds"][5:7], kwargs["ds"][8::])),
            file_name,
            ".json",
        )

        # Save profile urls to S3
        s3_hook = S3Hook(aws_conn_id=kwargs["aws_conn_id"])
        s3_hook.load_string(
            bucket_name=kwargs["bucket_name"],
            string_data=json.dumps(steam_profile_urls, indent=4),
            key=s3_bucket_key,
        )
        # Push s3 key
        kwargs["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_key)
        # if kwargs["log_response"]:
        #     print("SAVED DATA TO BUCKET:%s\nSAVED DATA TO KEY: %s\n" % (kwargs["bucket_name"], s3_bucket_key))
    else:
        kwargs["ti"].xcom_push(key="s3_bucket_key", value="no_timeline_data")


def get_json_object_s3(aws_conn_id="", bucket_name="", key=""):
    import json

    hook = S3Hook(aws_conn_id=aws_conn_id)
    file_content = hook.read_key(bucket_name=bucket_name, key=key)
    json_data = json.loads(file_content)
    return json_data


def save_pandas_object_s3(aws_conn_id="", bucket_name="", key="", pd_data_frame=None):
    from io import StringIO

    if pd_data_frame is None:
        return None

    # Convert dataframe into in memory file object
    csv_buffer = StringIO()
    pd_data_frame.to_csv(csv_buffer, index=False)

    print(
        "Saving pandas object:\nAWS CONN ID:%s\nBUCKET: %s\nKEY: %s\n"
        % (aws_conn_id, bucket_name, key)
    )

    # Save data
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(
        bucket_name=bucket_name, string_data=csv_buffer.getvalue(), key=key
    )
