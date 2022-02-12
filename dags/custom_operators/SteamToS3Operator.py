from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from requests.auth import HTTPBasicAuth
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import time


class SteamToS3Operator(BaseOperator):
    template_fields = ["request_params", "bucket_load_key", "bucket_save_key"]

    def __init__(
        self,
        bucket_name,
        bucket_load_key,
        bucket_save_key,
        aws_conn_id,
        endpoint,
        http_conn_id,
        request_params,
        headers,
        log_response=False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.log_response = log_response
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.bucket_load_key = bucket_load_key
        self.bucket_save_key = bucket_save_key
        self.aws_conn_id = aws_conn_id
        self.request_params = request_params or {}
        self.headers = headers or {}

    @staticmethod
    def is_vanity_url(steam_profile):
        return "/id/" in steam_profile

    # Some Steam API calls allow you to request 100 steam ids at a time
    def get_api_data_batched(self, http, steam_ids):
        responses = {"responses": []}
        r_params = self.request_params.copy()
        for i in range(0, len(steam_ids), 100):
            steam_ids_batch = ",".join(steam_ids[i : i + 100])  # noqa
            r_params["steamids"] = steam_ids_batch
            response = http.run(
                endpoint=self.endpoint, data=r_params, headers=self.headers
            )
            r_json = response.json()

            # Append response results
            responses["responses"].append(r_json)

            # API Rate Limit 1 request per 0.5 second
            time.sleep(0.5)

        if len(responses["responses"]) > 0:
            return responses
        return None

    def resolve_vanity_url(self, http, steam_vanity_url):
        vanity_id = steam_vanity_url.split("https://steamcommunity.com/id/")[1].replace(
            "/", ""
        )
        response = http.run(
            endpoint="ISteamUser/ResolveVanityURL/v0001/",
            data={"key": self.request_params["key"], "vanityurl": vanity_id},
            headers=self.headers,
        )
        # API Rate Limit 1 request per 0.5 second
        time.sleep(0.5)
        if self.log_response:
            self.log.info(response.text)
        if "steamid" in response.text:
            return str(response.json()["response"]["steamid"])
        return None

    def get_single_endpoint_data(self, http, steam_ids):
        responses = {"responses": []}
        if steam_ids is None:
            response = http.run(
                endpoint=self.endpoint, data=self.request_params, headers=self.headers
            )
            responses["responses"].append(response.json())
            time.sleep(0.5)
        else:
            for steam_id in steam_ids:
                # Some apis return a 401 status code with the response
                # when you try to access data in a private account
                try:
                    self.request_params["steamid"] = steam_id
                    response = http.run(
                        endpoint=self.endpoint,
                        data=self.request_params,
                        headers=self.headers,
                    )
                    # steam_id of incoming steam_id as key and response
                    r_json = response.json()
                    # Not all api calls return the associated steam id
                    r_json["queried_steam_id"] = str(steam_id)
                    responses["responses"].append(r_json)
                except Exception as e:
                    # https://partner.steamgames.com/doc/webapi_overview/responses
                    # This approach is taken since the API will say a profile is private
                    # when its not fully private. You cant actually tell if the profile is
                    # fully private or just pieces of the profile are private.
                    if "429" in str(e):
                        # To Many Requests
                        raise e
                    else:
                        pass

                time.sleep(0.5)

        if len(responses["responses"]) > 0:
            return responses
        return None

    def check_api_required_params(self, list_required_params):
        for param in list_required_params:
            if param not in self.request_params:
                raise AirflowException(
                    "Endpoint: %s Requires the param: %s" % (self.endpoint, param)
                )
                break

    def execute(self, context):
        if self.bucket_load_key == "None":
            return None

        # Make sure key is in requests params
        self.check_api_required_params(["key"])

        response_data = None
        http = HttpHook(
            method="GET", http_conn_id=self.http_conn_id, auth_type=HTTPBasicAuth
        )

        if "GetNewsForApp" in self.endpoint:
            self.check_api_required_params(["appid"])
            response_data = self.get_single_endpoint_data(http, None)
        elif "GetGlobalAchievementPercentagesForApp" in self.endpoint:
            self.check_api_required_params(["gameid"])
            response_data = self.get_single_endpoint_data(http, None)
        elif "GetSchemaForGame" in self.endpoint:
            self.check_api_required_params(["appid"])
        elif "GetNumberOfCurrentPlayers" in self.endpoint:
            self.check_api_required_params(["appid"])
            response_data = self.get_single_endpoint_data(http, None)
        elif "GetGlobalStatsForGame" in self.endpoint:
            self.check_api_required_params(["appid", "count", "name"])
            response_data = self.get_single_endpoint_data(http, None)
        else:
            # Get S3 Profile Data
            hook = S3Hook(aws_conn_id=self.aws_conn_id)
            file_content = hook.read_key(
                key=self.bucket_load_key, bucket_name=self.bucket_name
            )
            profiles_data = json.loads(file_content)

            # Get steams id from profile data
            steam_ids = []
            for profile_url in profiles_data["steam_profile_urls"]:
                # Get steam_id
                if self.is_vanity_url(profile_url["profile_url"]):
                    # Get the profile id from vanity
                    steam_id = self.resolve_vanity_url(http, profile_url["profile_url"])
                else:
                    steam_id = profile_url["profile_url"].split(
                        "http://steamcommunity.com/profiles/"
                    )[1]

                # steam_id can be None if vanity fails to resolve
                if steam_id is None:
                    raise AirflowException("No Steam ID was found for %s" % profile_url)
                else:
                    steam_ids.append(steam_id)

            # Handle api endpoints each differently.
            # Batching makes 1 requests for a 100 profiles vs 100 requests for 100 profiles
            # Batch Request Summaries and Bans
            if (
                "GetPlayerSummaries" in self.endpoint
                or "GetPlayerBans" in self.endpoint
            ):
                response_data = self.get_api_data_batched(http, steam_ids)
            else:
                if "GetFriendList" in self.endpoint:
                    self.check_api_required_params(["relationship"])
                elif "GetPlayerAchievements" in self.endpoint:
                    self.check_api_required_params(["appid"])
                elif "GetUserStatsForGame" in self.endpoint:
                    self.check_api_required_params(["appid"])

                response_data = self.get_single_endpoint_data(http, steam_ids)

        # Dont save the data. Its empty
        if response_data is None:
            if self.log_response:
                self.log.info("NO DATA TO SAVE")
            return

        file_name = "%s_to_%s" % (
            context["data_interval_start"].strftime("%Y-%m-%dT%H_%M_%SZ"),
            context["data_interval_end"].strftime("%Y-%m-%dT%H_%M_%SZ"),
        )

        s3_bucket_save_key = "%s%s%s%s" % (
            self.bucket_save_key,
            (
                "%s/%s/%s/"
                % (context["ds"][0:4], context["ds"][5:7], context["ds"][8::])
            ),
            file_name,
            ".json",
        )

        if self.log_response:
            self.log.info(
                "SAVING RESPONSE DATA FROM ENDPOINT: %s\nTO\nBucket: %s\nKey: %s\n"
                % (self.endpoint, self.bucket_name, s3_bucket_save_key)
            )

        # Save data
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_string(
            bucket_name=self.bucket_name,
            string_data=json.dumps(response_data, indent=4),
            key=s3_bucket_save_key,
        )

        context["ti"].xcom_push(key="s3_bucket_key", value=s3_bucket_save_key)
