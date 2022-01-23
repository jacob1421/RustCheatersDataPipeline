from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from requests.auth import HTTPBasicAuth
from airflow.exceptions import AirflowException
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class TwitterToS3Operator(BaseOperator):
    template_fields = ['request_params', 'bearer_token']

    def __init__(self, twitter_user_id, endpoint, http_conn_id, bearer_token, request_params, bucket_name, key, aws_conn_id, log_response=False,
                 response_check=None, **kwargs):
        super().__init__(**kwargs)
        self.twitter_user_id = twitter_user_id
        self.http_conn_id = http_conn_id
        self.log_response = log_response
        self.request_params = request_params or {}
        self.bearer_token = bearer_token
        self.endpoint = endpoint
        self.response_check = response_check
        self.bucket_name = bucket_name
        self.key = key
        self.aws_conn_id = aws_conn_id

    #https://developer.twitter.com/en/docs/twitter-ads-api/timezones
    def format_time_twitter_ISO8601(self, str_time):
        import pendulum
        dt = pendulum.parse(str_time)
        print(dt)
        print("%04i-%02i-%02iT%02i:%02i:%02iZ" % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second))
        return ("%04i-%02i-%02iT%02i:%02i:%02iZ" % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second))

    def execute(self, context):
        from airflow.utils.operator_helpers import determine_kwargs

        responses = {"responses": []}
        http = HttpHook(method="GET", http_conn_id=self.http_conn_id, auth_type=HTTPBasicAuth)

        self.log.info("Calling TWITTER HTTP method")

        headers = {
            "Authorization": "Bearer %s" % self.bearer_token,
            "Content-Type": "application/json"
        }

        self.endpoint = self.endpoint.replace(":id", str(self.twitter_user_id))

        if "start_time" in self.request_params:
            self.request_params["start_time"] = self.format_time_twitter_ISO8601(self.request_params["start_time"])
        if "end_time" in self.request_params:
            self.request_params["end_time"] = self.format_time_twitter_ISO8601(self.request_params["end_time"])

        pagination_token = True
        banned_players_found = True
        while pagination_token:
            response = http.run(endpoint=self.endpoint, data=self.request_params, headers=headers)

            if self.response_check:
                kwargs = determine_kwargs(self.response_check, [response], context)
                if not self.response_check(response, **kwargs):
                    raise AirflowException("Response check returned False.")

            if self.log_response:
                self.log.info("Request URL: %s\nRequest Response Text: %s\n" % (response.url, response.text))

            response_json = response.json()

            if response_json["meta"]["result_count"] > 0:
                responses["responses"].append(response_json)

                if "next_token" in response_json["meta"]:
                    if "start_time" in self.request_params:
                        del self.request_params["start_time"]
                    if "end_time" in self.request_params:
                        del self.request_params["end_time"]
                    self.request_params["pagination_token"] = response_json["meta"]["next_token"]
                    if self.log_response:
                        print(60*"*")
                        print("FOUND NEXT TOKEN: %s" % self.request_params["pagination_token"])
                        print(60*"*")
                else:
                    pagination_token = False
            else:
                if "pagination_token" not in self.request_params:
                    banned_players_found = False
                pagination_token = False

        if self.log_response:
            self.log.info(responses)

        if banned_players_found:
            if self.log_response:
                self.log.info("Saving Data To %s" % self.key)
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.load_string(
                bucket_name=self.bucket_name,
                string_data=json.dumps(responses, indent=4),
                key=self.key
            )

