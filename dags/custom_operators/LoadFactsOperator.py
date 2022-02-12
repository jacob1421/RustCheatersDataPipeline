from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactsOperator(BaseOperator):
    template_fields = ["bucket_key"]

    def __init__(
        self,
        postgres_conn_id="postgres_default",
        autocommit=False,
        database=None,
        schema="",
        bucket="",
        bucket_key="",
        region="",
        sql="",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.database = database
        self.hook = None
        self.schema = schema
        self.bucket = bucket
        self.bucket_key = bucket_key
        self.region = region
        self.sql = sql

    def execute(self, context):
        self.hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        self.hook.run(
            self.sql.format(
                schema=self.schema,
                bucket=self.bucket,
                bucket_key=self.bucket_key,
                region=self.region,
            ),
            self.autocommit,
        )
        for output in self.hook.conn.notices:
            self.log.info(output)
