from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimsOperator(BaseOperator):
    template_fields = ["bucket_key"]

    sql = """
            --NEEDED FOR AWS POSTGRES PLUGIN S3 -> Postgres Table
            CREATE EXTENSION IF NOT EXISTS aws_commons;
            CREATE EXTENSION IF NOT EXISTS aws_s3;
            --Create temp table for staged data
            CREATE TEMPORARY TABLE temp_dim ON COMMIT DROP AS
            SELECT {columns} FROM {schema}."{table}" LIMIT 0;
            --Copy data from S3 Bucket to Database
            select aws_s3.table_import_from_s3 (
               'temp_dim',
               '{columns}',
               '(FORMAT CSV, HEADER)',
               '{bucket}',
               '{bucket_key}',
               '{region}'
            );
            --Insert data that does not exist in our dimension table
            INSERT INTO {schema}."{table}"({columns})
            SELECT {columns} FROM temp_dim
            ON CONFLICT({conflict_columns})
            DO {conflict_action};
            """

    def __init__(
        self,
        postgres_conn_id="postgres_default",
        autocommit=False,
        database=None,
        columns="",
        table="",
        schema="",
        bucket="",
        bucket_key="",
        region="",
        conflict_columns="",
        conflict_action="",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.database = database
        self.hook = None
        self.columns = columns
        self.table = table
        self.schema = schema
        self.bucket = bucket
        self.bucket_key = bucket_key
        self.region = region
        self.conflict_action = conflict_action
        self.conflict_columns = conflict_columns

    def execute(self, context):
        self.hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        self.hook.run(
            LoadDimsOperator.sql.format(
                columns=self.columns,
                table=self.table,
                schema=self.schema,
                bucket=self.bucket,
                bucket_key=self.bucket_key,
                region=self.region,
                conflict_action=self.conflict_action,
                conflict_columns=self.conflict_columns,
            ),
            self.autocommit,
        )
        for output in self.hook.conn.notices:
            self.log.info(output)
