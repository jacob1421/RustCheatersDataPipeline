from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimsOperator(BaseOperator):
    template_fields = ["bucket_key"]

    sql = """
    
    """

    def __init__(self,
                 postgres_conn_id='postgres_default',
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
                 **kwargs):
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
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        self.hook.run(S3ToPostgresOperator.sql.format(columns=self.columns, table=self.table, schema=self.schema,
                                                      bucket=self.bucket, bucket_key=self.bucket_key,
                                                      region=self.region, conflict_action=self.conflict_action,
                                                      conflict_columns=self.conflict_columns), self.autocommit)
        for output in self.hook.conn.notices:
            self.log.info(output)
