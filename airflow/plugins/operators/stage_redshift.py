from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)

    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        JSON '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json_path=json_path
        self.region=region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info("Loading data from s3 to redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.json_path,
            credentials.access_key,
            credentials.secret_key,
            self.region
        )
        redshift.run(formatted_sql)

        self.log.info("Data has been loaded into redshift successfully")
