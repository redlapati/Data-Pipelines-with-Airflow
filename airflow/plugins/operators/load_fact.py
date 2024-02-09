from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        formatted_sql = f"INSERT INTO {self.table} {self.sql}"

        self.log.info(f"Loading data into fact table {self.table}")
        redshift.run(formatted_sql)

        self.log.info(f"Data has been loaded into fact table {self.table} successfully")
