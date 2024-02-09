from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 insert_mode="append-only",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        if self.insert_mode not in ['append-only', 'delete-load']:
            raise ValueError("Invalid insert mode")

        if self.insert_mode == "delete-load":
            self.log.info(f"Deleting data from dimension table {self.table}")
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info(f"Inserting data into dimension table {self.table}")
        formatted_sql = f"INSERT INTO {self.table} {self.sql}"
        redshift.run(formatted_sql)

        self.log.info(f"Data has been inserted into dimension table {self.table} successfully")
