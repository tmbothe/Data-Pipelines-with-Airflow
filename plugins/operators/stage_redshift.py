import logging 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        {};
        COPY {}
        FROM '{}'
        compupdate off region '{}'
        {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
     """
    
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table     = "",
                 create_sql_stmt="",
                 data_path = "",
                 region    = "",
                 format    = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table            = table
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt  = create_sql_stmt
        self.aws_credentials_id = aws_credentials_id
        self.data_path        = data_path
        self.region           = region
        self.format           = format

    def execute(self, context):
        aws_hook    = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift    = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        logging.info(f"Copying table {self.table} to Redshift ....")
              
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.create_sql_stmt,
            self.table,
            self.data_path,
            self.region,
            self.format,
            credentials.access_key,
            credentials.secret_key
        )
        redshift.run(formatted_sql)
        
       





