from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    dim_sql_template =""" 
                        INSERT INTO {} ( {} )
                        {} ;
                        
                        """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 insert_command="",
                 columns = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table            = table
        self.insert_command   = insert_command
        self.columns          = columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info(f"clearing the table {self.table}")
        redshift.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info(f"Loading table {self.table}")
        
        dimension_sql=LoadDimensionOperator.dim_sql_template.format (
              self.table,
              self.columns, 
              self.insert_command 
        )
        redshift.run(dimension_sql)
        
        
