#import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    facts_sql_template =""" 
                        INSERT INTO {} ( {} )
                        {} ;
                        """
    
    @apply_defaults
    def __init__(self,
                  redshift_conn_id  = "",
                  table = "",
                  columns ="", 
                  fact_command = "",
                  *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.fact_command = fact_command
       
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
       
        self.log.info(f'Loading data in {self.table}.')
        
        fact_sql = LoadFactOperator.facts_sql_template.format( 
            self.table,
            self.columns,
            self.fact_command
          )
            
        redshift.run(fact_sql)