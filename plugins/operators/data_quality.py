import logging 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                  tables=[],
                  *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        

    def execute(self, context):
        """
         This function checks the quality of the ETL data:
           - Check if all tables are populatted meaning record count > 0
        """
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        for current_table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {current_table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {current_table} is not populated." )
                
        dq_checks = [ {'table':'artists','check_sql' : f"SELECT COUNT(*) FROM public.artists" ,'expected_result':10025}, 
                      {'table':'songs','check_sql' : f"SELECT COUNT(*) FROM public.songs " ,'expected_result':14896}, 
                      {'table':'time','check_sql' : f"SELECT COUNT(*) FROM public.time"   ,'expected_result':68200 }, 
                      {'table':'users','check_sql' : f"SELECT COUNT(*) FROM public.users   ",'expected_result':104} 
                        ]
            
            
        for current_check in dq_checks:
            table_name = current_check['table']
            records = redshift.get_records(current_check['check_sql'])
            
            if records[0][0] != current_check['expected_result']:
                raise ValueError(f"Data quality check failed for table {table_name}. Row count = {records[0][0]} expected {current_check['expected_result']}.")
                                 
            else:
                logging.info(f" {table_name} table has {records[0][0]} expected {current_check['expected_result']}.")
                
        logging.info("Data quality check successfull ! ")
                                 
                
                
                
                
                
            