import logging 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                  table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        """
         This function checks the quality of the ETL data:
           - Check if all tables are populatted meaning record count > 0
           
        """
        redshift = PostgresHook(self.redshift_conn_id)
        all_tables = self.table.split(',')
        
        for current_table in all_tables:
            record_count = redshift.get_records(f"SELECT COUNT(*) FROM  {current_table}")
            
            logging.info(f"Quality check for table {current_table} ")
            if len(record_count) < 1 or len(record_count[0]) < 1:
                raise ValueError(f"Data quality check failed. {current_table} is not populated")
                num_records = record_count[0][0]
                if num_records < 1:
                    logging.info(f'Data quality check failed. {current_table} is not populated")')
               
            else:
                logging.info(f' {current_table}  table has  {record_count[0][0]:,2f} rows ")')
            