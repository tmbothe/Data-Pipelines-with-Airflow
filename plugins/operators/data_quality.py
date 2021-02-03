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
                  column= "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column = column

    def execute(self, context):
        """
         This function checks the quality of the ETL data:
           - Check if all tables are populatted meaning record count > 0
        """
        
        redshift = PostgresHook(self.redshift_conn_id)
        #all_tables = self.table.split(',')
        
        logging.info(f"Checking if table {self.table} ") 
        
        for current_table in self.table:
            table_name  = current_table
            
            column1     = self.table[current_table].split(',')[0]
            column2     = self.table[current_table].split(',')[1]
            
            dq_checks = [ {'check_sql' : f"SELECT COUNT(*) FROM {table_name} WHERE {column1} IS NULL",'expected_result':0, 'comparison':'='}, # check all primary keys are not null
                          {'check_sql' : f"SELECT {column1}, COUNT(DISTINT({column1})) as distinct_rows  FROM {table_name} GROUP BY {column1}  HAVING COUNT(*) > 1 ",'expected_result':0, 'comparison':'='}, #expect_column_values_to_be_unique
                          {'check_sql' : f"SELECT COUNT(*) FROM {table_name} WHERE {column2} IS NULL",'expected_result':0, 'comparison':'='}, #expect_column_values_to_match_strftime_format
                          {'check_sql' : f"SELECT COUNT(*) FROM {table_name} WHERE {column2} IS NULL",'expected_result':0, 'comparison':'>'} #expect_table_row_count_greater than zero
                        ]
            
            logging.info(f"Checking if table {table_name} has null in primary key column {column1} ") 
            
            not_null_record_count = redshift.get_records(dq_checks[0]['check_sql'])
            
            if len(not_null_record_count[0]) < 1 or len(not_null_record_count) < 1:
                raise ValueError(f"Data quality check failed. {table_name} has null values.")
            else:
                logging.info(f' {table_name}  does not have any null values.")')
                
            #Checking duplicates in primary key
            
            logging.info(f"Checking if table {table_name} has duplicated values in primary key column {column1} ") 
            
            duplicate_records = redshift.get_records(dq_checks[1]['check_sql'])
            
            if len(duplicate_records[0]) < 1 or len(duplicate_records) < 1 :
                raise ValueError(f"Data quality check failed. {table_name} has {duplicate_records[1][0]}  duplicated values")
            else:
                logging.info(f' {table_name}  table has  {duplicate_records[1][0]} duplicated rows ")')
                
           
                
                
                
                
                
            