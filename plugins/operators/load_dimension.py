import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    dim_sql_template =""" 
                        INSERT INTO {} ( {} )
                        {} ;
                        
                        """
    
    create_dim_table_sql = " {} ;"
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 insert_command="",
                 operation_type = "",
                 create_sql_stmt ="",
                 columns = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt  = create_sql_stmt
        self.operation_type   = operation_type
        self.table            = table
        self.insert_command   = insert_command
        self.columns          = columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        
        logging.info(f"Create table  {self.table} if not exist ...")
        
        create_dim_table= LoadDimensionOperator.create_dim_table_sql.format(self.create_sql_stmt)
        redshift.run(create_dim_table)
                            
        records =redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
                
        if (records[0][0] > 0  and self.operation_type == 'TRUNCATE'):
            logging.info(f"clearing the table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        elif (records[0][0]==0  and self.operation_type == 'TRUNCATE'):
            logging.info(f"Table {self.table} empty not need to truncate ...")  
        elif self.operation_type == 'INSERT':
            logging.info(f"Inserting rows into {self.table} table ...")
        else:
            raise ValueError('The operation type should be INSERT OR TRUNCATE !')
       
        logging.info(f"Loading data into {self.table} table .....")
        dimension_sql=LoadDimensionOperator.dim_sql_template.format (
              self.table,
              self.columns, 
              self.insert_command 
         )      
            
        redshift.run(dimension_sql)
        
                
        
        
        
