from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import  SqlQueries
import sql_statements



default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'Retries': 3,
    'Retry_delay': timedelta(minutes=5),
    'Catchup'  : False,
    'start_date': datetime(2019, 1, 12),
    'redshift_conn_id':"redshift"
}

dag = DAG('udac_airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@hourly',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    create_sql_stmt = sql_statements.CREATE_STAGING_EVENTS_TABLE_SQL,
    table="public.staging_events",
    data_path ="s3://udacity-dend/log_data",
    region = "us-west-2",
    format = "json",
    extract_format = "s3://udacity-dend/log_json_path.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id ='stage_songs',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    create_sql_stmt = sql_statements.CREATE_STAGING_SONGS_TABLE_SQL,
    table = "public.staging_songs",
    data_path ="s3://udacity-dend/song_data",
    region = "us-west-2",
    format = "json",
    extract_format = 'auto',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    operation_type = 'TRUNCATE',
    create_sql_stmt = sql_statements.CREATE_SONGPLAYS_TABLE_SQL,
    table = "public.songplays",
    columns = "playid,start_time,userid,level,songid,artistid,sessionid,location,user_agent",
    fact_command = SqlQueries.songplay_table_insert,
    dag=dag
    
)


load_user_dimension_table = LoadDimensionOperator(
     task_id = 'Load_user_dim_table',
     operation_type = 'TRUNCATE',
     create_sql_stmt = sql_statements.CREATE_USERS_TABLE_SQL,
     table = 'public.users',
     columns = "userid,first_name,last_name,gender,level",
     insert_command = SqlQueries.user_table_insert,
     dag = dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    operation_type = 'TRUNCATE',
    create_sql_stmt = sql_statements.CREATE_SONGS_TABLE_SQL,
    table = 'public.songs',
    columns = "songid,title,artistid,year,duration",
    insert_command = SqlQueries.song_table_insert,
    dag = dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    operation_type = 'TRUNCATE',
    create_sql_stmt = sql_statements.CREATE_ARTISTS_TABLE_SQL,
    table = 'public.artists',
    columns = "artistid,name,location,lattitude,longitude",
    insert_command= SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    operation_type = 'TRUNCATE',
    create_sql_stmt = sql_statements.CREATE_TIME_TABLE_SQL,
    table   = 'public.time',
    columns = "start_time,hour,day,week,month,year,weekday" ,
    insert_command = SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    tables = ["artists", "songplays","songs","time","users"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift ] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table ] >> run_quality_checks
 
run_quality_checks >>  end_operator