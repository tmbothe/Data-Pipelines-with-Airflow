from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import  SqlQueries


#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now() #datetime(2019, 1, 12),
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= None, #'@daily'
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_events",
    data_path ="s3://udacity-dend/log_data",
    region = "us-west-2",
    format = "json 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id ='stage_songs',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "public.staging_songs",
    data_path ="s3://udacity-dend/song_data",
    region = "us-west-2",
    format = "json 'auto'",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    redshift_conn_id = "redshift",
    table = "public.songplays",
    columns = "playid,start_time,userid,level,songid,artistid,sessionid,location,user_agent",
    fact_command = SqlQueries.songplay_table_insert,
    dag=dag
    
)


load_user_dimension_table = LoadDimensionOperator(
     task_id = 'Load_user_dim_table',
     redshift_conn_id = "redshift",
     table = 'public.users',
     columns = "userid,first_name,last_name,gender,level",
     insert_command = SqlQueries.user_table_insert,
     dag = dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    redshift_conn_id = "redshift",
    table = 'public.songs',
    columns = "songid,title,artistid,year,duration",
    insert_command = SqlQueries.song_table_insert,
    dag = dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    redshift_conn_id = "redshift",
    table = 'public.artists',
    columns = "artistid,name,location,lattitude,longitude",
    insert_command= SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table   = 'public.time',
    columns = "start_time,hour,day,week,month,year,weekday" ,
    insert_command = SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table = "public.artists,public.songplays,public.songs,public.time,public.users",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift 

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_time_dimension_table   >>  run_quality_checks
load_user_dimension_table   >>  run_quality_checks
load_song_dimension_table   >>  run_quality_checks
load_artist_dimension_table >>  run_quality_checks
 
run_quality_checks >>  end_operator