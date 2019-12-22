from datetime import datetime, timedelta
import os
import operators
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 12, 21),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'catchup_by_default': False,
}

dag = DAG('udaci_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
         )

start_operator = PostgresOperator(task_id='Begin_execution',
                                  dag=dag,
                                  postgres_conn_id='redshift',
                                  sql="create_tables.sql"
                                  )

stage_events_to_redshift = StageToRedshiftOperator(task_id = 'Stage_events',
                                                   dag = dag,
                                                   table = "staging_events",
                                                   json_path = "s3://udacity-dend/log_json_path.json",
                                                   file_type = 'json',
                                                   redshift_conn_id = 'redshift',
                                                   aws_conn_id = "aws_credentials",
                                                   s3_bucket = "udacity-dend",
                                                   s3_key = "log_data",                                                   
                                                   )

stage_songs_to_redshift = StageToRedshiftOperator(task_id = 'Stage_songs',
                                                   dag = dag,
                                                   table = "staging_songs",
                                                   json_path = "auto",
                                                   file_type = 'json',
                                                   redshift_conn_id = 'redshift',
                                                   aws_conn_id = "aws_credentials",
                                                   s3_bucket = "udacity-dend",
                                                   s3_key = "song_data/A/A/A",                                                   
                                                   )

load_songplays_table = LoadFactOperator(task_id='Load_songplays_fact_table',
                                        dag=dag,
                                        table = 'songplays',
                                        redshift_conn_id = 'redshift',
                                        aws_conn_id = 'aws_credentials',
                                        insert_sql_qry = SqlQueries.songplay_table_insert
                                        )

load_user_dimension_table = LoadDimensionOperator(task_id='Load_user_dim_table',
                                                  dag=dag,
                                                  table = 'users',
                                                  redshift_conn_id = 'redshift',
                                                  aws_conn_id = 'aws_credentials',
                                                  insert_sql_qry = SqlQueries.user_table_insert
                                                  )

load_song_dimension_table = LoadDimensionOperator(task_id='Load_song_dim_table',
                                                  dag=dag,
                                                  table = 'songs',
                                                  redshift_conn_id = 'redshift',
                                                  aws_conn_id = 'aws_credentials',
                                                  insert_sql_qry = SqlQueries.song_table_insert
                                                  )

load_artist_dimension_table = LoadDimensionOperator(task_id='Load_artist_dim_table',
                                                    dag=dag,
                                                    table = 'artists',
                                                    redshift_conn_id = 'redshift',
                                                    aws_conn_id = 'aws_credentials',
                                                    insert_sql_qry = SqlQueries.artist_table_insert
                                                    )

load_time_dimension_table = LoadDimensionOperator(task_id='Load_time_dim_table',
                                                  dag=dag,
                                                  table = 'time',
                                                  redshift_conn_id = 'redshift',
                                                  aws_conn_id = 'aws_credentials',
                                                  insert_sql_qry = SqlQueries.time_table_insert
                                                  )

run_quality_checks = DataQualityOperator(task_id='Run_data_quality_checks',
                                         dag=dag,
                                         tables = ['songplays', 'users', 'songs', 'artists', 'time'],
                                         redshift_conn_id = 'redshift'
                                         )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# task ordering:
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator