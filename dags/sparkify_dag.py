from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                               CreateTablesOperator,SongPopularityOperator,
                              UnloadToS3Operator)
from helpers import SqlQueries

default_args = {
    'owner': 'subham',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_redshift_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    region="us-west-2",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = "users",
    mode=0,
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = "songs",
    mode=0,
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = "artists",
    mode=0,
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = "time",
    mode=0,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    tables=["songplay", "users", "song", "artist", "time"]
)

calculate_song_popularity = SongPopularityOperator(
    task_id="calculate_song_popularity",
    dag=dag,
    redshift_conn_id = "redshift",
    destination_table = "songpopularity",
    origin_table = "songplays",
    origin_dim_table = "songs",
    groupby_column = "title",
    fact_column = "duration",
    join_column = "songid"
)

unload_to_s3 = UnloadToS3Operator(
    task_id="unload_to_s3",
    dag=dag,
    redshift_conn_id = "redshift",
    source_table = "songpopularity",
    s3_bucket = "sparkify-topsongs",
    s3_key = "songpopularity_{ds}",
    aws_credentials_id="aws_credentials"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                           load_time_dimension_table] >> run_quality_checks
run_quality_checks >> calculate_song_popularity
calculate_song_popularity >> unload_to_s3
unload_to_s3 >> end_operator
