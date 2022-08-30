import os, sys

import datetime
from pathlib import Path
from os.path import join

from airflow.models import DAG
from operators.hltv_operator import HltvOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 2),
   
}
BASE_FOLDER = join(
    './raw/{source}/{partition}'
)
PARTITION_FOLDER = 'extract_date={{ ds }}'

with DAG(
    dag_id='hltv_dag', 
    default_args=ARGS,
    schedule_interval='@daily',
    max_active_runs=1
    ) as dag:
    
    hltv_operator_task = HltvOperator(
        task_id='get_data',
        file_path=join(
            BASE_FOLDER.format(source='hltv', partition=PARTITION_FOLDER),
            "HLTV_{{ ds_nodash }}.json"
        )
    )

    spark_transform_data = SparkSubmitOperator(
        task_id='spark_transform',
        application=join(
            str(Path(__file__).parents[2]),
            'spark/get_matchs.py'
        ),
        name='hltv_transformation', 
        application_args=[
            '--src',
            BASE_FOLDER.format(source='hltv', partition=PARTITION_FOLDER),
            '--process-date',
            '{{ ds }}'
        ]
    )
    
    dbt_transform_task = BashOperator(task_id='dbt_transform', bash_command="cd /app/dbt/hltv_data && dbt run")

    hltv_operator_task >> spark_transform_data >> dbt_transform_task