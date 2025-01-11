from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['carlosquintino3105@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'brewery_breweries',
    default_args=default_args,
    schedule_interval='0 9 * * *', 
    start_date=datetime(2024, 6, 17),
    catchup=False,
    tags=[ 'breweries', 'pipeline']
) as dag:

    
    capture_api = BashOperator(
        task_id='capture_api_task',
        bash_command="""
        docker exec docker-pyspark-1 \
        spark-submit setup/scripts/capture_api.py \
        --table_name breweries \
        --source brewery \
        --email_on_failure carlosquintino3105@gmail.com
        """
    )

   
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver_task',
        bash_command="""
        docker exec docker-pyspark-1 \
        spark-submit setup/scripts/bronze_to_silver.py \
        --table_name breweries \
        --source brewery \
        --email_on_failure carlosquintino3105@gmail.com
        """
    )

    
    silver_to_gold = BashOperator(
        task_id='silver_to_gold_task',
        bash_command="""
        docker exec docker-pyspark-1 \
        spark-submit setup/scripts/silver_to_gold.py \
        --table_name oregon_breweries \
        --source silver \
        --email_on_failure carlosquintino3105@gmail.com
        """
    )

    
    data_quality = BashOperator(
        task_id='data_quality_task',
        bash_command="""
        docker exec docker-pyspark-1 \
        spark-submit setup/scripts/data_quality.py \
        --table_name breweries \
        --email_on_failure carlosquintino3105@gmail.com
        """
    )

    # Define task dependencies
    capture_api >> bronze_to_silver >> [silver_to_gold, data_quality]