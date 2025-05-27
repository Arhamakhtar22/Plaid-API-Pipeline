# orchestration/pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add project directory to path to import modules
sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', ''), '..', 'financial-pipeline'))
from extraction.plaid_connector import main as extract_data
from extraction.load_to_snowflake import main as load_to_snowflake

# Default arguments
default_args = {
    'owner': 'financial_analyst',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='Extract, transform and analyze financial transaction data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'etl'],
)

# Task 1: Extract data from Plaid API
extract_task = PythonOperator(
    task_id='extract_from_plaid',
    python_callable=extract_data,
    dag=dag,
)

# Task 2: Load data to Snowflake
load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag,
)

# Task 3: Run dbt transformations
dbt_task = BashOperator(
    task_id='run_dbt_transformations',
    bash_command='cd $AIRFLOW_HOME/../financial-pipeline/transformation && dbt run',
    dag=dag,
)

# Task 4: Run dbt tests
dbt_test_task = BashOperator(
    task_id='test_dbt_models',
    bash_command='cd $AIRFLOW_HOME/../financial-pipeline/transformation && dbt test',
    dag=dag,
)

# Set task dependencies
extract_task >> load_task >> dbt_task >> dbt_test_task