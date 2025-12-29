# airflow_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

dag = DAG(
    'banking_etl',
    start_date=days_ago(1),
    schedule_interval='@daily'
)

generate_data = BashOperator(
    task_id='generate_data',
    bash_command='python3 banking_datawarehouse_project/scripts/banking_data.py',
    dag=dag
)

etl_spark = BashOperator(
    task_id='etl_spark',
    bash_command='spark-submit banking_datawarehouse_project/scripts/etl_spark.py',
    dag=dag
)

load_postgres = BashOperator(
    task_id='load_postgres',
    bash_command='python3 banking_datawarehouse_project/scripts/load_postgres.py',
    dag=dag
)

generate_data >> etl_spark >> load_postgres
