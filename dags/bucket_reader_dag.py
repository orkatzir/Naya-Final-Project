from airflow import DAG
from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
default_args = {
    'owner': 'ork',
    'depends_on_past': False,
    'retries': 0
}

with DAG(
    dag_id='bucket_reader_dag',
    default_args=default_args,
    description='Stock Prediction',
    start_date=datetime(2020, 7, 29, 2),
    schedule_interval='35 2 * * *'
) as dag:
    task_1=SSHOperator(
        task_id="ssh_run_bucket_reader",
        ssh_conn_id='ssh_default',
        command='python /home/naya/Final_Project/read_bucket.py'
    )