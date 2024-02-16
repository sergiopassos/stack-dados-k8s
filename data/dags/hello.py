from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    return 'Hello'

def print_world():
    return 'World'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('simple_hello_world',
          default_args=default_args,
          description='A simple hello world DAG',
          schedule_interval=timedelta(days=1),
          catchup=False)

start_task = DummyOperator(task_id='start', dag=dag)

hello_task = PythonOperator(task_id='hello_task',
                            python_callable=print_hello,
                            dag=dag)

world_task = PythonOperator(task_id='world_task',
                            python_callable=print_world,
                            dag=dag)

start_task >> hello_task >> world_task
