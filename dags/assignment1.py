from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

def print_world(execution_date, **kwargs):
    new_time = execution_date + timedelta(hours=1)
    print(f"New time is: {new_time}")

default_args = {
    'owner': 'viethq31',  
    'start_date': days_ago(1), 
    'retries': 1  
}

with DAG(
    'assignment1_viethq31',  
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),  
    catchup=False  #Only run future tasks, not past ones
) as dag:

    t1 = TimeDeltaSensor(
        task_id='wait_5_minutes',
        delta=timedelta(minutes=5)
    )

    t2 = BashOperator(
        task_id='print_execution_date',
        bash_command='echo "Execution date is: {{ execution_date.in_tz("Asia/Bangkok") }}"'  
    )

    t3 = PythonOperator(
        task_id='print_world',
        python_callable=print_world
    )

    t1 >> t2 >> t3
