from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import date
import os
import pendulum


local_tz = pendulum.timezone('Europe/Moscow')
today = date.today()
dir_path = os.path.dirname(os.path.realpath(__file__))


with DAG(
        dag_id=f'rates_report',
        start_date=datetime(2022, 10, 1),
        schedule_interval='20 8 * * *',
) as dag:

    from function_writer import write

    writer_task = PythonOperator(
        task_id="rates_report",
        python_callable=write,
        dag=dag
    )