from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from move_files.script_to_dag import etl_principal, etl_secundario, envia_email_e_move_arquivos

default_args = {
    'owner': 'Camila Costa',
    'depends_on_past': False,
    "start_date": days_ago(0,0,0,0,0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'move_dag',
    default_args=default_args,
    description='Processo de ETL para o desafio da MOVE',
    schedule_interval='@monthly', 
    catchup=False,
)


run_etl_principal = PythonOperator(
    task_id='ETL_principal',
    python_callable=etl_principal,
    dag=dag,
)

run_etl_secundario = PythonOperator(
    task_id='ETL_secundario',
    python_callable=etl_secundario,
    dag=dag,
)

move_arquivos = PythonOperator(
    task_id='move_arquivos',
    python_callable=envia_email_e_move_arquivos,
    dag=dag,
)

run_etl_principal >> run_etl_secundario >> move_arquivos