import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
from datetime import datetime, timedelta

from dags.main import DatasetPi

def process_data():
    output_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    with open(f'{output_dir}/preset-tables.json', 'r', encoding='utf-8-sig') as json_file:
        data = json.load(json_file)

    tabelas = [item['tabela'] for item in data if item['pasta'] == "Desenvolvimento Econômico"]

    executor = DatasetPi(tabelas)
    metatable, failed = executor.batch_info()
    executor.batch_extraction()
    executor.processed_template(metatable)

    if failed:
        executor = DatasetPi(failed)
        metatable, _ = executor.batch_info()
        executor.batch_extraction()
        executor.processed_template(metatable)

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dag-dataset-piaui',
    default_args=default_args,
    description='A monthly data processing DAG',
    schedule_interval='0 0 1 * *',  # Executa no primeiro dia de cada mês às 00:00
    catchup=False
)

# Tarefa de processamento de dados
process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)
