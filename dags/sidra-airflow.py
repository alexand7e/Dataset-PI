
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from configure_dag import (configure_paths,
                           configure_managers,
                           get_sidra_api_info,
                           get_sidra_api_data,
                           configure_drive_repository)

# Supondo que as funções configure_paths, configure_managers, get_data_info, get_data, e configure_drive_repository estejam definidas corretamente

# def task_configure_paths(**kwargs):
#     _, _, info_data = configure_paths()  
#     kwargs['ti'].xcom_push(key='info_data', value=info_data)

# def task_configure_managers(**kwargs):
#     ti = kwargs['ti']
#     credentials_path, information_path, main_folder, knowledge_path = ti.xcom_pull(task_ids='configure_paths', key='return_value')
#     info_data = ti.xcom_pull(task_ids='configure_paths', key='info_data')
#     managers = configure_managers(credentials_path, information_path, knowledge_path)
#     return managers, info_data  

# def task_get_data_info(**kwargs):
#     ti = kwargs['ti']
#     sheet_manager, drive_manager, information_manager, sidra_manager, _ = ti.xcom_pull(task_ids='configure_managers', key='return_value')
#     data_info = get_sidra_api_info(sheet_manager, sidra_manager, information_manager.knowledge_path)
#     return data_info

# def task_get_data(**kwargs):
#     ti = kwargs['ti']
#     _, _, _, sidra_manager = ti.xcom_pull(task_ids='configure_managers', key='return_value')
#     df_tabelas, df_variaveis, df_categorias = ti.xcom_pull(task_ids='get_data_info', key='return_value')
#     get_sidra_api_data(sidra_manager, df_tabelas, df_variaveis)

# def task_configure_drive_repository(**kwargs):
#     ti = kwargs['ti']
#     _, drive_manager, json_manager, _ = ti.xcom_pull(task_ids='configure_managers', key='return_value')
#     information_path, _, _, knowledge_path = ti.xcom_pull(task_ids='configure_paths', key='return_value')
#     info_data = ti.xcom_pull(task_ids='configure_paths', key='info_data')
#     sheet_manager = ti.xcom_pull(task_ids='configure_managers', key='return_value')[0]  # Acessando sheet_manager diretamente
#     configure_drive_repository(drive_manager, sheet_manager, json_manager, information_path, knowledge_path, info_data)

def task_configure_paths(**kwargs):
    paths = configure_paths()  # Supondo que esta função retorne uma lista com 3 caminhos
    # Salva info_data no XCom
    kwargs['ti'].xcom_push(key='info_data', value=paths)
    # Retorna os caminhos para o próximo operador via XCom
    return paths

def task_configure_managers(**kwargs):
    ti = kwargs['ti']
    # A função configure_paths retorna uma lista, então devemos ajustar para o formato esperado
    paths = ti.xcom_pull(task_ids='configure_paths', key='info_data')
    credentials_path, information_path, knowledge_path = paths
    # Retorna os gerenciadores e info_data para serem usados nas próximas tarefas
    managers = configure_managers(credentials_path, information_path, knowledge_path)
    return managers 

def task_get_data_info(**kwargs):
    ti = kwargs['ti']

    _, _, knowledge_path = ti.xcom_pull(task_ids='configure_paths')
    managers = ti.xcom_pull(task_ids='configure_managers') 
    sheet_manager, _, _, sidra_manager = managers
    data_info = get_sidra_api_info(sheet_manager, sidra_manager, knowledge_path)

    # Retorna data_info para uso nas próximas tarefas
    return data_info

def task_get_data(**kwargs):
    ti = kwargs['ti']
    managers = ti.xcom_pull(task_ids='configure_managers')
    _, _, _, sidra_manager = managers
    df_tabelas, df_variaveis, _ = ti.xcom_pull(task_ids='get_data_info')
    # Supondo que get_sidra_api_data retorne os dados necessários
    data = get_sidra_api_data(sidra_manager, df_tabelas, df_variaveis)
    # O retorno dessa função pode variar dependendo do que get_sidra_api_data retorna
    return data

def task_configure_drive_repository(**kwargs):
    ti = kwargs['ti']
    managers = ti.xcom_pull(task_ids='configure_managers')
    _, drive_manager, json_manager, _ = managers
    paths = ti.xcom_pull(task_ids='configure_paths')
    _, information_path, knowledge_path = paths

    # Acessa sheet_manager diretamente da lista de gerenciadores
    sheet_manager = managers[0]  
    configure_drive_repository(drive_manager, sheet_manager, json_manager, information_path, knowledge_path)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG("sidra-pi-update-dag", 
         schedule_interval='0 0 1 * *', 
         default_args=default_args) as dag:
    
    t1 = PythonOperator(
        task_id='configure_paths',
        python_callable=task_configure_paths,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='configure_managers',
        python_callable=task_configure_managers,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='get_data_info',
        python_callable=task_get_data_info,
        provide_context=True
    )

    t4 = PythonOperator(
        task_id='get_data',
        python_callable=task_get_data,
        provide_context=True
    )

    t5 = PythonOperator(
        task_id='configure_drive_repository',
        python_callable=task_configure_drive_repository,
        provide_context=True
    )

    # Define a sequência de tarefas
    t1 >> t2 >> t3 >> t4 >> t5