# Standard library imports
import logging
import os
from time import sleep, time
from typing import List, Tuple

# Third-party imports
import pandas as pd
from tqdm import tqdm

# Local application/library specific imports
from sidrapi import SidraAPI, SidraManager
from postgres import PostgreSQL

class InfoDatasetPi:
    def __init__(self, 
                 list_of_tables: list = None,
                 output_dir: str = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"),
                 is_conected: bool = False) -> None:

        self.list_of_tables = list_of_tables
        logging.basicConfig(level=logging.INFO)

        self.list_df_tables = []
        self.list_df_variables = []
        self.list_df_categories = []

        self.output_dir = output_dir
        self.output_dirs = self.create_directories()
        self.sidra_service = SidraManager()
        self.sidra_api = SidraAPI()
        self.execution_interval = 5

        if is_conected:
            self.db = PostgreSQL(schema='datasetpi')


    def create_directories(self):
        # Dicionário de diretórios
        directories = {
            'geral': self.output_dir,
            'gold': os.path.join(self.output_dir, 'gold'),
            'silver': os.path.join(self.output_dir, 'silver'),
            'bronze': os.path.join(self.output_dir, 'bronze')
        }

        # Criar diretórios se create_remote_directory é True e os diretórios não existem
        for path in directories.values():
            os.makedirs(path, exist_ok=True)
        
        return directories
    

    def process_table_metadata(self, table, max_retries):
        retry_count = 0
        while retry_count <= max_retries:
            start_time = time()  # Tempo de início do loop
            try:
                data = self.sidra_service.sidra_get_metadata(table)
                sleep(self.execution_interval)
                if data:
                    return self.process_data(table, data)
                else:
                    logging.error(f"Falha ao obter os dados de {table}")
                    retry_count += 1
            except Exception as e:
                logging.error(f"Erro ao processar os dados de {table}: {e}")
                retry_count += 1

            elapsed_time = time() - start_time
            if elapsed_time > 120:  # Verifica se o tempo excedeu 2 minutos
                logging.error(f"Tempo limite excedido para a tabela {table}")
                break

        return None, retry_count
    
    
    def process_data(self, table, data):
        df_tables, df_table_info = self.sidra_service.sidra_process_table(data)
        df_variables = self.sidra_service.sidra_process_variables(data, table)
        df_categories = self.sidra_service.sidra_process_categories(data, table)

        self.list_df_tables.append(df_tables)
        self.list_df_variables.append(df_variables)
        self.list_df_categories.append(df_categories)

        if False:  # Verifica se deve criar tabelas no banco de dados
            self.db.set_schema('datasetpi')
            self.db.create_table(table_name='sidra_tabelas', df=df_tables)
            self.db.create_table(table_name='sidra_variaveis', df=df_variables)
            self.db.create_table(table_name='sidra_categorias', df=df_categories)
            self.create_remote_directory = False  # Evita a recriação das tabelas

            self.db.insert_into_table(table_name='sidra_tabelas', df=df_tables)
            self.db.insert_into_table(table_name='sidra_variaveis', df=df_variables)
            self.db.insert_into_table(table_name='sidra_categorias', df=df_categories)

        return {"table": table, "data": df_table_info}

    def batch_info(self, max_retries=3) -> tuple:
        metatable = []
        failed_requests = {}

        for table in tqdm(self.list_of_tables, total=len(self.list_of_tables), unit="Tables"):
            table_info, retries = self.process_table_metadata(table, max_retries)
            if table_info:
                metatable.append(table_info)
                # self.generate_excel_files(table_info.get('data'), f"sidra_info_{table}.xlsx")
            else:
                failed_requests[table] = retries

        final_df_tables = pd.concat(self.list_df_tables, ignore_index=True)
        final_df_variables = pd.concat(self.list_df_variables, ignore_index=True)
        final_df_categories = pd.concat(self.list_df_categories, ignore_index=True)

        # Geração de arquivos consolidados
        self.generate_excel_files(final_df_tables, "tables_adjusted.xlsx")
        self.generate_excel_files(final_df_variables, "variables_adjusted.xlsx")
        self.generate_excel_files(final_df_categories, "categories_adjusted.xlsx")

        return metatable, failed_requests

    def generate_excel_files(self, df, filename):
        df.to_excel(os.path.join(self.output_dirs.get('bronze'), filename), index=False)

    # def batch_info(self, max_retries=3) -> tuple:

    #     metatable = []
    #     failed_requests = {}
    #     create = True

    #     for table in tqdm(self.list_of_tables, total=len(self.list_of_tables), unit="Tables"):
    #         retry_count = 0

    #         while retry_count <= max_retries:
    #             start_time = time()  # Tempo de início do loop

    #             try:
    #                 data = self.sidra_service.sidra_get_metadata(table)
    #                 sleep(self.execution_interval)
                    
    #                 if data:
    #                     df_tables, df_table_info = self.sidra_service.sidra_process_table(data)
    #                     df_variables = self.sidra_service.sidra_process_variables(data, table)
    #                     df_categories = self.sidra_service.sidra_process_categories(data, table)

    #                     self.list_df_tables.append(df_tables)
    #                     self.list_df_variables.append(df_variables)
    #                     self.list_df_categories.append(df_categories)

    #                     metatable.append({"table": table, "data": df_table_info})
    #                     df_table_info.to_excel(f"{self.output_dirs['bronze']}/sidra_info_{table}.xlsx", index=False)

    #                     if create:
    #                         self.db.set_schema('datasetpi')
    #                         self.db.create_table(table_name='sidra_tabelas', df=df_tables)
    #                         self.db.create_table(table_name='sidra_variaveis', df=df_variables)
    #                         self.db.create_table(table_name='sidra_categorias', df=df_categories)
    #                     create = False

    #                     self.db.insert_into_table(table_name='sidra_tabelas', df=df_tables)
    #                     self.db.insert_into_table(table_name='sidra_variaveis', df=df_variables)
    #                     self.db.insert_into_table(table_name='sidra_categorias', df=df_categories)

    #                     break  # Sai do loop de retry ao sucesso
    #                 else:
    #                     logging.error(f"Falha ao obter os dados de {table}")
    #                     retry_count += 1
    #             except Exception as e:
    #                 logging.error(f"Erro ao processar os dados de {table}: {e}")
    #                 retry_count += 1

    #             elapsed_time = time() - start_time
    #             if elapsed_time > 120:  # Verifica se o tempo excedeu 2 minutos
    #                 logging.error(f"Tempo limite excedido para a tabela {table}")
    #                 break

    #         if retry_count > max_retries:
    #             logging.error(f"Todas as tentativas falharam para a tabela {table}")
    #             failed_requests[table] = retry_count

    #     # Concatenação e gravação dos dataframes
    #     final_df_tables = pd.concat(self.list_df_tables, ignore_index=True)
    #     final_df_variables = pd.concat(self.list_df_variables, ignore_index=True)
    #     final_df_categories = pd.concat(self.list_df_categories, ignore_index=True)

    #     final_df_tables.to_excel(f"{self.output_dirs['bronze']}/tables_adjusted.xlsx", index=False)
    #     final_df_variables.to_excel(f"{self.output_dirs['bronze']}/variables_adjusted.xlsx", index=False)
    #     final_df_categories.to_excel(f"{self.output_dirs['bronze']}/categories_adjusted.xlsx", index=False)

    #     return metatable, failed_requests