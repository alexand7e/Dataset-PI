# Standard library imports
import json
import logging
import os
import re
import unicodedata
from time import sleep, time
from typing import List, Tuple

# Third-party imports
import pandas as pd
from tqdm import tqdm

# Local application/library specific imports
from dags.local_directory import DirectoryManager
from dags.remote_directory import GoogleDriveManager
from sidrapi import SidraAPI, SidraManager
from postgres import PostgreSQL

# Configuração das pastas de destino
class DatasetPi:
    def __init__(self, 
                 list_of_tables: list = None,
                 output_dir: str = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"),
                 create_remote_directory: bool = False) -> None:
    
        self.output_dirs = {
            'geral': output_dir,
            'gold': os.path.join( output_dir , 'gold' ),
            'silver': os.path.join( output_dir , 'silver' ),
            'bronze': os.path.join( output_dir , 'bronze' )
        }

        self.sidra_service = SidraManager()
        self.sidra_api = SidraAPI()
        self.db = PostgreSQL(schema='datasetpi')

        self.list_of_tables = list_of_tables
        self.output_dir = output_dir
        self.gd = GoogleDriveManager(os.path.join(output_dir, 'credentials.json'))

        if create_remote_directory:
            self.main_folder_id, self.url_banco = self.gd.create_folder('Banco de Dados de Emprego - Piauí', make_public=True)
            print(f'banco de dados criado em {self.url_banco}')

        self.execution_interval = 1
        logging.basicConfig(level=logging.INFO)
        
    def batch_extraction(self):

        df_tables = pd.read_excel(f"{self.output_dirs['bronze']}/tables_adjusted.xlsx", dtype=str)
        df_variables = pd.read_excel(f"{self.output_dirs['bronze']}/variables_adjusted.xlsx", dtype=str)
        df_categories = pd.read_excel(f"{self.output_dirs['bronze']}/categories_adjusted.xlsx", dtype=str)

        for idx, row in df_tables.iterrows():
            table_number = row["id"]
            variaveis_filtradas = df_variables[df_variables["Tabela"] == table_number]

            pages_data: List[pd.DataFrame] = []
            pages_names: List[str] = []

            unique_categories = df_categories[df_categories["Tabela"] == table_number]['classificacao_id'].unique().tolist()
            unique_categories = [f'c{category}' for category in unique_categories if category is not None and category != '']
            categories_str = '/all/'.join(unique_categories) + '/all/' if unique_categories else ''
            assunto = self.format_string(row['assunto'])

            for _, row_var in variaveis_filtradas.iterrows():
                try:
                    self.sidra_api.build_url(
                        tabela=table_number,
                        variavel=row_var['id'],
                        classificacao=categories_str,
                        nivel_territorial=row["Nível Territorial"],
                        periodo={'Frequência': row["Frequência"], 
                                 'Inicio': row["Data Inicial"], 
                                 'Final': row["Data Final"]}
                    )

                    df = self.sidra_api.fetch_data()
                    self.process_dataframe(df, table_number, row_var, assunto)

                    pages_data.append(df)
                    pages_names.append(f'Variável {row_var["id"]}')

                    sleep(self.execution_interval)

                except Exception as e:
                    print(f"Um erro ocorreu na tabela {table_number}, variável {row_var['id']}: {e}")
                    sleep(10)

            if pages_data:
                with pd.ExcelWriter(f'{self.output_dirs.get("silver")}/Tabela {table_number}.xlsx', engine='openpyxl') as writer:
                    for df, nome_aba in zip(pages_data, pages_names):
                        df.to_excel(writer, sheet_name=nome_aba, index=False)
            else:
                print(f"Nenhum dado para escrever em Excel: {table_number}")

    def process_dataframe(self, df, table_number, row_var, assunto):
        if df is not None:
            df_banco = df.copy()
            df_banco.columns = [self.format_string(col) for col in df_banco.columns]
            self.db.set_schema(assunto)
            self.db.create_table(df=df_banco, table_name=f'tabela_{table_number}_var_{row_var["id"]}', adjust_dataframe=False)
        else:
            print(f"Nenhum dado retornado para tabela {table_number}, variável {row_var['id']}")

    def processed_template(self):
        try:
            dm = DirectoryManager(self.output_dirs['bronze'], self.output_dirs['gold'])
            file_list_df = dm._list_files()
            file_list_df['filename'] = file_list_df['filename'].astype(str)
            file_list_df['table_number'] = file_list_df['filename'].str.extract('(\d+)').astype(str)
            file_list_df = file_list_df[file_list_df['table_number'].str.isdigit()]

            template = f"{self.output_dirs.get('geral')}/template.xlsx"

            for _, file_info in file_list_df.iterrows():
                try:
                    data_df = pd.read_excel(file_info['full_filename'])
                    table_number = file_info['table_number']
                    output_path = f"{self.output_dirs['silver']}\\Tabela {table_number}.xlsx"

                    if os.path.exists(output_path):
                        dm.process_template_file(data_df, template, output_path, table_number)
                        logging.info(f"Tabela processada: {table_number}.")
                        
                except Exception as e:
                    logging.error(f"Erro ao processar a tabela {table_number}: {e}")

        except Exception as e:
            logging.error(f"Failed to process data files: {e}")

    def format_string(self, input_string):
        normalized_string = unicodedata.normalize('NFKD', input_string)
        cleaned_string = ''.join(char for char in normalized_string if not unicodedata.combining(char))
        
        cleaned_string = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned_string)
        cleaned_string = cleaned_string.lower()
        cleaned_string = cleaned_string.replace(' ', '_').replace('-', '_')
        return cleaned_string
    
    def process_data(self):
        dm = DirectoryManager(self.output_dirs.get('gold'), None)
        df = dm._list_files()
        df['filename'] = df['filename'].astype(str)
        df['tabela'] = df['filename'].str.extract('(\d+)').astype(str)
        df = df[df['tabela'].str.isdigit()]

        df_tables = self.db.read_table_columns('sidra_tabelas', columns=['*'], return_type='dataframe')
        df_tables['id'] = df_tables['id'].astype(str)

        df_final = df.merge(df_tables, left_on='tabela', right_on='id', how='inner')
        df_final = df_final[['tabela', 'filename', 'full_filename', 'assunto']]

        df_final['gdrive_id'], df_final['url'] = zip(*df_final.apply(self.upload_to_drive, axis=1))
        df_final['download'] = df_final['gdrive_id'].apply(lambda x: f"https://drive.google.com/uc?export=download&id={x}")

        self.db.set_schema('datasetpi')
        self.db.create_table('sidra_url_banco', df_final)

        print(df_final)

    def upload_to_drive(self, row):
        folder_id, _ = self.gd.create_folder(row['assunto'], parent_folder_id=self.main_folder_id)
        file_id, file_url = self.gd.upload_file(row['full_filename'], parent_folder_id=folder_id)
        return file_id, file_url
    
    def configure_schemas_files(self):
        schemas = ['trabalho', 'domicilios', 'mercado_de_trabalho', 'rendimento', 'populacao_desocupada', 'pessoal_ocupado', 'rendimento_de_todas_as_fontes', 'características_do_trabalho_e_apoio_social']

        for schema in schemas:
            db = PostgreSQL(schema=schema)
            df = db.read_all_tables()
            # Define o caminho onde o DataFrame será salvo
            file_path = os.path.join(self.output_dir, f'{schema}_data.csv')
            # Salva o DataFrame em um arquivo CSV
            df.to_csv(file_path, index=False)
            print(f'DataFrame do esquema {schema} salvo em {file_path}')


if __name__ == "__main__": 

    output_dir: str = os.path.join(os.path.dirname(__file__), "..", "data")
    with open(f'{output_dir}/preset-tables.json', 'r', encoding='utf-8-sig') as json_file:
        data = json.load(json_file)

    tabelas = [item['tabela'] for item in data]

    executor = DatasetPi(tabelas)
    # metatable, failed = executor.batch_info()
    # executor.batch_extraction()
    executor.processed_template()
    executor.process_data()

    # if failed:
    #     executor = DatasetPi(failed)
    #     metatable, _ = executor.batch_info()
    #     executor.batch_extraction()
    #     executor.processed_template(metatable)




# def individual_table_interaction(table_number: int):
#     sidra_service = SidraManager() 
#     sidra_api = SidraAPI()

#     metadata = sidra_service.sidra_get_metadata(table_number)
#     tabela = sidra_service.sidra_process_table(metadata)[0]
#     variaveis = sidra_service.sidra_process_variables(metadata, table_number)
#     categorias = sidra_service.sidra_process_categories(metadata, table_number)

#     pages_data = []
#     pages_names = []

#     for idx, var in variaveis.iterrows():

#         try:
#             sidra_api.build_url(t=table_number,
#                                 v=var['id'],
#                                 c=''.join(categorias[1]),
#                                 n=tabela.loc[0, 'Nível Territorial'],
#                                 p=tabela.loc[0, 'Frequência'])
            
#             response = sidra_api.fetch_data()
#             df = pd.DataFrame(response)
#             df.columns = df.iloc[0]

#             if df is not None:
#                 pages_names.append(f"Variável {var['id']}")
#                 pages_data.append(df)
#             sleep(1.02)
            
#         except Exception as e:

#             print(f"Erro ao obter dados: {table_number}: {e}")
#             sleep(10)
#             continue

#     if pages_data:
#         with pd.ExcelWriter(f'{output_dirs.get("silver")}\\Tabela {table_number}.xlsx', engine='openpyxl') as writer:
#             for tab, nome_aba in zip(pages_data, pages_names):
#                 tab.to_excel(writer, sheet_name=nome_aba, index=False)
    
#     return pages_data