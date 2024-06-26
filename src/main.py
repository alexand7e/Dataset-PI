# Standard library imports
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from time import sleep, time
from typing import List, Tuple

# Local application/library specific imports
from src.local_directory import DirectoryManager
from src.remote_directory import GoogleDriveManager
from src.sidra_extraction import SidraMetadataExecute
from src.database_manager import PostgreSQL

import logging
import pandas as pd
from typing import List, Tuple, Optional

class Main:
    """
    Classe principal para gerenciar a criação de diretórios, conexão com banco de dados e processamento de dados usando o SIDRA.

    Atributos:
        list_of_tables (list): Lista de IDs de tabelas a serem processadas.
        gd (GoogleDriveManager): Gerenciador de operações no Google Drive.
        db (PostgreSQL): Instância do banco de dados PostgreSQL.
        execution_interval (int): Intervalo de execução entre operações.
        main_folder_id (str): ID da pasta principal no Google Drive.
        url_banco (str): URL da pasta principal no Google Drive.
    """

    def __init__(self, 
                 list_of_tables: Optional[List[int]] = None,
                 create_remote_directory: bool = False,
                 conecting_db: bool = False) -> None:
        """
        Inicializa a classe Main com configurações de diretórios, Google Drive e banco de dados.

        Parâmetros:
            list_of_tables (Optional[List[int]]): Lista de IDs de tabelas para processamento.
            create_remote_directory (bool): Define se um diretório remoto deve ser criado no Google Drive.
            conecting_db (bool): Define se a conexão com o banco de dados deve ser estabelecida.
        """
        self.setup_directories()
        self.setup_google_drive(create_remote_directory)
        self.setup_database(conecting_db)

        self.list_of_tables = list_of_tables
        self.execution_interval = 1
        logging.basicConfig(level=logging.INFO)

    def main(self):
        """
        Método principal para orquestrar o processamento dos dados em três etapas:
        1. Processamento dos metadados (bronze).
        2. Extração dos dados (silver).
        3. Aplicação do template e processamento final (gold).
        """
        sidra_executor = SidraMetadataExecute(self.list_of_tables)
        sidra_executor.batch_info()
        sidra_executor.batch_extraction()
        sidra_executor.processed_template()

    def process_data(self):
        """
        Processa os dados finais e faz o upload para o Google Drive, atualizando a informação no banco de dados.
        """
        dm = DirectoryManager(origin_directory=self.output_dirs.get('gold'), destiny_directory=self.output_dirs.get('gold'))
        df = dm._list_files()
        df['filename'] = df['filename'].astype(str)
        df['tabela'] = df['filename'].str.extract('(\d+)').astype(str)
        df = df[df['tabela'].str.isdigit()]

        df['assunto'] = "Padrão"
        df_final = df[['tabela', 'filename', 'full_filename', 'assunto']]

        df_final['gdrive_id'], df_final['url'] = zip(*df_final.apply(self.upload_to_drive, axis=1))
        df_final['download'] = df_final['gdrive_id'].apply(lambda x: f"https://drive.google.com/uc?export=download&id={x}")

    def upload_to_drive(self, row) -> Tuple[str, str]:
        """
        Faz o upload de um arquivo para o Google Drive em uma pasta específica.

        Parâmetros:
            row (pd.Series): Linha do DataFrame contendo informações do arquivo.

        Retorna:
            Tuple[str, str]: ID do arquivo no Google Drive e URL de acesso.
        """
        folder_id, _ = self.gd.create_folder(row['assunto'], parent_folder_id=self.main_folder_id)
        file_id, file_url = self.gd.upload_file(row['full_filename'], parent_folder_id=folder_id)
        return file_id, file_url

    def setup_directories(self) -> None:
        """
        Configura os diretórios necessários para a execução.
        """
        self.directory_manager = DirectoryManager()
        self.output_dirs = self.directory_manager._create_directories()

    def setup_google_drive(self, create_remote_directory: bool) -> None:
        """
        Configura o Google Drive para upload dos arquivos processados.

        Parâmetros:
            create_remote_directory (bool): Define se um diretório remoto deve ser criado no Google Drive.
        """
        self.gd = GoogleDriveManager(os.path.join(self.output_dirs.get('geral'), 'credentials.json'))
        if create_remote_directory:
            self.main_folder_id, self.url_banco = self.gd.create_folder('Teste de Banco', make_public=True)
            print(f'banco de dados criado em {self.url_banco}')

    def setup_database(self, conecting_db: bool) -> None:
        """
        Configura a conexão com o banco de dados, se necessário.

        Parâmetros:
            conecting_db (bool): Define se a conexão com o banco de dados deve ser estabelecida.
        """
        if conecting_db:
            self.db = PostgreSQL(schema='datasetpi')

if __name__ == "__main__":
    # Configurações iniciais
    list_of_tables = [109, 4090]  # Exemplo de tabelas
    create_remote_directory = True
    conecting_db = False

    # Inicializa a classe principal e executa o processamento
    main_process = Main(list_of_tables, create_remote_directory, conecting_db)
    # main_process.main()
    main_process.process_data()


        

    # def configure_schemas_files(self):
    #     schemas = ['trabalho', 'domicilios', 'mercado_de_trabalho', 'rendimento', 'populacao_desocupada', 'pessoal_ocupado', 'rendimento_de_todas_as_fontes', 'características_do_trabalho_e_apoio_social']

    #     for schema in schemas:
    #         db = PostgreSQL(schema=schema)
    #         df = db.read_all_tables()
    #         # Define o caminho onde o DataFrame será salvo
    #         file_path = os.path.join(self.output_dirs.get('geral'), f'{schema}_data.csv')
    #         # Salva o DataFrame em um arquivo CSV
    #         df.to_csv(file_path, index=False)
    #         print(f'DataFrame do esquema {schema} salvo em {file_path}')
 
    # def batch_extraction(self):

    #     df_tables = pd.read_excel(f"{self.output_dirs['bronze']}/tables_adjusted.xlsx", dtype=str)
    #     df_variables = pd.read_excel(f"{self.output_dirs['bronze']}/variables_adjusted.xlsx", dtype=str)
    #     df_categories = pd.read_excel(f"{self.output_dirs['bronze']}/categories_adjusted.xlsx", dtype=str)

    #     for idx, row in df_tables.iterrows():
    #         table_number = row["id"]
    #         variaveis_filtradas = df_variables[df_variables["Tabela"] == table_number]

    #         pages_data: List[pd.DataFrame] = []
    #         pages_names: List[str] = []

    #         unique_categories = df_categories[df_categories["Tabela"] == table_number]['classificacao_id'].unique().tolist()
    #         unique_categories = [f'c{category}' for category in unique_categories if category is not None and category != '']
    #         categories_str = '/all/'.join(unique_categories) + '/all/' if unique_categories else ''
    #         assunto = self.format_string(row['assunto'])

    #         for _, row_var in variaveis_filtradas.iterrows():
    #             try:
    #                 self.sidra_api.build_url(
    #                     tabela=table_number,
    #                     variavel=row_var['id'],
    #                     classificacao=categories_str,
    #                     nivel_territorial=row["Nível Territorial"],
    #                     periodo={'Frequência': row["Frequência"], 
    #                              'Inicio': row["Data Inicial"], 
    #                              'Final': row["Data Final"]}
    #                 )

    #                 df = self.sidra_api.fetch_data()
    #                 self.process_dataframe(df, table_number, row_var, assunto)

    #                 pages_data.append(df)
    #                 pages_names.append(f'Variável {row_var["id"]}')

    #                 sleep(self.execution_interval)

    #             except Exception as e:
    #                 print(f"Um erro ocorreu na tabela {table_number}, variável {row_var['id']}: {e}")
    #                 sleep(10)

    #         if pages_data:
    #             with pd.ExcelWriter(f'{self.output_dirs.get("silver")}/Tabela {table_number}.xlsx', engine='openpyxl') as writer:
    #                 for df, nome_aba in zip(pages_data, pages_names):
    #                     df.to_excel(writer, sheet_name=nome_aba, index=False)
    #         else:
    #             print(f"Nenhum dado para escrever em Excel: {table_number}")