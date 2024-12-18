# Standard library imports
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from typing import List, Tuple

# Local application/library specific imports
from src.db.local_directory import DirectoryManager
from src.db.remote_directory import GoogleDriveManager
from src.main.setup import SidraMetadataExecute
from src.db.database_manager import PostgreSQL

import logging
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

        self.conecting_db = conecting_db
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
        # sidra_executor.batch_info()
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


        if self.conecting_db:
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
        if create_remote_directory:
            self.gd = GoogleDriveManager(os.path.join(self.output_dirs.get('geral'), 'credentials.json'))
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


