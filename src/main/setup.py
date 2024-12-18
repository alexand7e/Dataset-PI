# Standard library imports
import logging
import os
import re
import sys
import unicodedata
from time import sleep, time
from typing import List, Tuple, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

# Third-party imports
import pandas as pd
from tqdm import tqdm

# Local application/library specific imports
from src.services.sidra_api import SidraAPI
from src.services.ibge_api import SidraManager
from src.db.database_manager import PostgreSQL
from src.db.local_directory import DirectoryManager

def format_string(input_string: str) -> str:
    """
    Formata uma string, removendo acentos, caracteres especiais e substituindo espaços por underscores.

    Parâmetros:
        input_string (str): String a ser formatada.

    Retorna:
        str: String formatada.
    """
    normalized_string = unicodedata.normalize('NFKD', input_string)
    cleaned_string = ''.join(char for char in normalized_string if not unicodedata.combining(char))
    cleaned_string = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned_string)
    cleaned_string = cleaned_string.lower()
    cleaned_string = cleaned_string.replace(' ', '_').replace('-', '_')
    return cleaned_string

class SidraMetadataExecute:
    """
    Classe para gerenciar a extração e processamento de metadados de tabelas do SIDRA.

    Atributos:
        list_of_tables (Optional[List[int]]): Lista de IDs de tabelas a serem processadas.
        output_dir (str): Diretório onde os arquivos de saída serão salvos.
        processing_db (bool): Indica se os dados processados devem ser salvos em um banco de dados PostgreSQL.
        list_df_tables (List[pd.DataFrame]): Lista de DataFrames de tabelas processadas.
        list_df_variables (List[pd.DataFrame]): Lista de DataFrames de variáveis processadas.
        list_df_categories (List[pd.DataFrame]): Lista de DataFrames de categorias processadas.
        sidra_service (SidraManager): Serviço para gerenciar operações de metadados SIDRA.
        sidra_api (SidraAPI): API para interagir com o SIDRA.
        execution_interval (int): Intervalo de execução entre as tentativas de requisição.
        directory_manager (DirectoryManager): Gerenciador de diretórios.
        output_dirs (dict): Dicionário com os diretórios de saída.
        db (Optional[PostgreSQL]): Instância do banco de dados PostgreSQL (se habilitado).

    Métodos:
        __init__: Inicializa a classe com diretórios de saída, serviços SIDRA e configurações de banco de dados, se necessário.
        process_table_metadata: Processa metadados para uma tabela específica com tentativas de repetição em caso de falhas.
        _process_data: Processa dados de tabela, variáveis e categorias e armazena em listas internas.
        batch_info: Processa uma lista de tabelas e gera arquivos Excel com os metadados.
        _generate_excel_files: Salva um DataFrame em um arquivo Excel no diretório especificado.
        _load_data: Carrega dados de metadados de tabelas, variáveis e categorias de arquivos Excel.
        _build_and_fetch_data: Constrói uma URL para consulta e busca dados da API do SIDRA.
        _process_and_save_data: Processa e salva dados em arquivos Excel para cada tabela.
        batch_extraction: Executa a extração em lote de dados usando métodos definidos na classe.
        processed_template: Processa arquivos de dados e aplica um template para cada tabela.
    """

    def __init__(self, list_of_tables: Optional[List[int]] = None, output_dir: str = os.path.join(os.path.dirname(__file__), "..", "..", "data"), processing_db: bool = False) -> None:
        """
        Inicializa a classe com diretórios de saída, serviços SIDRA e configurações de banco de dados, se necessário.

        Parâmetros:
            list_of_tables (Optional[List[int]]): Lista de IDs de tabelas para processamento.
            output_dir (str): Caminho do diretório de saída para arquivos processados.
            processing_db (bool): Define se os resultados devem ser armazenados em um banco de dados.
        """
        self.list_of_tables = list_of_tables
        self.output_dir = output_dir
        self.processing_db = processing_db

        logging.basicConfig(level=logging.INFO)
        logging.info(f"Objeto SidraMetadataExecute criado \nDiretórios: {self.output_dir}")

        # Inicializa listas para armazenar os dados processados
        self.list_df_tables = []
        self.list_df_variables = []
        self.list_df_categories = []

        # Inicializa serviços e gerenciadores
        self.sidra_service = SidraManager()
        self.sidra_api = SidraAPI()
        self.execution_interval = 5

        self.directory_manager = DirectoryManager()
        self.output_dirs = self.directory_manager._create_directories()

        # Configura banco de dados se necessário
        if self.processing_db:
            self.db = PostgreSQL(schema='datasetpi')

    def process_table_metadata(self, table: int, max_retries: int = 2) -> Tuple[Optional[pd.DataFrame], int]:
        """
        Tenta recuperar e processar os metadados de uma tabela do SIDRA, com tentativas de repetição especificadas.

        Parâmetros:
            table (int): ID da tabela a ser processada.
            max_retries (int): Número máximo de tentativas em caso de falha.

        Retorna:
            Tuple[Optional[pd.DataFrame], int]: DataFrame com os metadados da tabela e contagem de tentativas.
        """
        retry_count = 0
        while retry_count < max_retries:
            try:
                data = self.sidra_service.sidra_get_metadata(table)
                sleep(self.execution_interval)
                if data:
                    df = self._process_data(table, data)
                    return df, retry_count
                else:
                    logging.error(f"Falha ao obter os dados de {table}")
                    retry_count += 1
            except Exception as e:
                logging.error(f"Erro ao processar os dados de {table}: {e}")
                retry_count += 1
            sleep(self.execution_interval)
        return None, retry_count 

    def _process_data(self, table: int, data: dict) -> pd.DataFrame:
        """
        Processa dados de tabela, variáveis e categorias e armazena em listas internas.

        Parâmetros:
            table (int): ID da tabela a ser processada.
            data (dict): Dados da tabela obtidos da SIDRA.

        Retorna:
            pd.DataFrame: DataFrame com informações processadas da tabela.
        """
        df_tables, df_table_info = self.sidra_service.sidra_process_table(data)
        df_variables = self.sidra_service.sidra_process_variables(data, table)
        df_categories = self.sidra_service.sidra_process_categories(data, table)

        self.list_df_tables.append(df_tables)
        self.list_df_variables.append(df_variables)
        self.list_df_categories.append(df_categories)

        return df_table_info

    def batch_info(self, max_retries: int = 3) -> Tuple[List[dict], dict]:
        """
        Processa uma lista de tabelas e gera arquivos Excel com os metadados.

        Parâmetros:
            max_retries (int): Número máximo de tentativas em caso de falha ao processar uma tabela.

        Retorna:
            Tuple[List[dict], dict]: Lista de metadados das tabelas processadas e dicionário com contagem de tentativas falhadas.
        """
        metatable = []
        failed_requests = {}

        for table in tqdm(self.list_of_tables, total=len(self.list_of_tables), unit="Tables"):
            table_info, retries = self.process_table_metadata(table, max_retries)
            if table_info is not None:
                metatable.append({"tabela": table, "dados": table_info})
                self._generate_excel_files(table_info, f"sidra_info_{table}.xlsx")
            else:
                failed_requests[table] = retries

        final_df_tables = pd.concat(self.list_df_tables, ignore_index=True)
        final_df_variables = pd.concat(self.list_df_variables, ignore_index=True)
        final_df_categories = pd.concat(self.list_df_categories, ignore_index=True)

        # Geração de arquivos consolidados
        self._generate_excel_files(final_df_tables, "_tables_adjusted_.xlsx", pasta="bronze")
        self._generate_excel_files(final_df_variables, "_variables_adjusted_.xlsx", pasta="bronze")
        self._generate_excel_files(final_df_categories, "_categories_adjusted_.xlsx", pasta="bronze")

        return metatable, failed_requests

    def _generate_excel_files(self, df: pd.DataFrame, filename: str, pasta: str = 'bronze') -> None:
        """
        Salva um DataFrame em um arquivo Excel no diretório especificado.

        Parâmetros:
            df (pd.DataFrame): DataFrame a ser salvo.
            filename (str): Nome do arquivo Excel.
            pasta (str): Diretório onde o arquivo será salvo.
        """
        df.to_excel(os.path.join(self.output_dirs.get(pasta), filename), index=False)

    def _load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Carrega dados de metadados de tabelas, variáveis e categorias de arquivos Excel.

        Retorna:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: DataFrames de tabelas, variáveis e categorias.
        """
        df_tables = pd.read_excel(os.path.join(self.output_dirs.get("bronze"), "_tables_adjusted_.xlsx"), dtype=str)
        df_variables = pd.read_excel(os.path.join(self.output_dirs.get("bronze"), "_variables_adjusted_.xlsx"), dtype=str)
        df_categories = pd.read_excel(os.path.join(self.output_dirs.get("bronze"), "_categories_adjusted_.xlsx"), dtype=str)

        return df_tables, df_variables, df_categories
    
    def _build_and_fetch_data(self, table_number: int, row: pd.Series, row_var: pd.Series, categories_str: str) -> pd.DataFrame:
        """
        Constrói uma URL para consulta e busca dados da API do SIDRA.

        Parâmetros:
            table_number (int): ID da tabela a ser processada.
            row (pd.Series): Linha do DataFrame de tabelas.
            row_var (pd.Series): Linha do DataFrame de variáveis.
            categories_str (str): String formatada de categorias.

        Retorna:
            pd.DataFrame: DataFrame com os dados obtidos da API.
        """
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
        return df

    def _process_and_save_data(self, pages_data: List[pd.DataFrame], pages_names: List[str], table_number: int) -> None:
        """
        Processa e salva dados em arquivos Excel para cada tabela.

        Parâmetros:
            pages_data (List[pd.DataFrame]): Lista de DataFrames de dados processados.
            pages_names (List[str]): Lista de nomes das abas do Excel.
            table_number (int): ID da tabela a ser salva.
        """
        if pages_data:
            output_file = f'{self.output_dirs.get("silver")}/Tabela {table_number}.xlsx'
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                for df, nome_aba in zip(pages_data, pages_names):
                    df.to_excel(writer, sheet_name=nome_aba, index=False)
        else:
            logging.warning(f"Nenhum dado para escrever em Excel: {table_number}")

    def batch_extraction(self) -> None:
        """
        Executa a extração em lote de dados usando métodos definidos na classe.
        """
        df_tables, df_variables, df_categories = self._load_data()

        for idx, row in df_tables.iterrows():
            table_number = row["id"]
            variaveis_filtradas = df_variables[df_variables["Tabela"] == table_number]

            pages_data: List[pd.DataFrame] = []
            pages_names: List[str] = []

            unique_categories = df_categories[df_categories["Tabela"] == table_number]['classificacao_id'].unique().tolist()
            unique_categories = [f'c{category}' for category in unique_categories if category is not None and category != '']
            categories_str = '/all/'.join(unique_categories) + '/all/' if unique_categories else ''
            assunto = format_string(row['assunto'])

            for _, row_var in variaveis_filtradas.iterrows():
                try:
                    df = self._build_and_fetch_data(table_number, row, row_var, categories_str)
                    pages_data.append(df)
                    pages_names.append(f'Variável {row_var["id"]}')
                    sleep(self.execution_interval)
                except Exception as e:
                    logging.error(f"Um erro ocorreu na tabela {table_number}, variável {row_var['id']}: {e}")
                    sleep(10)

            self._process_and_save_data(pages_data, pages_names, table_number)

    def processed_template(self) -> None:
        """
        Processa arquivos de dados e aplica um template para cada tabela.

        Este método executa as seguintes etapas:
        1. Inicializa um objeto `DirectoryManager` para gerenciar os diretórios de origem e destino.
        2. Lista todos os arquivos no diretório de origem e extrai os números das tabelas dos nomes dos arquivos.
        3. Para cada arquivo de tabela:
            a. Lê o arquivo Excel.
            b. Verifica se o arquivo de saída correspondente já existe.
            c. Aplica o template e processa o arquivo.
            d. Registra a conclusão do processamento.

        Exceções são tratadas e registradas em logs.
        """
        try:
            dm = DirectoryManager(origin_directory=self.output_dirs.get('bronze'), 
                                  destiny_directory=self.output_dirs.get('gold'))
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
            logging.error(f"Erro ao processar os arquivos de dados: {e}")
