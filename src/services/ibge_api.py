import time
import logging
import requests
import pandas as pd

class SidraManager:
    """
    Gerencia as interações com a API do SIDRA do IBGE para obtenção e processamento de dados.
    
    Atributos:
    ----------
    uf_ref : int
        Código da unidade federativa (UF) de referência. Valor padrão é 22 (Piauí).
    TABLE_INDEX : int
        Índice da tabela para controle de processamento.
    failed_requests : list
        Lista que armazena números de tabelas com falhas nas requisições.
    BASE_URL : str
        URL base para as requisições à API do IBGE.
    """
    
    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

    def __init__(self, uf_code: int = 22) -> None:
        """
        Inicializa a instância da classe SidraManager.
        
        Parâmetros:
        -----------
        uf_code : int, opcional
            Código da UF de referência, padrão é 22 (Piauí).
        """
        self.uf_ref = uf_code
        self.TABLE_INDEX = 0
        self.failed_requests = []  # Lista para armazenar tentativas falhas

        # Configurando logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def sidra_get_metadata(self, numero_tabela: int) -> dict:
        """
        Obtém os metadados de uma tabela específica do SIDRA via API do IBGE.
        
        Parâmetros:
        -----------
        numero_tabela : int
            Número da tabela para a qual os metadados são solicitados.
        
        Retorna:
        --------
        dict
            Um dicionário contendo os metadados da tabela ou None em caso de falha.
        """
        url = f"{self.BASE_URL}/{numero_tabela}/metadados"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            logging.info(f"Dados da tabela {numero_tabela} obtidos com sucesso.")
            return response.json()
        except requests.exceptions.RequestException as re:
            logging.error(f"Erro ao obter dados da tabela {numero_tabela}: {re}")
            self.failed_requests.append(numero_tabela)
            return None
    
    def retry_failed_requests(self, delay_seconds=5):
        """
        Tenta novamente buscar os metadados para as tabelas que falharam na primeira tentativa.
        
        Parâmetros:
        -----------
        delay_seconds : int, opcional
            Tempo de espera entre as tentativas, em segundos. Padrão é 5 segundos.
        """
        if not self.failed_requests:
            logging.info("Não há falhas para retry.")
            return
        
        logging.info("Retentando as falhas...")
        time.sleep(delay_seconds)
        
        retry_list = self.failed_requests[:]
        self.failed_requests = []  # Limpa a lista de falhas para novas adições nesta tentativa
        
        for numero_tabela in retry_list:
            logging.info(f"Tentando novamente a tabela {numero_tabela}")
            self.sidra_get_metadata(numero_tabela)
        
        if self.failed_requests:
            logging.warning("Algumas tabelas ainda falharam após retentativas.")
        else:
            logging.info("Todas as tabelas foram retentadas com sucesso.")
    
    def sidra_process_table(self, dados: dict):
        """
        Processa e estrutura os dados de uma tabela do SIDRA em um DataFrame.

        Parâmetros:
        -----------
        dados : dict
            Dicionário com os dados da tabela SIDRA.

        Retorna:
        --------
        tuple
            Dois DataFrames contendo as informações gerais e detalhadas da tabela.
        """
        try:
            info_geral = {k: v for k, v in dados.items() if k not in ['variaveis', 'classificacoes']}
            info_geral['Frequência'] = dados['periodicidade']['frequencia']
            info_geral['Data Inicial'] = dados['periodicidade']['inicio']
            info_geral['Data Final'] = dados['periodicidade']['fim']
            info_geral['Nível Territorial'] = ', '.join(dados['nivelTerritorial']['Administrativo'])

            df_sidra_table = pd.DataFrame([info_geral])

            info_geral_transposed = [(k, v) for k, v in info_geral.items()]
            info_geral_transposed.append(("", ""))
            info_geral_transposed.append(("Variáveis:", "Unidades:"))

            for var in dados['variaveis']:
                info_geral_transposed.append((var['nome'], f"{var['unidade']} (Sumarização: {', '.join(var['sumarizacao'])})"))

            df_sidra_table_to_excel = pd.DataFrame(info_geral_transposed, columns=['Campo', 'Informação'])
            logging.info("Dados da tabela processados com sucesso.")
            return df_sidra_table, df_sidra_table_to_excel
        except Exception as e:
            logging.error(f"Erro ao processar os dados da tabela: {e}")
            return pd.DataFrame(), pd.DataFrame()

    def sidra_process_variables(self, dados: dict, request_id: int) -> pd.DataFrame:
        """
        Processa e estrutura as variáveis de uma tabela do SIDRA em um DataFrame.

        Parâmetros:
        -----------
        dados : dict
            Dicionário com os dados da tabela SIDRA.
        request_id : int
            ID da requisição da tabela.

        Retorna:
        --------
        pd.DataFrame
            DataFrame contendo as variáveis da tabela.
        """
        try:
            df_variaveis = pd.DataFrame(dados['variaveis'])
            df_variaveis['Tabela'] = request_id
            logging.info(f"Variáveis da tabela {request_id} processadas com sucesso.")
            return df_variaveis
        except Exception as e:
            logging.error(f"Erro ao processar variáveis da tabela {request_id}: {e}")
            return pd.DataFrame()

    def sidra_process_categories(self, dados: dict, request_id: int) -> pd.DataFrame:
        """
        Processa e estrutura as categorias de uma tabela do SIDRA em um DataFrame.

        Parâmetros:
        -----------
        dados : dict
            Dicionário com os dados da tabela SIDRA.
        request_id : int
            ID da requisição da tabela.

        Retorna:
        --------
        pd.DataFrame
            DataFrame contendo as categorias da tabela.
        """
        try:
            all_categories = []
            for classification in dados['classificacoes']:
                for categoria in classification['categorias']:
                    categoria['classificacao_nome'] = classification['nome']
                    categoria['classificacao_id'] = classification['id']
                    categoria['Tabela'] = request_id
                    all_categories.append(categoria)

            df_categorias = pd.DataFrame(all_categories)
            logging.info(f"Categorias da tabela {request_id} processadas com sucesso.")
            return df_categorias
        except Exception as e:
            logging.error(f"Erro ao processar categorias da tabela {request_id}: {e}")
            return pd.DataFrame()
