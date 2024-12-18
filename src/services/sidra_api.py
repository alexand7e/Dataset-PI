# Bibliotecas padrão
import time
import locale
import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

# Bibliotecas de terceiros
from src.utils.utils import GeradorDePeriodos
import pandas as pd
import requests
from requests.exceptions import (
    HTTPError, 
    ConnectionError, 
    Timeout, 
    TooManyRedirects
)

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SidraAPI:
    """
    Classe para gerenciar requisições e processamento de dados da API SIDRA do IBGE.
    
    Atributos:
    ----------
    get_p : GeradorDePeriodos
        Instância da classe responsável por gerar períodos para as requisições.
    urls : list
        Lista de URLs geradas para fazer as requisições à API SIDRA.
    """
    
    def __init__(self):
        """
        Inicializa a classe SidraAPI com um gerador de períodos.
        """
        self.get_p = GeradorDePeriodos()
        logging.info('Objeto SidraAPI criado com sucesso')

    def _ajustar_nivel_territorial(self):
        """
        Ajusta o nível territorial com base no mapeamento fornecido.
        
        Retorna:
        --------
        list
            Lista de partes ajustadas para o nível territorial.
        """
        n_map = {
            "N1": "N1/1",
            "N2": "N2/2",
            "N3": "N3/22",
            "N6": "N6/2200053,2211704"
        }

        parts = self.nivel_territorial.split(", ")
        parts_ajuste = [n_map[part] for part in parts if part in n_map]

        return parts_ajuste
    
    def build_url(self, 
                  tabela: str, 
                  variavel: str, 
                  classificacao: str = None, 
                  nivel_territorial: str = None, 
                  periodo: dict = {'Frequência': None, 'Inicio': None, 'Final': None}, 
                  formato: str = 'f/n', 
                  decimais: str = 'd/4', 
                  cabecalho: str = 'h/y', 
                  api: str = None):
        """
        Constrói as URLs para as requisições à API SIDRA.

        Parâmetros:
        -----------
        tabela : str
            Número da tabela do SIDRA.
        variavel : str
            Variável que será solicitada.
        classificacao : str, opcional
            Classificação dos dados.
        nivel_territorial : str, opcional
            Nível territorial para a solicitação.
        periodo : dict, opcional
            Dicionário com a frequência, início e fim do período.
        formato : str, opcional
            Formato da resposta (padrão é 'f/n').
        decimais : str, opcional
            Número de casas decimais (padrão é 'd/4').
        cabecalho : str, opcional
            Inclusão ou não do cabeçalho (padrão é 'h/y').
        api : str, opcional
            URL da API para sobrescrever a construção.

        Retorna:
        --------
        None
        """
        self.tabela = tabela
        self.variavel = variavel
        self.classificacao = classificacao
        self.nivel_territorial = nivel_territorial
        self.periodo = periodo 
        self.formato = formato
        self.decimais = decimais
        self.cabecalho = cabecalho

        n_adjust_list = self._ajustar_nivel_territorial()
        p_adjust_list = self.get_p.obter_periodo(
            self.periodo.get('Frequência'),
            self.periodo.get('Inicio'),
            self.periodo.get('Final'),
        )

        url_base = 'https://apisidra.ibge.gov.br/values'
        urls = []

        if not api:
            for n_adjust in n_adjust_list:
                for p_adjust in p_adjust_list:
                    if pd.isna(self.classificacao) or self.classificacao == "" or self.classificacao is None:
                        url = f"{url_base}/t/{self.tabela}/{n_adjust}/v/{self.variavel}/{p_adjust}/{self.formato}/{self.decimais}/{self.cabecalho}"
                    else:
                        url = f"{url_base}/t/{self.tabela}/{n_adjust}/v/{self.variavel}/{p_adjust}/{self.classificacao}/{self.formato}/{self.decimais}/{self.cabecalho}"
                    urls.append(url)
        else:
            urls.append(api)

        self.urls = urls
        logging.info(f'URLs construídas com sucesso: {len(self.urls)} URL(s) gerada(s)')
    
    def fetch_data(self, timeout=30, max_retries=2):
        """
        Faz requisições às URLs geradas e obtém os dados em formato JSON.
        
        Parâmetros:
        -----------
        timeout : int, opcional
            Tempo de espera máximo para a requisição (padrão é 30 segundos).
        max_retries : int, opcional
            Número máximo de tentativas de requisição (padrão é 2).
        
        Retorna:
        --------
        pd.DataFrame
            DataFrame com os dados coletados.
        """
        results = []
        logging.info(f'Processando a Tabela {self.tabela} | Variável {self.variavel} | Total de URLs: {len(self.urls)}')

        for url in self.urls:
            attempt = 0
            while attempt < max_retries:
                try:
                    response = requests.get(url, timeout=timeout)
                    response.raise_for_status()
                    response_df = self.format_data(response.json())
                    results.append(response_df)
                    break  # Se a requisição for bem-sucedida, sai do loop
                except (HTTPError, ConnectionError, Timeout, TooManyRedirects) as e:
                    logging.warning(f"Tentativa {attempt + 1}: Erro ao buscar dados da URL {url}: {e}")
                    attempt += 1
                    time.sleep(5)
                except Exception as e:
                    logging.error(f"Erro inesperado ao buscar dados da URL {url}: {e}")
                    break

        if results:
            final_df = pd.concat(results, ignore_index=True)
            logging.info("Dados concatenados com sucesso.")
        else:
            final_df = pd.DataFrame()
            logging.warning("Nenhum dado foi retornado das requisições.")

        return final_df
    
    def format_data(self, data):
        """
        Formata os dados recebidos da API em um DataFrame.

        Parâmetros:
        -----------
        data : dict
            Dados em formato JSON recebidos da API.
        
        Retorna:
        --------
        pd.DataFrame
            DataFrame formatado.
        """
        if not data:
            logging.warning("Nenhum dado disponível para formatar.")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        rename_map = {
            'Grande Região': 'Região',
            'Município': 'Região',
            'Unidade da Federação': 'Região',
            'Brasil': 'Região',
            'Ano': 'Período',
            'Trimestre': 'Período',
            'Mês': 'Período'
        }

        df.columns = df.iloc[0]
        df = df.rename(columns=rename_map)
        df = df[1:]  # Remove a primeira linha que contém o cabeçalho original
        columns_to_remove = [column for column in df.columns if "(Código)" in column]
        df = df.drop(columns=columns_to_remove)
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')  # Ajusta a localidade conforme necessário
        df['Valor'] = df['Valor'].apply(lambda x: locale.format_string('%.2f', x, grouping=True))

        if df.columns[-1] != 'Categorias':
            df.columns = [*df.columns[:-1], 'Categorias']

        logging.info("Dados formatados com sucesso.")
        return df
