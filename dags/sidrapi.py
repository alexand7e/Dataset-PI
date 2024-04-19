from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import locale

import requests
from requests.exceptions import (HTTPError, 
                                 ConnectionError, 
                                 Timeout, 
                                 TooManyRedirects)

# Configura o locale para o formato brasileiro
# locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

class SidraManager:
    """
    Documentação
    """
    def __init__(self, uf_code: int = 22) -> None:
        self.uf_ref = uf_code

        # atributos 
        self.TAB_COLUMN_NAME = "tabela"
        self.VAR_COLUMN_NAME = "Número da Variável"
        self.CLASS_COLUMN_NAME = "Classificador da Tabela"
        self.SPATIAL_COLUMN_NAME = "recorte_espacial"
        self.TABLE_INDEX = 0
        self.failed_requests = []  # Lista para armazenar tentativas falhas

    def sidra_get_metadata(self, número_tabela: int):
        """
        Obtém os metadados de uma tabela específica do SIDRA via API do IBGE.

        Parâmetros:
        número_tabela (int): Número da tabela para a qual os metadados são solicitados.

        Retorna:
        dict: Um dicionário contendo os metadados da tabela ou None em caso de falha.
        """
        url = f"https://servicodados.ibge.gov.br/api/v3/agregados/{número_tabela}/metadados"

        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as re:
            print(f"Erro ao obter dados da tabela {número_tabela}: {re}")
            self.failed_requests.append(número_tabela)
            return None

    def retry_failed_requests(self, delay_seconds=5):
        """
        Tenta novamente buscar os metadados para as tabelas que falharam na primeira tentativa.

        Parâmetros:
        delay_seconds (int): Tempo de espera entre as tentativas, em segundos.
        """
        if not self.failed_requests:
            print("Não há falhas para retry.")
            return

        print("Retentando as falhas...")
        time.sleep(delay_seconds)  # Espera por um tempo especificado antes de tentar novamente

        retry_list = self.failed_requests[:]
        self.failed_requests = []  # Limpa a lista de falhas para novas adições nesta tentativa

        for número_tabela in retry_list:
            print(f"Tentando novamente a tabela {número_tabela}")
            self.sidra_get_metadata(número_tabela)

        if self.failed_requests:
            print("Algumas tabelas ainda falharam após retentativas.")
        else:
            print("Todas as tabelas foram retentadas com sucesso.")
  
        
    def sidra_process_variables(self, dados, request_id):
        try:
            df_variaveis = pd.DataFrame(dados['variaveis'])
            df_variaveis['Tabela'] = request_id
            return df_variaveis
        except Exception as e:
            print(f"Erro ao criar o DataFrame de variáveis: {e}")
            return pd.DataFrame()

    def sidra_process_categories(self, dados, request_id):
        try:
            # Criando o DataFrame de classificações
            df_classificacoes = pd.DataFrame(dados['classificacoes'])
            
            # Expandindo cada classificação com suas categorias
            all_categories = []
            for classification in dados['classificacoes']:
                for categoria in classification['categorias']:
                    categoria['classificacao_nome'] = classification['nome']
                    categoria['classificacao_id'] = classification['id']
                    categoria['Tabela'] = request_id
                    all_categories.append(categoria)

            # Criando DataFrame de categorias com informações de classificação
            df_categorias = pd.DataFrame(all_categories)

            # Unindo os DataFrames se necessário ou retornando apenas df_categorias
            return df_categorias
        except Exception as e:
            print(f"Erro ao processar dados combinados: {e}")
            return pd.DataFrame()

    def sidra_info(self, número_tabela: int):
        """
        Descrição
        """
        url = f'https://apisidra.ibge.gov.br/desctabapi.aspx?c={número_tabela}'

        try:
            response = requests.get(url)
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            content = soup.find('div', {'id': 'pnlConteudo'})
        except requests.exceptions.Timeout as to:
            print(f"Erro ao obter os dados: {to}")
        except requests.exceptions.TooManyRedirects as tm:
            print(f"Erro ao obter os dados: {tm}")
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        if content is not None:
            classificador = self.get_class_code(html_content)
            territorias = self.get_territorial_code(html_content)
            df = pd.DataFrame({
                'Nome da Tabela': [content.find('span', {'id': 'lblNomeTabela'}).get_text(strip=True)],
                'Tipo do Período': [content.find('span', {'id': 'lblNomePeriodo'}).get_text(strip=True)],
                'Classificador da Tabela': [classificador],
                'Níveis Territoriais': [territorias],
                'Períodos Disponíveis':  [content.find('span', {'id': 'lblPeriodoDisponibilidade'}).get_text(strip=True)],
                'Última Atualização':  [content.find('span', {'id': 'lblDataAtualizacao'}).get_text(strip=True)],
                'Nome da Pesquisa':  [content.find('span', {'id': 'lblNomePesquisa'}).get_text(strip=True)],
                'Assunto':  [content.find('span', {'id': 'lblNomeAssunto'}).get_text(strip=True)],
                'Fonte':  [content.find('span', {'id': 'lblFonte'}).get_text(strip=True)],
                'Nota':  [content.find('span', {'id': 'lblTextoDescricao'}).get_text(strip=True)]
            })
        else:
            df = pd.DataFrame()  # DataFrame vazio

        # print(f"Tabela {número_tabela} - ok!")
        time.sleep(5)

        return html_content, df
  

    def get_class_code(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')

        # Encontrar o span com o ID específico para a classificação
        span_classificacao = soup.find('span', {'id': lambda x: x and x.startswith('lstClassificacoes_lblIdClassificacao_')})

        if span_classificacao:
            return "C" + span_classificacao.text.strip()
        else:
            return np.nan
        
        
    def get_territorial_code(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')

        span_terr_level = soup.find_all('span', id=lambda x: x and x.startswith('lstNiveisTerritoriais_lblIdNivelterritorial_'))

        if span_terr_level:
            return ', '.join(['N' + span.text for span in span_terr_level])
        else:
            return np.nan

    def sidra_get_vars(self, html_content):
        # Parsear o HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        tables = soup.find_all('table')
        dfs = []

        for table in tables:
            rows = table.find_all('tr')
            table_data = []

            for row in rows:
                row_text = ''.join(cell.text.strip() for cell in row.find_all('td'))
                cols_text = row_text.split(u'\xa0\xa0')
                table_data.append(cols_text)

            df = pd.DataFrame(table_data)
            dfs.append(df)

        if len(dfs) >= 2:
            df_variables = dfs[1].rename(columns={0: 'Número da Variável', 1: 'Descrição'})
        else:
            return None, None
        
        # Tratar a terceira tabela, se existir; caso contrário, usar "all"
        df_groups = pd.DataFrame({'Cód Grupo': [np.nan], 'Grupo': ["Sem categorias"]}) if len(dfs) <= 3 else dfs[2].iloc[1:, 0:2].rename(columns={0: 'Cód Grupo', 1: 'Grupo'})

        return df_variables, df_groups


    def fetch_sidra_data(self, t, v, c, n, p, f='f/n', d='d/4', h='h/y') -> pd.DataFrame:
        """
        Parâmetros: 
        * t/ – para especificar o código da tabela de onde se deseja extrair os dados.
        * p/ – para especificar os períodos (meses, anos etc.) desejados.
        * v/ – para especificar as variáveis desejadas.
        * n<nível territorial> – para especificar os níveis territoriais e suas unidades territoriais desejadas.
        * c<classificação> – para especificar as classificações da tabela e suas categorias desejadas.
        * f – para especificar a formatação do resultado, i.e., que tipo de descritor de cada uma das dimensões da tabela comporá o resultado recebido.
        * d – para especificar com quantas casas decimais serão formatados os valores.
        * h – para especificar se o resultado recebido será precedido por um registro de cabeçalho (header) (n/y)
        """
        def get_n(n: str):
            parts = []

            if "N1" in n:
                parts.append('n1/1')
            if "N2" in n:
                parts.append('n2/all')
            if "N3" in n:
                parts.append('n3/22')
            
            if parts:
                return '/' + '/'.join(parts)
            else:
                return '/n1/1'
            
        def get_p(p: str):
            if p == "Ano":
                return '/p/2012-2024'
            elif p == "Mês":
                return '/p/201201-202403'    
            else:
                return '/p/201201-202401'

        n_adjust = get_n(n=n)
        p_adjust = get_p(p=p)

        url_base = 'https://apisidra.ibge.gov.br/values'

        url = f"{url_base}/t/{t}/{n_adjust}/v/{v}/{p_adjust}/{c}/all/{f}/{d}/{h}"

        if pd.isna(c) or c == "" or c is None:
            url = f"{url_base}/t/{t}/{n_adjust}/v/{v}/{p_adjust}/{f}/{d}/{h}"

        try:
            print(f"Requesting {url}...")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            df.columns = df.iloc[0]
            df = df[1:]

            # Identify columns containing "(Code)" in the name
            columns_to_remove = [column for column in df.columns if "(Código)" in column]
            df = df.drop(columns=columns_to_remove)

            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
            locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')  # Set to your locale
            df['Valor'] = df['Valor'].apply(lambda x: locale.format_string('%.2f', x, grouping=True))

            return df    
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")
        except Timeout as timeout_err:
            print(f"Request timed out: {timeout_err}")
        except TooManyRedirects as redirects_err:
            print(f"Too many redirects: {redirects_err}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return pd.DataFrame()


