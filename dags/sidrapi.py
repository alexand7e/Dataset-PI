from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import locale

import requests
from requests.exceptions import (HTTPError, ConnectionError, Timeout, TooManyRedirects)

# Configura o locale para o formato brasileiro
# locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

class Sidra_Data:
    """
    Explicação
    """

    def __init__(self) -> None:
        pass


    # def sidra_info(self, número_tabela: int):
    #     """
    #     Descrição
    #     """
        
    #     url = f'https://apisidra.ibge.gov.br/desctabapi.aspx?c={número_tabela}'

    #     try:
    #         response = requests.get(url)
    #         html_content = response.text
    #         soup = BeautifulSoup(html_content, 'html.parser')
    #         content = soup.find('div', {'id': 'pnlConteudo'})
    #     except requests.exceptions.Timeout as to:
    #         print(f"Erro ao obter os dados: {to}")
    #     except requests.exceptions.TooManyRedirects as tm:
    #         print(f"Erro ao obter os dados: {tm}")
    #     except requests.exceptions.RequestException as e:
    #         raise SystemExit(e)

    #     classificador = self.get_class_code(html_content)
        
    #     # Pesquisa
    #     df = pd.DataFrame({
    #     'Nome da Tabela': [content.find('span', {'id': 'lblNomeTabela'}).get_text(strip=True)],
    #     'Tipo do Período': [content.find('span', {'id': 'lblNomePeriodo'}).get_text(strip=True)],
    #     'Classificador da Tabela': [classificador],
    #     'Períodos Disponíveis':  [content.find('span', {'id': 'lblPeriodoDisponibilidade'}).get_text(strip=True)],
    #     'Última Atualização':  [content.find('span', {'id': 'lblDataAtualizacao'}).get_text(strip=True)],
    #     'Nome da Pesquisa':  [content.find('span', {'id': 'lblNomePesquisa'}).get_text(strip=True)],
    #     'Assunto':  [content.find('span', {'id': 'lblNomeAssunto'}).get_text(strip=True)],
    #     'Fonte':  [content.find('span', {'id': 'lblFonte'}).get_text(strip=True)],
    #     'Nota':  [content.find('span', {'id': 'lblTextoDescricao'}).get_text(strip=True)]}
    #     ) if content is not None else pd.DataFrame()

    #     print(f"Tabela {número_tabela} - ok!")
    #     time.sleep(5)

    #     return html_content, df
    

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
            df = pd.DataFrame({
                'Nome da Tabela': [content.find('span', {'id': 'lblNomeTabela'}).get_text(strip=True)],
                'Tipo do Período': [content.find('span', {'id': 'lblNomePeriodo'}).get_text(strip=True)],
                'Classificador da Tabela': [classificador],
                'Períodos Disponíveis':  [content.find('span', {'id': 'lblPeriodoDisponibilidade'}).get_text(strip=True)],
                'Última Atualização':  [content.find('span', {'id': 'lblDataAtualizacao'}).get_text(strip=True)],
                'Nome da Pesquisa':  [content.find('span', {'id': 'lblNomePesquisa'}).get_text(strip=True)],
                'Assunto':  [content.find('span', {'id': 'lblNomeAssunto'}).get_text(strip=True)],
                'Fonte':  [content.find('span', {'id': 'lblFonte'}).get_text(strip=True)],
                'Nota':  [content.find('span', {'id': 'lblTextoDescricao'}).get_text(strip=True)]
            })
        else:
            df = pd.DataFrame()  # DataFrame vazio

        print(f"Tabela {número_tabela} - ok!")
        time.sleep(5)

        return html_content, df
  

    def get_class_code(self, html_content):
        # Parsear o HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Encontrar o span com o ID específico para a classificação
        span_classificacao = soup.find('span', {'id': lambda x: x and x.startswith('lstClassificacoes_lblIdClassificacao_')})

        if span_classificacao:
            return "C" + span_classificacao.text.strip()
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


    def fetch_sidra_data(self, t, v, c, p='p/all', n = 'n3/22', f='f/n', d='d/4', h='h/y') -> pd.DataFrame:
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

        url_base = 'https://apisidra.ibge.gov.br/values'

        url = f"{url_base}/t/{t}/{n}/v/{v}/{p}/{c}/all/{f}/{d}/{h}"

        if pd.isna(c) or c == "" or c is None:
            url = f"{url_base}/t/{t}/{n}/v/{v}/{p}/{f}/{d}/{h}"

        try:
            response = requests.get(url)
            response.raise_for_status()  # unsuccessful status code
            data = response.json()

            df = pd.DataFrame(data)
            df.columns = df.iloc[0]
            df = df[1:]
 
            # Identificar colunas que contêm "(Código)" no nome
            colunas_para_remover = [coluna for coluna in df.columns if "(Código)" in coluna]
            df_final = df.drop(columns=colunas_para_remover, inplace=True)

            df_final['Valor'] = pd.to_numeric(df_final['Valor'], errors='coerce')
            df_final['Valor'] = df_final['Valor'].astype(float).map(lambda x: locale.format_string('%.2f', x, grouping=True))

            return df_final
        
        except HTTPError as http_err:
            print(f"Erro HTTP ocorreu: {http_err}")
        except ConnectionError as conn_err:
            print(f"Erro de conexão ocorreu: {conn_err}")
        except Timeout as timeout_err:
            print(f"O tempo da requisição excedeu o tempo limite: {timeout_err}")
        except TooManyRedirects as redirects_err:
            print(f"Requisição excedeu o número máximo de redirecionamentos: {redirects_err}")
        except Exception as e:
            print(f"Um erro inesperado ocorreu: {e}")
            return None

