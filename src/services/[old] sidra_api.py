
import time
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

class old_sidra_manager():
    def __init__(self) -> None:
        pass

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
    