import os

data_path = os.path.join(os.path.dirname(__file__), "..", "data")

from sidrapi import SidraAPI, SidraManager, DataFrameFormatter

import pandas as pd
import json
from tqdm import tqdm
from time import sleep

# Configuração das pastas de destino
output_dirs = {
    'gold': f'{data_path}/gold',
    'silver': f'{data_path}/silver',
    'bronze': f'{data_path}/bronze'
}


def bath_info(tables_list: list, sidra_service: SidraManager):
    """"""
    lista_df_tabelas = pd.DataFrame()
    lista_df_variaveis = pd.DataFrame()
    lista_df_categorias = pd.DataFrame()

    for tabela in tqdm(tables_list, total=len(tables_list), unit="Tabelas"):
        dados = sidra_service.sidra_get_metadata(tabela)
        sleep(1)
        if dados:
            df_tabelas = sidra_service.sidra_process_table(dados)[1] # primeiro elemento da tupla
            df_variaveis = sidra_service.sidra_process_variables(dados, tabela)
            df_categorias = sidra_service.sidra_process_categories(dados, tabela)

            # Adicionando os DataFrames às respectivas listas
            lista_df_tabelas = pd.concat([lista_df_tabelas, df_tabelas], ignore_index=True)
            lista_df_variaveis = pd.concat([lista_df_variaveis, df_variaveis], ignore_index=True)    # lista_df_variaveis.append(df_variaveis)
            lista_df_categorias = pd.concat([lista_df_categorias, df_categorias], ignore_index=True)    # lista_df_categorias.append(df_categorias)

        else:
            print(f"Falha ao obter dados de {tabela}")

    lista_df_tabelas.to_excel(f"{output_dirs['bronze']}/tabelas_ajustadas.xlsx", index=False)
    lista_df_variaveis.to_excel(f"{output_dirs['bronze']}/variaveis_ajustadas.xlsx", index=False)
    lista_df_categorias.to_excel(f"{output_dirs['bronze']}/categorias_ajustadas.xlsx", index=False)
    sidra_service.retry_failed_requests()    


def batch_extraction(df_tabelas, df_variaveis, sidra_api: SidraAPI, sidra_service: SidraManager):

    for idx, row in df_tabelas.iterrows():
        table_number = row[sidra_service.TAB_COLUMN_NAME]
        tb_var = df_variaveis[df_variaveis[sidra_service.TAB_COLUMN_NAME] == table_number]

        # dados paras os arquivos excel
        pages_data = []
        pages_names = []

        for i, row_var in tb_var.iterrows():
            try:
                sidra_api_execute = sidra_api(t=table_number,
                                                v=row_var[sidra_service.VAR_COLUMN_CODE],
                                                c=row[sidra_service.CLASS_COLUMN_NAME],
                                                n=row[sidra_service.SPATIAL_COLUMN_NAME],
                                                p=row[sidra_service.PERIOD_COLUMN_NAME])
                df = sidra_api_execute.fetch_data()
                # print(df.head(3))
                if df is not None:
                    pages_data.append(df)
                    nome_aba = f'Variável {row_var[sidra_service.VAR_COLUMN_CODE]}'
                    pages_names.append(nome_aba)
                else:
                    print(f"Nenhum dado retornado para {table_number}, na variável {row_var[sidra_service.VAR_COLUMN_CODE]}")
                sleep(1.02)

            except Exception as e:
                print(f"An error occurred for table number {table_number}: {e}")
                sleep(10)
                continue

        # Save DataFrames as different tabs in an Excel file
        if pages_data:
            with pd.ExcelWriter(f'{output_dirs.get("silver")}\\Tabela {table_number}.xlsx', engine='openpyxl') as writer:
                for df, nome_aba in zip(pages_data, pages_names):
                    df.to_excel(writer, sheet_name=nome_aba, index=False)
        else:
            print(f"Nenhum dado retornado para escrever em Excel")


def individual_table_interaction(table_number: int):
    sidra_service = SidraManager() 
    sidra_api = SidraAPI()

    metadata = sidra_service.sidra_get_metadata(table_number)
    tabela = sidra_service.sidra_process_table(metadata)[0]
    variaveis = sidra_service.sidra_process_variables(metadata, table_number)
    categorias = sidra_service.sidra_process_categories(metadata, table_number)

    pages_data = []
    pages_names = []

    for idx, var in variaveis.iterrows():

        try:
            sidra_api.build_url(t=table_number,
                                v=var['id'],
                                c=''.join(categorias[1]),
                                n=tabela.loc[0, 'Nível Territorial'],
                                p=tabela.loc[0, 'Frequência'])
            
            response = sidra_api.fetch_data()
            df = pd.DataFrame(response)
            df.columns = df.iloc[0]

            if df is not None:
                pages_names.append(f"Variável {var['id']}")
                pages_data.append(df)
            sleep(1.02)
            
        except Exception as e:

            print(f"Erro ao obter dados: {table_number}: {e}")
            sleep(10)
            continue

    if pages_data:
        with pd.ExcelWriter(f'{output_dirs.get("silver")}\\Tabela {table_number}.xlsx', engine='openpyxl') as writer:
            for tab, nome_aba in zip(pages_data, pages_names):
                tab.to_excel(writer, sheet_name=nome_aba, index=False)
    
    return pages_data




if __name__ == "__main__": 

    # with open(f'{data_path}/preset-tables.json', 'r') as json_file:
    #     data = json.load(json_file)

    # table_numbers = [item['tabela'] for item in data]
    
    df = individual_table_interaction(109)
    print(df)