import os
import sys
import pandas as pd
import json

# Define caminhos como variáveis globais
current_path = os.path.abspath(os.path.dirname(__file__))
data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

print(data_path)
sys.path.append(current_path)

from operadores import GoogleSheetManager, GoogleDriveManager
from estruturas import InformationManager
from sidrapi import SidraManager

def configure_paths():
    """
    Configura e retorna os caminhos essenciais e informações de configuração.
    """
    credentials_path = os.path.join(data_path, 'credentials.json')
    information_path = os.path.join(data_path, '__data__info.json')
    knowledge_path = "https://docs.google.com/spreadsheets/d/1ghxTewM8Zm8U-sMnn_Z8tJh0TUzLUiBgXWqNoj2WCzM/edit?usp=sharing"
    
    return [credentials_path, information_path, knowledge_path]

def configure_managers(credentials_path, information_path, knowledge_path):
    """
    Configura e retorna os gerenciadores para interação com serviços e dados.
    """
    sheet_manager = GoogleSheetManager(credentials_path)
    drive_manager = GoogleDriveManager(credentials_path, information_path)
    json_manager = InformationManager(information_path)
    sidra_manager = SidraManager()
    
    return [sheet_manager, drive_manager, json_manager, sidra_manager]

def get_sidra_api_info(sheet_manager, sidra_manager, knowledge_path):
    """
    Obtém informações de dados e retorna DataFrames concatenados com informações, variáveis e grupos.
    """
    df_information = sheet_manager.get_data_info_from_url(knowledge_path, 0)

    # Inicializando DataFrames vazios
    df_tabelas = pd.DataFrame()
    df_variaveis = pd.DataFrame()
    df_categorias = pd.DataFrame()

    for idx, row in df_information.iterrows():
        table_number = row["Número da Tabela"]

        content_api, df_table = sidra_manager.sidra_info(table_number)
        df_variables, df_groups = sidra_manager.sidra_get_vars(content_api)

        # Concatenando tabelas
        df_tables_expanded = pd.json_normalize(df_table.to_dict('records'))
        df_tables_expanded['Número da Tabela'] = table_number
        df_tabelas = pd.concat([df_tabelas, df_tables_expanded], ignore_index=True)

        # Tratando e concatenando variáveis
        df_variables_expanded = pd.json_normalize(df_variables.to_dict('records')) if df_variables is not None else pd.DataFrame()
        df_variables_expanded['Número da Tabela'] = table_number
        df_variaveis = pd.concat([df_variaveis, df_variables_expanded], ignore_index=True)

        # Tratando e concatenando grupos
        df_groups_expanded = pd.json_normalize(df_groups.to_dict('records')) if df_variables is not None else pd.DataFrame()
        df_groups_expanded['Número da Tabela'] = table_number
        df_categorias = pd.concat([df_categorias, df_groups_expanded], ignore_index=True)

    # Salvando em Excel
    with pd.ExcelWriter(f'{data_path}/processed/dados_sidra.xlsx', engine='openpyxl') as writer:
        df_tabelas.to_excel(writer, sheet_name='Tabelas', index=False)
        df_variaveis.to_excel(writer, sheet_name='Variáveis', index=False)
        df_categorias.to_excel(writer, sheet_name='Grupos', index=False)

    return [df_tabelas, df_variaveis, df_categorias]


def configure_initial_sheet(table_number, **kwargs):
    """
    A documentar
    """
    ti = kwargs["ti"]

    tabela, variaveis, categorias = ti.xcom_pull(task_ids="id_da_tarefa_aqui")

    tabela = tabela[tabela['Número da Tabela'] == table_number]
    variaveis = variaveis[variaveis['Número da Tabela'] == table_number]
    categorias = categorias[categorias['Número da Tabela'] == table_number]

    tabela_row = tabela.iloc[0] if not tabela.empty else None
    variaveis_rows = variaveis.to_dict('records')  # Converte para lista de dicionários
    categorias_rows = categorias.to_dict('records')  # Converte para lista de dicionários

    data_format = {
        'Nome da Tabela': tabela_row['Nome da Tabela'] if tabela_row is not None else '',
        'Tipo do Período': tabela_row['Tipo do Período'] if tabela_row is not None else '',
        'Páginas': ',\n'.join([var["Descrição"] for var in variaveis_rows]),
        'Categorias': ',\n'.join([cat['Grupo'] for cat in categorias_rows]),
        'Classificador da Tabela': tabela_row['Classificador da Tabela'] if tabela_row is not None else '',
        'Períodos Disponíveis': tabela_row['Períodos Disponíveis'] if tabela_row is not None else '',
        'Última Atualização': tabela_row['Última Atualização'] if tabela_row is not None else '',
        'Nome da Pesquisa': tabela_row['Nome da Pesquisa'] if tabela_row is not None else '',
        'Assunto': tabela_row['Assunto'] if tabela_row is not None else '',
        'Fonte': tabela_row['Fonte'] if tabela_row is not None else '',
        'Nota': tabela_row['Nota'] if tabela_row is not None else ''
    }

    # Criando um DataFrame de forma mais eficiente
    df_final = pd.DataFrame(list(data_format.items()), columns=['Campos', 'Valores'])

    return df_final


def get_sidra_api_data(sidra_manager, df_tabelas, df_variaveis):
    """
    Processa e retorna os dados obtidos em DataFrames, juntamente com metadados para inserção.
    """
    results = []
    for idx, row in df_tabelas.iterrows():
        num_tb = row["Número da Tabela"]
        tb_var = df_variaveis[df_variaveis["Número da Tabela"] == num_tb]

        pages_data = []
        pages_names = []

        for i, row_var in tb_var.iterrows():
            df = sidra_manager.fetch_sidra_data(t=num_tb, 
                                                v=row_var["Número da Variável"],
                                                c=row["Classificador da Tabela"])
            pages_data.append(df)
            nome_aba = row_var["Descrição"].split("-")[0]
            nome_aba = nome_aba.translate({ord(c): None for c in '\\/*?:"[]'})
            nome_aba = nome_aba[:31]  
            pages_names.append(nome_aba)

        results.append({
            'table_name': row["Nome da Tabela"],
            'table_number': num_tb,
            'page_data': pages_data,
            'page_name': pages_names
        })
    
    return results


def configure_drive_repository(drive_manager, sheet_manager, json_manager, information_path, knowledge_path):
    """
    Configura o repositório no Google Drive, criando uma pasta principal e subpastas conforme necessário.
    Atualiza o arquivo JSON com informações das pastas e compartilha com usuários administradores.
    """

    admins = [
        {
            "Name": "Alexandre Barros",
            "E-mail": "alexand7e@gmail.com"
        },
        {
            "Name": "Chistiano Filho",
            "E-mail": "christiannofilo@gmail.com"
        },
        {
            "Name": "Gustavo Carvalho",
            "E-mail": "g_car@ufpi.edu.br"
        }
    ]

    # Criação da pasta principal no Drive e atualização do arquivo JSON
    folder_name = "Banco de Dados do Piauí - Emprego e Renda"
    folder_id, folder_url = drive_manager.create_folder(folder_name)


    json_manager = InformationManager(information_path)
    json_manager.create_json(admins = admins, 
                             knowledge_base = knowledge_path,
                             folder_name = folder_name,
                             folder_id = folder_id,
                             folder_url = folder_url)

    data_info = sheet_manager.get_data_info_from_url(knowledge_path)


    for subfolder_name in list(data_info['Eixo (nome da pasta)'].unique()):

        subfolder_id, subfolder_url = drive_manager.create_folder(subfolder_name, parent_folder_id=folder_id)
        json_manager.add_subfolders_to_folder(folder_id,
                                              subfolder_id,
                                              subfolder_name,
                                              subfolder_url)

    # Compartilhamento da pasta principal com os administradores
    for email in data_info.get('Admins', {}).get("E-mail", None):
        drive_manager.share_with_user(folder_id, email)

    subfolders = json_manager.get_folders_or_subfolders()
    grouped_data_info = data_info.groupby('Eixo (nome da pasta)')

    for subfolder in subfolders:
        subfolder_id = subfolder.get("ID")
        subfolder_name = subfolder.get("Name")
        
        # Verificar se o subfolder_name existe no agrupamento
        if subfolder_name in grouped_data_info.groups:
            for idx, row in grouped_data_info.get_group(subfolder_name).iterrows():
                spreadsheet_name = f"{row['ID']} - {row['Nome da Tabela']}"
                spreadsheet_id, spreadsheet_url = drive_manager.create_spreadsheet(spreadsheet_name, subfolder_id)
                drive_manager.move_spreadsheet_to_folder(spreadsheet_id, subfolder_id)

                json_manager.add_spreadsheets_to_subfolder(subfolder_id,
                                                           spreadsheet_id,
                                                           spreadsheet_name,
                                                           spreadsheet_url)

                df = configure_initial_sheet(row['ID'])
                sheet_manager.insert_data_from_df(spreadsheet_id, df, 0)

        
def read_and_populate_spreadsheets(sheet_manager, drive_manager, json_manager, information_path, df_tabelas, df_variaveis):
    """
    Lê a lista de planilhas do arquivo JSON 'data_info' e popula com os dados do DataFrame.
    """
    # Carrega o arquivo JSON
    with open(information_path, 'r') as file:
        data_info = json.load(file)

    # Itera sobre as subpastas e suas planilhas
    results = get_sidra_api_data()

    for subfolder in data_info["MainFolder"]["Subfolders"]:
        for spreadsheet in subfolder["Spreadsheets"]:
            spreadsheet_num = spreadsheet["Número"]
            spreadsheet_url = spreadsheet["URL"]

            for result in results:
                if result['table_number'] == spreadsheet_num:
                    worksheet = sheet_manager.add_worksheet(spreadsheet_url, result['page_name'])
                    sheet_manager.insert_data_to_worksheet(worksheet, result['page_data'])
