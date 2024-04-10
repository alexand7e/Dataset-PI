import os
import sys

sys.path.append(os.path.dirname(__file__))

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

from estruturas import *

class GoogleDriveManager:
    def __init__(self, credentials_path: str, information_path: str) -> None:
        """Inicializa a instância da classe GoogleDriveManager com os caminhos para as credenciais e informações."""

        self.credentials_path = credentials_path
        self.drive_service = None
        self.sheets_service = None

        self.INFORMATION_PATH = information_path
        self._authenticate()

    def _authenticate(self):
        """Autentica a instância para uso dos serviços do Google Drive e Google Sheets usando as credenciais fornecidas."""

        scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(self.credentials_path, scopes=scopes)
        self.drive_service = build('drive', 'v3', credentials=creds)
        self.sheets_service = gspread.authorize(creds)

    def create_folder(self, folder_name, parent_folder_id=None, make_public=False):
        """
        Cria uma nova pasta no Google Drive.
        Argumentos:
        folder_name -- Nome da nova pasta.
        parent_folder_id -- ID opcional da pasta pai onde a nova pasta será criada.
        make_public -- Torna a pasta pública pela URL."""
        
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        # Se um ID de pasta pai for fornecido, cria a nova pasta dentro dela
        if parent_folder_id:
            folder_metadata['parents'] = [parent_folder_id]

        folder = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
        folder_id = folder.get('id')

        if make_public:
            self._make_public(folder_id)

        folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
        return folder_id, folder_url


    def create_spreadsheet(self, spreadsheet_name):
        """Cria uma nova planilha no Google Sheets.
        Argumento:
        spreadsheet_name -- Nome da nova planilha."""

        spreadsheet = self.sheets_service.create(spreadsheet_name)
        spreadsheet_id = spreadsheet.id
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        return spreadsheet_id, spreadsheet_url
    

    def move_spreadsheet_to_folder(self, spreadsheet_id, folder_id):
        """Move uma planilha para uma pasta específica no Google Drive.
        Argumentos:
        spreadsheet_id -- ID da planilha a ser movida.
        folder_id -- ID da pasta de destino."""

        file = self.drive_service.files().get(fileId=spreadsheet_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        self.drive_service.files().update(
            fileId=spreadsheet_id,
            addParents=folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()


    def _make_public(self, file_id):
        public_permission = {
            'type': 'anyone',
            'role': 'reader',
        }
        self.drive_service.permissions().create(
            fileId=file_id,
            body=public_permission,
            fields='id'
        ).execute()


    def share_with_user(self, file_id, user_email):
        """Compartilha um arquivo ou pasta com um usuário específico.
        Argumentos:
        file_id -- ID do arquivo ou pasta a ser compartilhado.
        user_email -- Email do usuário com quem o arquivo será compartilhado."""

        user_permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': user_email
        }
        self.drive_service.permissions().create(
            fileId=file_id,
            body=user_permission,
            fields='id'
        ).execute()

    def delete_folder(self, folder_id):
        """Exclui uma pasta e todos os seus conteúdos no Google Drive.
        Argumento:
        folder_id -- ID da pasta a ser excluída."""

        query = f"'{folder_id}' in parents"
        response = self.drive_service.files().list(q=query).execute()
        files = response.get('files', [])

        # Exclui cada arquivo dentro da pasta
        for file in files:
            self.drive_service.files().delete(fileId=file['id']).execute()

        # Exclui a pasta
        self.drive_service.files().delete(fileId=folder_id).execute()
    
    def delete_file(self, file_id):
        """Exclui um arquivo específico no Google Drive.
        Argumento:
        file_id -- ID do arquivo a ser excluído."""
    
        self.drive_service.files().delete(fileId=file_id).execute()

    def list_all_contents(self, folder_id=None):
        """Lista todos os conteúdos de uma pasta específica ou do diretório raiz, se nenhum ID de pasta for fornecido.
        Argumento:
        folder_id -- ID opcional da pasta cujo conteúdo será listado (padrão é o diretório raiz)."""

        contents = self._list_folder_contents(folder_id)
        return json.dumps(contents, indent=4)

    def _list_folder_contents(self, folder_id):
        """Método auxiliar para listar os conteúdos de uma pasta no Google Drive.
        Argumento:
        folder_id -- ID da pasta cujo conteúdo será listado."""

        query = f"'{folder_id}' in parents" if folder_id else "trashed = false and 'root' in parents"

        items = self.drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute().get('files', [])

        contents = []
        for item in items:
            item_info = {
                'id': item['id'],
                'name': item['name'],
                'type': 'folder' if item['mimeType'] == 'application/vnd.google-apps.folder' else 'file'
            }

            if item_info['type'] == 'folder':
                item_info['contents'] = self._list_folder_contents(item['id'])

            contents.append(item_info)

        return contents



class GoogleSheetManager:

    def __init__(self, credentials_path):
        """Inicializa a instância da classe com o caminho para as credenciais do Google Sheets."""
        self.credentials_path = credentials_path
        self.gc = gspread.service_account(filename=self.credentials_path)

    def get_data_info_from_url(self, url, sheet_index=0) -> pd.DataFrame:
        """Obtém dados de uma planilha do Google Sheets como um DataFrame do pandas.
            Argumentos:
            url -- URL da planilha do Google Sheets.
            sheet_index -- Índice da página (worksheet) dentro da planilha (padrão 0, que é a primeira página)."""
        
        sh = self.gc.open_by_url(url)
        worksheet = sh.get_worksheet(sheet_index)

        df = pd.DataFrame(worksheet.get_all_records())
        return df

    def insert_data(self, url, sheet_index, data, row, col):
        """Insere dados em uma posição específica de uma página do Google Sheets.
        Argumentos:
        url -- URL da planilha do Google Sheets.
        sheet_index -- Índice da página onde os dados serão inseridos.
        data -- Dados a serem inseridos (lista ou valor individual).
        row -- Número da linha para começar a inserção.
        col -- Número da coluna para começar a inserção."""

        sh = self.gc.open_by_url(url)
        worksheet = sh.get_worksheet(sheet_index)

        worksheet.update(f'{gspread.utils.rowcol_to_a1(row, col)}', data)


    def insert_data_from_df(self, url, sheet_index, df):
        """Insere dados a partir de pandas dataframes.
        Argumentos:
        url -- URL da planilha do Google Sheets.
        sheet_index -- Índice da página onde os dados serão inseridos.
        data -- Dados a serem inseridos (lista ou valor individual).
        row -- Número da linha para começar a inserção.
        col -- Número da coluna para começar a inserção."""

        sh = self.gc.open_by_url(url)
        worksheet = sh.get_worksheet(sheet_index)

        set_with_dataframe(worksheet, df)


    def insert_data_to_worksheet(self, worksheet, df):
        """
        A comentar
        """
        set_with_dataframe(worksheet, df)


    def create_new_sheet(self, url, title, rows=1000, cols=26):
        """Cria uma nova página (worksheet) em uma planilha do Google Sheets.
        Argumentos:
        url -- URL da planilha do Google Sheets.
        title -- Título da nova página.
        rows -- Número de linhas da nova página (padrão 1000).
        cols -- Número de colunas da nova página (padrão 26)."""

        sh = self.gc.open_by_url(url)
        worksheet = sh.add_worksheet(title=title, rows=rows, cols=cols)
        return worksheet

    def insert_rows(self, url, sheet_index, data, start_index=1):
        """Insere linhas em uma página existente do Google Sheets.
        Argumentos:
        url -- URL da planilha do Google Sheets.
        sheet_index -- Índice da página onde as linhas serão inseridas.
        data -- Dados das linhas a serem inseridas (cada sublista é uma linha).
        start_index -- Índice da linha onde a inserção deve começar (padrão 1)."""

        sh = self.gc.open_by_url(url)
        worksheet = sh.get_worksheet(sheet_index)
        worksheet.insert_rows(data, start_index)

    def add_worksheet(self, url, title, rows = 1000, cols=26):
        """Adiciona uma nova página (worksheet) à planilha do Google Sheets.
        Argumentos:
        url -- URL da planilha do Google Sheets.
        title -- Título da nova página.
        rows -- Número de linhas da nova página.
        cols -- Número de colunas da nova página."""

        sh = self.gc.open_by_url(url)
        worksheet = sh.add_worksheet(title=title, rows=rows, cols=cols)
        return worksheet

    def delete_worksheet(self, url, title):
        """Deleta uma página específica da planilha do Google Sheets.
        Argumentos:
        url -- URL da planilha do Google Sheets.
        title -- Título da página a ser deletada."""

        sh = self.gc.open_by_url(url)
        worksheet = sh.worksheet(title)
        sh.del_worksheet(worksheet)

    def list_worksheets(self, url):
        """Lista todas as páginas (worksheets) de uma planilha do Google Sheets.
        Argumento:
        url -- URL da planilha do Google Sheets."""

        sh = self.gc.open_by_url(url)
        return [worksheet.title for worksheet in sh.worksheets()]
    
