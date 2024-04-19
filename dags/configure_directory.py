import os
import shutil
import pandas as pd

class DirectoryManager:
    def __init__(self, origin_directory, destiny_directory):
        self.origin_directory = origin_directory
        self.destiny_directory = destiny_directory
    
    def _list_files(self):
        files = [f for f in os.listdir(self.origin_directory) if os.path.isfile(os.path.join(self.origin_directory, f))]
        return pd.DataFrame(files, columns=['filename'])
    
    def _organize_files(self, df):
        for idx, row in df.iterrows():
            if 'pasta' in df.columns and 'subpasta' in df.columns:
                folder_path = os.path.join(self.destiny_directory, row['pasta'], row['subpasta'])
                os.makedirs(folder_path, exist_ok=True)
                file_path = os.path.join(self.origin_directory, row['filename'])
                new_path = os.path.join(folder_path, row['filename'])
                if os.path.exists(file_path):
                    shutil.copy(file_path, new_path)  # Copiar em vez de mover
    
    def execute_organize_files(self, df_folders):
        df = self._list_files()
        df['tabela'] = df['filename'].str.extract('(\d+)')  # Extrai o número
        df['tabela'] = df['tabela'].astype(str)  # Converte para string

        df_folders['tabela'] = df_folders['tabela'].astype(str)  # Assegura que 'tabela' no df_folders também seja string

        if 'tabela' in df_folders.columns:
            df = df.merge(df_folders, on='tabela', how='inner')  # Agora 'tabela' em ambos os DataFrames são strings
            self._organize_files(df)

