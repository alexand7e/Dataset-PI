import sys
import os
import pandas as pd

# Caminho para o diretório onde o módulo está localizado
module_path = 'C:\\Users\\usuario\\OneDrive\\Documentos\\R\\Datasetpi\\Python\\dags'
data_path = 'C:\\Users\\usuario\\OneDrive\\Documentos\\R\\Datasetpi\\Python\\data\\bi'
sys.path.insert(0, module_path)

from postgres import PostgreSQL

schemas = ['trabalho', 'domicilios', 'mercado_de_trabalho', 'rendimento', 'populacao_desocupada', 'pessoal_ocupado', 'rendimento_de_todas_as_fontes', 'características_do_trabalho_e_apoio_social']

for schema in schemas:
    db = PostgreSQL(schema=schema)
    df = db.read_all_tables()
    # Define o caminho onde o DataFrame será salvo
    file_path = os.path.join(data_path, f'{schema}_data.csv')
    # Salva o DataFrame em um arquivo CSV
    df.to_csv(file_path, index=False)
    print(f'DataFrame do esquema {schema} salvo em {file_path}')
