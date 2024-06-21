# Dependências
from psycopg2 import (Error, sql)
import psycopg2
import pandas as pd
import unicodedata
import re


class PostgreSQL:

    def __init__(self, default_connection: bool = True, 
                 user: str = None, 
                 passw: str = None, 
                 host: str = None, 
                 database: str = None, 
                 port: str = None,
                 schema: str = "public"):
        
        if default_connection:
            self.user = "airflow"
            self.password = "airflow"
            self.host = "localhost"
            self.db = "airflow"
            self.port = "5432"
        else:
            self.user = user
            self.password = passw
            self.host = host
            self.db = database
            self.port = port

        self.connector = self._connection()
        self.set_schema(schema)

    def _connection(self):
        try:
            cnx = psycopg2.connect(user=self.user, 
                                   password=self.password,
                                   host=self.host,
                                   dbname=self.db,
                                   port=self.port,
                                   options='-c client_encoding=utf8')
            
            print("Conexão com o banco de dados estabelecida com sucesso.")
            return cnx
        except Error as e:
            print(f"Erro ao conectar ao banco de dados PostgreSQL: {e}")
            return None

    def set_schema(self, schema):
        cursor = self.connector.cursor()
        self.schema = schema
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            self.connector.commit()
        except Error as e:
            print(f"Erro ao criar ou verificar o esquema '{schema}': {e}")
        finally:
            cursor.close()
        
    def charge_table(self, 
                     filename: str, 
                     namepage: str, 
                     skip_rows: int = 0):
        
        return self.I.importar_tabelas_from_excel(filename, namepage, skip_rows)
            
             
    def table_exists(self, 
                     table_name: str) -> bool:
        
        query = sql.SQL("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = {} AND table_name = {})").format(
            sql.Literal(self.schema),
            sql.Literal(table_name)
        )

        cursor = self.connector.cursor()
        cursor.execute(query)
        exists = cursor.fetchone()[0]
        cursor.close()

        return exists   


    def create_table(self, table_name: str, df: pd.DataFrame, adjust_dataframe: bool = False) -> None:
        
        full_table_name = f'{self.schema}.{table_name}'
        
        if df is None:
            raise ValueError("O DataFrame 'df' é None antes de chamar create_table.")
        # else:
        #     print(f"DataFrame tem {len(df)} linhas e {len(df.columns)} colunas antes de chamar create_table.")

        try:
            cursor = self.connector.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")

            cursor.execute(f'CREATE TABLE {full_table_name} (indice SERIAL PRIMARY KEY)')

            if adjust_dataframe:
                df.columns = [self.format_string(col) for col in df.columns]


            column_data_types = ['TEXT'] * len(df.columns)

            for col, data_type in zip(df.columns, column_data_types):
                col_query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    sql.Identifier(self.schema, table_name),
                    sql.Identifier(col),
                    sql.SQL(data_type)
                )
                cursor.execute(col_query)

            for row in df.itertuples(index=False):
                valores = tuple(row)
                colunas = sql.SQL(', ').join([sql.Identifier(c) for c in df.columns])
                placeholders = sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(self.schema, table_name),
                    colunas,
                    placeholders
                )
                cursor.execute(insert_query, valores)

            self.connector.commit()
            cursor.close()

        except Error as e:
            print(f"Erro ao criar ou recriar a tabela: {e}")


    def insert_into_table(self, table_name: str, df: pd.DataFrame, adjust_dataframe: bool = False) -> None:

        if adjust_dataframe:
            df.columns = [self.format_string(col) for col in df.columns]

        if self.table_exists(table_name):
            try:
                cursor = self.connector.cursor()
                colunas = sql.SQL(', ').join([sql.Identifier(c) for c in df.columns])
                placeholders = sql.SQL(', ').join(sql.Placeholder() for _ in df.columns)  # Corrected line
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(self.schema, table_name),
                    colunas,
                    placeholders
                )
                # Execute insert query for each row
                for row in df.itertuples(index=False):
                    valores = tuple(row)
                    cursor.execute(insert_query, valores)
                self.connector.commit()
                cursor.close()
            except Exception as e:  # Changed from Error to Exception for a broader catch
                print(f"Erro ao inserir dados na tabela: {e}")
        else:
            print(f"A tabela '{table_name}' não existe.")
                
                
    def upsert_table_data(self, table_name: str, df: pd.DataFrame, adjust_dataframe: bool = False) -> None:
        if adjust_dataframe:
            df.columns = [self.format_string(col) for col in df.columns]

        if self.table_exists(table_name):
            try:
                cursor = self.connector.cursor()
                colunas = sql.SQL(', ').join([sql.Identifier(c) for c in df.columns])
                placeholders = sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
                key_column = 'tabela'  # Adjust as necessary to match your key column name

                # SQL para UPSERT
                upsert_query = sql.SQL("""
                    INSERT INTO {table} ({columns})
                    VALUES ({values})
                    ON CONFLICT ({key_column})
                    DO UPDATE SET
                    {assignments}
                """).format(
                    table=sql.Identifier(self.schema, table_name),
                    columns=colunas,
                    values=placeholders,
                    key_column=sql.Identifier(key_column),
                    assignments=sql.SQL(', ').join([
                        sql.SQL("{column} = EXCLUDED.{column}").format(column=sql.Identifier(c))
                        for c in df.columns if c != key_column
                    ])
                )

                # Executa upsert para cada linha
                for row in df.itertuples(index=False):
                    valores = tuple(row)
                    cursor.execute(upsert_query, valores)
                self.connector.commit()
                cursor.close()
            except Exception as e:
                print(f"Erro ao inserir/atualizar dados na tabela '{table_name}': {e}")
        else:
            print(f"A tabela '{table_name}' não existe.")


    def read_table_columns(self, table_name: str, columns: list, return_type: str = "list") -> list:

        try:
            cursor = self.connector.cursor()
            if columns == ["*"]:
                query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(self.schema, table_name))
            else:
                columns_sql = sql.SQL(', ').join([sql.Identifier(c) for c in columns])
                query = sql.SQL("SELECT {} FROM {}").format(columns_sql, sql.Identifier(self.schema, table_name))
                
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if return_type == "list":
                return [list(row) for row in rows]
            elif return_type == "dict":
                return {col: [row[i] for row in rows] for i, col in enumerate(columns)}
            elif return_type == "dataframe":
                # Retorna um DataFrame com as colunas nomeadas corretamente
                df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
                return df
            else:
                raise ValueError("O parâmetro 'return_type' deve ser 'list', 'dict', ou 'dataframe'.")

        except Error as e:
            print(f"Erro ao ler a tabela: {e}")
            if return_type == "list":
                return []
            elif return_type == "dict":
                return {}
            else:
                return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
        finally:
            cursor.close()

    def execute_query(self, query: str) -> None:
        if not query:
            raise ValueError("A query fornecida está vazia ou é None.")

        try:
            cursor = self.connector.cursor()
            cursor.execute(query)
            self.connector.commit()
            print("Query executada com sucesso.")
        except Error as e:
            print(f"Erro ao executar a query: {e}")
        finally:
            cursor.close()

    def read_all_tables(self) -> pd.DataFrame:
        cursor = self.connector.cursor()
        query = sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = {}").format(sql.Literal(self.schema))
        cursor.execute(query)
        tables = cursor.fetchall()
        cursor.close()

        all_data_frames = []

        for table_name in tables:
            df = self.read_table_columns(table_name=table_name[0], columns=["*"], return_type="dataframe")

            if len(df.columns) == 8:
                df.rename(columns={df.columns[-1]: 'categorias'}, inplace=True)
            elif len(df.columns) == 7:
                df.rename(columns={df.columns[-1]: 'periodo'}, inplace=True)
                df['categorias'] = None  # Adiciona a coluna 'categorias' como None
            
            # Verifica e ajusta as colunas para o DataFrame final
            if len(df.columns) < 8:
                missing_cols = 8 - len(df.columns)
                for _ in range(missing_cols):
                    df[None] = None

            df['tabela'] = table_name[0]

            all_data_frames.append(df)

        # Concatena todos os DataFrames
        if all_data_frames:
            return pd.concat(all_data_frames, ignore_index=True)
        else:
            return pd.DataFrame() 
        
    def read_and_concatenate_tables(self):
        with self.connector.cursor() as cursor:
            cursor.execute(f"""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = '{self.schema}'
                AND table_type = 'BASE TABLE';
            """)
            tables = cursor.fetchall()

            df_list = []
            for table_name in tables:
                query = f"SELECT * FROM {self.schema}.{table_name[0]}"
                df = pd.read_sql(query, self.connector)

                # Renomear colunas conforme o número de colunas
                if len(df.columns) == 8:
                    df.rename(columns={df.columns[-1]: 'categorias'}, inplace=True)
                elif len(df.columns) == 7:
                    df.rename(columns={df.columns[-1]: 'periodo'}, inplace=True)
                    df['categorias'] = None  # Adiciona a coluna 'categorias' como None
                
                # Verifica e ajusta as colunas para o DataFrame final
                if len(df.columns) < 8:
                    missing_cols = 8 - len(df.columns)
                    for _ in range(missing_cols):
                        df[None] = None  # Adiciona colunas faltantes como None

                df_list.append(df)

            if df_list:
                # Ajuste final das colunas
                final_columns = df_list[0].columns.tolist()  # Assume que o primeiro DataFrame é o padrão
                for i, df in enumerate(df_list):
                    df_list[i] = df.reindex(columns=final_columns)  # Reindexa para garantir a mesma estrutura de colunas

                concatenated_df = pd.concat(df_list, ignore_index=True)
                print("DataFrames concatenados com sucesso.")
                return concatenated_df
            else:
                print("Nenhuma tabela encontrada no esquema especificado.")
                return None
        
    def format_string(self, input_string):
        normalized_string = unicodedata.normalize('NFKD', input_string)
        cleaned_string = ''.join(char for char in normalized_string if not unicodedata.combining(char))
        
        cleaned_string = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned_string)
        cleaned_string = cleaned_string.lower()
        cleaned_string = cleaned_string.replace(' ', '_').replace('-', '_')
        return cleaned_string