
import json

class InformationBaseManager:
    def __init__(self, file_path) -> None:
        self.FILE_PATH = file_path

    def update_json_file(self, keys, new_data):
        """
        Atualiza um arquivo JSON, adicionando novos dados em uma chave específica, que pode estar em vários níveis.

        :param keys: Lista de chaves para alcançar o nível desejado no JSON.
        :param new_data: Os novos dados a serem adicionados.
        """
        try:
            with open(self.FILE_PATH, 'r') as file:
                data = json.load(file)

            temp = data
            for key in keys[:-1]:
                if key in temp:
                    temp = temp[key]
                else:
                    return "Uma ou mais chaves não foram encontradas no caminho especificado."

            last_key = keys[-1]
            if isinstance(temp.get(last_key, None), dict):
                temp[last_key].update(new_data)
            else:
                temp[last_key] = new_data

            with open(self.FILE_PATH, 'w') as file:
                json.dump(data, file, indent=4)

            return "Dados atualizados com sucesso."

        except FileNotFoundError:
            return "Arquivo JSON não encontrado."

        except json.JSONDecodeError:
            return "Erro ao decodificar o JSON."

        except Exception as e:
            return f"Ocorreu um erro: {e}"
    

    def add_subfolder_to_json(self, new_subfolder):
        """
        Adiciona um novo subfolder ao arquivo JSON especificado.

        :param new_subfolder: Dicionário representando o novo subfolder a ser adicionado.
        """
        try:
            # Lendo o arquivo JSON
            with open(self.FILE_PATH, 'r') as file:
                data = json.load(file)

            # Adicionando o novo subfolder
            if "MainFolder" in data and "Subfolders" in data["MainFolder"]:
                data["MainFolder"]["Subfolders"].append(new_subfolder)
            else:
                return "A estrutura do JSON não possui 'MainFolder' e 'Subfolders' adequados."

            # Escrevendo as modificações de volta para o arquivo JSON
            with open(self.FILE_PATH, 'w') as file:
                json.dump(data, file, indent=4)

            return "Subfolder adicionado com sucesso."

        except FileNotFoundError:
            return "Arquivo JSON não encontrado."

        except json.JSONDecodeError:
            return "Erro ao decodificar o JSON."

        except Exception as e:
            return f"Ocorreu um erro: {e}"
        


    def add_spreadsheet_to_json(self, subfolder_name, new_spreadsheet):
        """
        Adiciona uma nova planilha à lista 'Spreadsheets' de um subfolder específico no arquivo JSON.

        :param subfolder_name: Nome do subfolder onde a planilha será adicionada.
        :param new_spreadsheet: Dicionário representando a nova planilha.
        """
        try:
            # Lendo o arquivo JSON
            with open(self.FILE_PATH, 'r') as file:
                data = json.load(file)

            # Verifica se a estrutura do JSON é adequada
            if "MainFolder" in data and "Subfolders" in data["MainFolder"]:
                # Procura o subfolder pelo nome
                subfolder_found = False
                for subfolder in data["MainFolder"]["Subfolders"]:
                    if subfolder["Name"] == subfolder_name:
                        subfolder["Spreadsheets"].append(new_spreadsheet)
                        subfolder_found = True
                        break

                if not subfolder_found:
                    return f"Subfolder '{subfolder_name}' não encontrado."

            else:
                return "A estrutura do JSON não possui 'MainFolder' e 'Subfolders' adequados."

            # Escrevendo as modificações de volta para o arquivo JSON
            with open(self.FILE_PATH, 'w') as file:
                json.dump(data, file, indent=4)

            return "Planilha adicionada com sucesso."

        except FileNotFoundError:
            return "Arquivo JSON não encontrado."

        except json.JSONDecodeError:
            return "Erro ao decodificar o JSON."

        except Exception as e:
            return f"Ocorreu um erro: {e}"
        

    def get_field_from_json(self, keys):
        """
        Obtem um campo específico de um arquivo JSON usando uma lista de chaves.

        :param keys: Lista de chaves para acessar o campo desejado.
        :return: O campo em formato string, ou uma mensagem de erro se não encontrado.
        """
        try:
            with open(self.FILE_PATH, 'r') as file:
                data = json.load(file)

            # Acessando o campo desejado
            for key in keys:
                if key in data:
                    data = data[key]
                else:
                    return "Chave não encontrada."

            return str(data)

        except FileNotFoundError:
            return "Arquivo JSON não encontrado."

        except json.JSONDecodeError:
            return "Erro ao decodificar o JSON."

        except Exception as e:
            return f"Ocorreu um erro: {e}"
        

    def get_subfolder_id(self, subfolder_name):
        """
        Obtem o ID de uma subpasta específica de um arquivo JSON.

        :param subfolder_name: Nome da subpasta a ser buscada.
        :return: O ID da subpasta em formato string, ou uma mensagem de erro se não encontrada.
        """
        try:
            with open(self.FILE_PATH, 'r') as file:
                data = json.load(file)

            # Verificando se a estrutura principal do JSON é a esperada
            if "MainFolder" in data and "Subfolders" in data["MainFolder"]:
                # Percorrendo as subpastas
                for subfolder in data["MainFolder"]["Subfolders"]:
                    if subfolder.get("Name") == subfolder_name:
                        return subfolder.get("ID", "ID não encontrado para a subpasta especificada.")
                return "Subpasta não encontrada."
            else:
                return "A estrutura do JSON não possui 'MainFolder' e 'Subfolders' adequados."

        except FileNotFoundError:
            return "Arquivo JSON não encontrado."

        except json.JSONDecodeError:
            return "Erro ao decodificar o JSON."

        except Exception as e:
            return f"Ocorreu um erro: {e}"

import json
import datetime

class DataManager():
    def __init__(self, file_path):
        self.FILE_PATH = file_path
        self.update = datetime.datetime.now()

    def create_json(self, admins, knowledge_base, folder_id, folder_name, folder_url):
        data = {
            "Admins": admins,
            "Knowledge base": knowledge_base,
            "Last update": self.update,
            "MainFolder": {
                "Name": folder_name,
                "ID": folder_url,
                "URL": folder_id,
                "Subfolders": []
            }
        }
        with open(self.FILE_PATH, 'w') as file:
            json.dump(data, file, indent=4)

    def add_subfolders_to_folder(self, folder_id, subfolder_id, subfolder_name, subfolder_url):
        data = {"ID": subfolder_id,
                "Name": subfolder_name,
                "URL": subfolder_url}

        with open(self.FILE_PATH, 'r') as file:
            data = json.load(file)
        if folder_id == data["MainFolder"]["ID"]:
            data["MainFolder"]["Subfolders"].append(data)
        else:
            for sub in data["MainFolder"]["Subfolders"]:
                if sub["ID"] == folder_id:
                    if "Subfolders" not in sub:
                        sub["Subfolders"] = []
                    sub["Subfolders"].append(data)
                    break
        with open(self.FILE_PATH, 'w') as file:
            json.dump(data, file, indent=4)


    def add_spreadsheets_to_subfolder(self, 
                                      subfolder_id, 
                                      spreadsheet_id, 
                                      spreadsheet_num, 
                                      spreadsheet_name, 
                                      spreadsheet_url):
        
        spread_data = {"ID": spreadsheet_id, 
                       "Número": spreadsheet_name, 
                       "Name": spreadsheet_name, 
                       "URL": spreadsheet_url}

        with open(self.FILE_PATH, 'r') as file:
            data = json.load(file)
            
        for subfolder in data["MainFolder"]["Subfolders"]:
            if subfolder["ID"] == subfolder_id:
                subfolder["Spreadsheets"].append(spread_data)
                break
        with open(self.FILE_PATH, 'w') as file:
            json.dump(data, file, indent=4)


    def add_sheets_to_spreadsheet(self, spreadsheet_id, sheet_idx: int, sheet_name: str):
        sheet_data = {"Index": sheet_idx, "Name": sheet_name}

        with open(self.FILE_PATH, 'r') as file:
            data = json.load(file)
        for subfolder in data["MainFolder"]["Subfolders"]:
            for spreadsheet in subfolder.get("Spreadsheets", []):
                if spreadsheet["ID"] == spreadsheet_id:
                    spreadsheet["Sheets"].append(sheet_data)
                    break

        with open(self.FILE_PATH, 'w') as file:
            json.dump(data, file, indent=4)


    def get_folders_or_subfolders(self, folder_id=None) -> list:
        with open(self.FILE_PATH, 'r') as file:
            data = json.load(file)
        if folder_id is None or folder_id == data["MainFolder"]["ID"]:
            return data["MainFolder"]["Subfolders"]
        for subfolder in data["MainFolder"]["Subfolders"]:
            if subfolder["ID"] == folder_id:
                return subfolder.get("Subfolders", [])
        return []
    

    def get_spreadsheets(self, subfolder_id) -> list:
        with open(self.FILE_PATH, 'r') as file:
            data = json.load(file)
        for subfolder in data["MainFolder"]["Subfolders"]:
            if subfolder["ID"] == subfolder_id:
                return subfolder.get("Spreadsheets", [])
        return []


    def get_spreadsheets_from_folder(self, folder_id) -> list:
        spreadsheets = []
        for subfolder in self.get_folders_or_subfolders(folder_id):
            spreadsheets.extend(subfolder.get("Spreadsheets", []))
        return spreadsheets
    

    def get_sheets_from_spreadsheet(self, spreadsheet_id) -> list:
        with open(self.FILE_PATH, 'r') as file:
            data = json.load(file)
        for subfolder in data["MainFolder"]["Subfolders"]:
            for spreadsheet in subfolder.get("Spreadsheets", []):
                if spreadsheet["ID"] == spreadsheet_id:
                    return spreadsheet.get("Sheets", [])
        return []


    def update_field_by_path(self, path, value) -> None:
        """
        Atualiza um campo no JSON definindo um caminho (lista de chaves) até ele.

        :param path: Lista de chaves que define o caminho até o campo. Exemplo: ['MainFolder', 'ID']
        :param value: Novo valor a ser atribuído ao campo.
        """
        with open(self.FILE_PATH, 'r') as file:
            data = json.load(file)

        # Acessa o campo no dicionário usando o caminho especificado, exceto o último elemento
        temp = data
        for key in path[:-1]:  # Percorre todas as chaves, exceto a última
            if key.isdigit() and key.isdigit() < len(temp):  # Se a chave for um dígito e dentro do índice
                temp = temp[int(key)]  # Acessa o índice se a chave for um número
            else:
                temp = temp[key]  # Acessa a chave diretamente

        # Atualiza o valor do campo especificado pela última chave no caminho
        last_key = path[-1]
        if last_key.isdigit() and int(last_key) < len(temp):  # Se a última chave for um índice numérico
            temp[int(last_key)] = value
        else:
            temp[last_key] = value

        with open(self.FILE_PATH, 'w') as file:
            json.dump(data, file, indent=4)
