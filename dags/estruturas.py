import json
import datetime

class InformationManager():
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
