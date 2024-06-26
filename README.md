
# Automação dos dados do SIDRA

## Descrição
Este banco de dados oferece um acesso facilitado e segmentado às principais tabelas do SIDRA, especificamente adaptadas para o estado do Piauí, permitindo análises detalhadas e personalizadas dos dados socioeconômicos e demográficos da região.

## Autor
Alexandre Barros

## Estrutura do Repositório
- `.gitignore`: Arquivo de configuração para ignorar arquivos desnecessários.
- `README.md`: Este arquivo README.
- `README.pdf`: Versão PDF do README.
- `data/`: Contém dados e arquivos relacionados.
  - `bronze/*`
  - `silver/*`
  - `gold/*`
  - `__data__info.json`
  - `preset-tables.json`
  - `template.xlsx`
- `docker-compose.yaml`: Arquivo de configuração do Docker Compose.
- `dockerfile`: Arquivo de configuração do Docker.
- `notebooks/`: Contém notebooks Jupyter.
  - `Sidra-Get_Data-Suporte.ipynb`
  - `Sidra-Set_repository.ipynb`
- `painel_dataset_piaui.pbix`: Arquivo do painel de dados do Piauí.
- `requirements.txt`: Lista de dependências do Python.
  - `__init__.py`: Inicializa o pacote src.
  - `database_manager.py`: Gerencia a conexão e operações com o banco de dados. **(Em desenvolvimento)**
  - `local_directory.py`: Gerencia operações de diretórios locais. **(Pronto)**
  - `main.py`: Script principal que integra todas as funcionalidades. **(Em desenvolvimento)**
  - `remote_directory.py`: Gerencia operações de diretórios remotos. **(Pronto)**
  - `sidra.py`: Funções específicas para interagir com a API do SIDRA. **(Pronto)**
  - `sidra_extraction.py`: Realiza a extração dos dados do SIDRA. **(Pronto)**
  - `airflow_dag.py`: DAG do Airflow para orquestrar as tarefas de extração e processamento. **(Ignorado)**


## Instalação

### Requisitos
- Python 3.10
- Jupyter Notebook
- Docker
- Bibliotecas necessárias (listadas no `requirements.txt`)

### Passos para Instalação
Clone o repositório e instale as dependências:

```bash
git clone https://github.com/alexand7e/Dataset-PI.git
cd Dataset-PI
pip install -r requirements.txt
```

Para utilizar o Docker, execute:

```bash
docker-compose up
```

## Exemplo de Uso

### Script Python

```Python
# exemplo_script.py
from main import Main

if __name__ == "__main__":
    # Configurações iniciais
    list_of_tables = [109, 4090]  # Exemplo de tabelas
    create_remote_directory = True
    conecting_db = False

    # Inicializa a classe principal e executa o processamento
    main_process = Main(list_of_tables, create_remote_directory, conecting_db)
    # main_process.main()
    main_process.process_data()
```

### Configuração do Arquivo .env

Exemplo de um arquivo .env:

```bash
# Credenciais do banco de dados
DB_HOST=localhost
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_NAME=nome_do_banco

# Pode configurar as credenciais da API google
API_KEY=sua_api_key

```

### .gitignore

Atualize o `.gitignore` para incluir itens específicos de Python e Jupyter:

```
# Arquivos temporários
*.tmp
*.log

# Dados
data/bronze
data/silver
data/gold

# Arquivos Python
src/*
*.pyc

# Arquivos Jupyter
.ipynb_checkpoints
```

### Passos Finais

1. **Preencha os arquivos README.md e a documentação detalhada conforme o modelo acima.**
2. **Organize seus scripts na pasta `scripts/` com subpastas se necessário.**
3. **Adicione notebooks na pasta `notebooks/`.**
4. **Adicione exemplos de dados na pasta `data/`.**
5. **Comite e envie suas mudanças para o GitHub.**

```bash
git add .
git commit -m "Organiza repositório, adiciona documentação e exemplos de uso"
git push origin main
```
