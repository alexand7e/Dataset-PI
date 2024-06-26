
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
  - `__data__info.json`
  - `credentials.json`
  - `preset-tables.json`
  - `silver/Tabela 109.xlsx`
  - `template.xlsx`
- `docker-compose.yaml`: Arquivo de configuração do Docker Compose.
- `dockerfile`: Arquivo de configuração do Docker.
- `notebooks/`: Contém notebooks Jupyter.
  - `Sidra-Get_Data-Suporte.ipynb`
  - `Sidra-Set_repository.ipynb`
- `painel_dataset_piaui.pbix`: Arquivo do painel de dados do Piauí.
- `requirements.txt`: Lista de dependências do Python.
- `src/`: Contém scripts Python.
  - `__init__.py`
  - `database_manager.py`
  - `local_directory.py`
  - `main.py`
  - `remote_directory.py`
  - `sidra.py`
  - `sidra_extraction.py`

## Instalação

### Requisitos
- Python 3.x
- Jupyter Notebook
- Docker
- Bibliotecas necessárias (listadas no `requirements.txt`)

### Passos para Instalação
Clone o repositório e instale as dependências:

\`\`\`bash
git clone https://github.com/alexand7e/Dataset-PI.git
cd Dataset-PI
pip install -r requirements.txt
\`\`\`

Para utilizar o Docker, execute:

\`\`\`bash
docker-compose up
\`\`\`

## Exemplo de Uso

### Script Python

\`\`\`python
# exemplo_script.py
import pandas as pd

def processar_dados(file_path):
    df = pd.read_csv(file_path)
    # Realiza processamento dos dados
    return df

if __name__ == "__main__":
    df = processar_dados('data/exemplo.csv')
    print(df.head())
\`\`\`

### Notebook Jupyter

Um exemplo de notebook Jupyter está disponível em `notebooks/Sidra-Get_Data-Suporte.ipynb`. Este notebook demonstra como carregar, processar e visualizar dados:

\`\`\`python
# Sidra-Get_Data-Suporte.ipynb

import pandas as pd
import matplotlib.pyplot as plt

# Carregar dados
df = pd.read_excel('../data/silver/Tabela 109.xlsx')

# Processar dados
df_processed = df[df['coluna'] > 0]

# Visualizar dados
plt.plot(df_processed['coluna'])
plt.show()
\`\`\`

### Estrutura de Pastas e Arquivos

1. **scripts/**: Contém scripts Python.
   - exemplo_script.py
2. **notebooks/**: Contém notebooks Jupyter.
   - Sidra-Get_Data-Suporte.ipynb
   - Sidra-Set_repository.ipynb
3. **data/**: Contém exemplos de dados.
   - __data__info.json
   - credentials.json
   - preset-tables.json
   - silver/Tabela 109.xlsx
   - template.xlsx
4. **docs/**: Contém a documentação detalhada.
   - overview.md
   - installation.md
   - scripts/
     - exemplo_script.md

### .gitignore

Atualize o `.gitignore` para incluir itens específicos de Python e Jupyter:

\`\`\`
# Arquivos temporários
*.tmp
*.log

# Dados
data/*.csv
data/*.xlsx

# Arquivos Python
__pycache__/
*.pyc

# Arquivos Jupyter
.ipynb_checkpoints
\`\`\`

### Passos Finais

1. **Preencha os arquivos README.md e a documentação detalhada conforme o modelo acima.**
2. **Organize seus scripts na pasta `scripts/` com subpastas se necessário.**
3. **Adicione notebooks na pasta `notebooks/`.**
4. **Adicione exemplos de dados na pasta `data/`.**
5. **Comite e envie suas mudanças para o GitHub.**

\`\`\`bash
git add .
git commit -m "Organiza repositório, adiciona documentação e exemplos de uso"
git push origin main
\`\`\`
