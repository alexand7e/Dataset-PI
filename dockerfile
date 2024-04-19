FROM apache/airflow:2.8.1-python3.11


USER root
RUN apt-get update && apt-get install -y postgresql libpq-dev && apt-get clean

COPY requirements.txt .

# Instale as dependências
USER airflow
RUN python -m pip install --upgrade pip 
RUN pip install --no-deps googletrans==3.0.0
RUN pip install -r requirements.txt 
RUN pip install connexion[swagger-ui]
RUN pip install --no-deps gspread-dataframe
# RUN pip install httpx

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/dags:/opt/airflow/src"