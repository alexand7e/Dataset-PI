FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y postgresql libpq-dev && apt-get clean

COPY requirements.txt .

# Instale as dependências
USER airflow

RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install connexion[swagger-ui]

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/dags:/opt/airflow/src"