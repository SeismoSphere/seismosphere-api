FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY operators /opt/airflow/operators
COPY init-airflow.sh /usr/local/bin/init-airflow.sh

USER root
RUN chmod +x /usr/local/bin/init-airflow.sh
USER airflow

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__DATABASE__CHECK_MIGRATIONS=False
