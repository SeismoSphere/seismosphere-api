FROM apache/airflow:2.8.0-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY operators /opt/airflow/operators
COPY init-airflow.sh /opt/airflow/init-airflow.sh

USER root
RUN chmod +x /opt/airflow/init-airflow.sh
USER airflow

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

ENTRYPOINT ["/opt/airflow/init-airflow.sh"]
