FROM apache/airflow:8.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        <your-system-dependency> \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir <your-python-library>

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
