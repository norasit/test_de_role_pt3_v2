FROM apache/airflow:2.7.2

# ติดตั้ง dependencies ที่จำเป็น
USER root
RUN apt-get update && apt-get install -y \
    libpq-dev gcc && \
    apt-get clean

USER airflow

# ติดตั้ง Python dependencies จาก requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGS, Plugins, และ Tests
COPY ./mnt/dags /opt/airflow/dags
COPY ./mnt/plugins /opt/airflow/plugins
COPY ./mnt/tests /opt/airflow/tests
