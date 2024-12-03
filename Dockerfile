FROM apache/airflow:2.9.0-python3.9

USER root
RUN apt-get update && apt-get install -y \
    libpq-dev gcc && \
    apt-get clean

USER airflow

# อัปเดต setuptools และ wheel
RUN pip install --upgrade pip setuptools wheel
# RUN pip install --upgrade pip 

# ติดตั้ง dependencies จาก requirements.txt
COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --verbose --no-cache-dir -r requirements.txt


# คัดลอกไฟล์ DAGS, Plugins, และ Tests
COPY ./mnt/dags /opt/airflow/dags
COPY ./mnt/plugins /opt/airflow/plugins
COPY ./mnt/tests /opt/airflow/tests
