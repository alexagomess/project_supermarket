FROM apache/airflow:3.0.6

COPY requirements-docker.txt /tmp/requirements-docker.txt
RUN pip install --no-cache-dir -r /tmp/requirements-docker.txt
