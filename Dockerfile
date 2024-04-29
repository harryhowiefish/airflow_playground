FROM apache/airflow:2.9.0
COPY requirements.txt .
COPY gcp_creds.json .
ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/gcp_creds.json"
RUN pip install -r requirements.txt
