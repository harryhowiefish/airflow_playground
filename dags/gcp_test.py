from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

default_args = {
    'owner': 'airflow'
}


def print_bucket(bucket_name: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    for file in bucket.list_blobs():
        print(file.name)
    return


gcp_test = DAG(
    dag_id='gcp_test',
    default_args=default_args,
    # schedule_interval='0 0 1 * *',
    # catchup=False
)

list_bucket_object = PythonOperator(
    dag=gcp_test,
    task_id='list_bucket_object',
    python_callable=print_bucket,
    op_kwargs={'bucket_name': "tir101-group2"}
)
