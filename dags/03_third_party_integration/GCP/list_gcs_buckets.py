'''
This dag lists all the bucket in the target project
GOOGLE_APPLICATION_CREDENTIALS env is required for this to work.
if GOOGLE_APPLICATION_CREDENTIALS is not setup correctly
please checkout gcp_load_crediential.py for more information.

此 DAG 列出了目標專案中的所有Bucket。
需要設定 GOOGLE_APPLICATION_CREDENTIALS 環境變數才能使其正常運行。
如果 GOOGLE_APPLICATION_CREDENTIALS尚未設置，
請查看 gcp_load_crediential.py 以獲取更多資訊。

'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

default_args = {
    'owner': 'airflow'
}


def list_buckets() -> None:
    storage_client = storage.Client()
    buckets: list[storage.Bucket] = storage_client.list_buckets()
    for bucket in buckets:
        print(bucket.name)
    return


gcs_client = DAG(
    dag_id='gcs_client',
    default_args=default_args,
    # schedule_interval='0 0 1 * *',
    # catchup=False
)

list_bucket_object = PythonOperator(
    dag=gcs_client,
    task_id='list_buckets',
    python_callable=list_buckets,
)
