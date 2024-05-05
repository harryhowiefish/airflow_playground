
"""
Example DAG showing xcom
xcom is mostly replaced by the new taskflow format
and should only be used in special cases
"""


import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import TaskInstance


example_xcom = DAG(
    "example_xcom",
    schedule="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)


def push_1(ti: TaskInstance = None):
    data = [1, 2, 3]
    ti.xcom_push(key="value from pusher1", value=data)


def push_2(ti: TaskInstance = None):
    data = ['a', 'b', 'c']
    ti.xcom_push(key="value from pusher2", value=data)


def pull_one(ti: TaskInstance = None):
    pulled_value = ti.xcom_pull(task_ids="pusher1", key="value from pusher1")
    print(pulled_value)


def pull_everything(ti: TaskInstance = None):
    pulled_value = ti.xcom_pull(task_ids=None, key=None)
    print(pulled_value)


pusher1 = PythonOperator(
    task_id='pusher1',
    dag=example_xcom,
    python_callable=push_1
)

pusher2 = PythonOperator(
    task_id='pusher1',
    dag=example_xcom,
    python_callable=push_2
)

middle_nothing_task = EmptyOperator(
    task_id='do_nothing',
    dag=example_xcom,
)

puller = PythonOperator(
    task_id='puller',
    dag=example_xcom,
    python_callable=pull_one,
    provide_context=True,
)

indirect_puller = PythonOperator(
    task_id='indirect_puller',
    dag=example_xcom,
    python_callable=pull_everything,
    provide_context=True,
)

pusher1 >> middle_nothing_task
pusher2 >> middle_nothing_task
middle_nothing_task >> indirect_puller
pusher1 >> puller
