'''
https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
'''

from datetime import datetime

from airflow.decorators import dag, python_task

default_args = {
    "owner": "airflow",
}


@dag(
    dag_id="partial_n_expand",
    default_args=default_args,
    schedule_interval="* * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def partial_n_expand():
    @python_task
    def add(x: int, y: int):
        return x + y

    add.partial(y=10).expand(x=[1, 2, 3])


partial_n_expand()
