'''
https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
'''

from datetime import datetime

from airflow.decorators import dag, python_task

default_args = {
    "owner": "airflow",
}


@dag(
    dag_id="task_mapping",
    default_args=default_args,
    schedule_interval="* * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def task_mapping():
    @python_task
    def generate_some_str() -> str:
        return "HELLO"

    @python_task
    def split_str_to_list(some_str: str) -> list[str]:
        return list(some_str)

    @python_task
    def print_something_separately(something: str | list) -> None:
        print("======")
        print(something)
        print("======")

    some_str = generate_some_str()
    result = split_str_to_list(some_str)
    # with TaskGroup(group_id='do_something_task_group') as _:
    # Dynamically create task instances for processing
    print_something_separately.expand(something=result)


# Instantiate the DAG
task_mapping()
