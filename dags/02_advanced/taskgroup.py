'''
This shows how Taskgroups show up in the airflow UI
The actual tasks do nothing at all.
This example also shows how to override a task_id
'''

from datetime import datetime

from airflow.decorators import dag, python_task
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
}


@dag(
    dag_id="taskgroup_example",
    default_args=default_args,
    schedule_interval="* * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def taskgroup_example():
    @python_task
    def do_nothing() -> None:
        return

    first_task = do_nothing()
    join_task = do_nothing()
    with TaskGroup(group_id='parallel_group') as _:
        first_task >> do_nothing.override(task_id='p1')() >> join_task
        first_task >> do_nothing.override(task_id='p2')() >> join_task
        first_task >> do_nothing.override(task_id='p3')() >> join_task
        first_task >> do_nothing.override(task_id='p4')() >> join_task

    with TaskGroup(group_id='sequential_group') as _:
        s_1 = do_nothing.override(task_id='s1')()
        s_2 = do_nothing.override(task_id='s2')()
        s_3 = do_nothing.override(task_id='s3')()
        join_task >> s_1 >> s_2 >> s_3


taskgroup_example()
