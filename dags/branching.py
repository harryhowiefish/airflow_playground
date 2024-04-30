"""
Sample code for branching
"""

import random
import pendulum
from airflow.decorators import dag, branch_task, python_task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="branching_demo",
    start_date=pendulum.today(tz="Asia/Taipei"),
    catchup=False,
    schedule="* * * * *",
)
def five_branch_branching():

    options = ["a", "b", "c", "d"]

    @branch_task
    def branching(choices: list[str]) -> str:
        # this has to match the next task name
        return f"branch_{random.choice(choices)}"

    random_choice_instance = branching(choices=options)

    @python_task(
    )
    def finally_run():
        print('this will be run regardless of the branch')

    join = finally_run()
    for option in options:

        @python_task(
            # this has to match the previous branching task name
            task_id=f"branch_{option}"
        )
        def some_task():
            print("only one of the branch will run!")
        t = some_task()

        empty = EmptyOperator(task_id=f"next_step_in_{option}")

        random_choice_instance >> t >> empty >> join


five_branch_branching()
