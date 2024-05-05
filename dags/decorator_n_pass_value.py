'''
The new kind of decorator based format is called Taskflow
'''

from airflow.decorators import dag, python_task, bash_task
import pendulum


default_args = {
    'owner': 'airflow'
}


@dag(
    description='',
    schedule='* * * * *',
    start_date=pendulum.today(tz="Asia/Taipei"),
    catchup=False,
    on_success_callback=None,
    on_failure_callback=None,
    default_args=default_args
)
def pass_value():
    @python_task
    def task1() -> str:
        return 'output_from_task1'

    @python_task
    def task2(data: str) -> str:
        print(f'Recieved {data}')
        return 'output_from_task2'

    @bash_task
    def task3(data: str) -> None:
        return f"echo 'I got this from task2 {data}!'"

    t1_output = task1()
    t2_output = task2(t1_output)
    task3(t2_output)


pass_value()
