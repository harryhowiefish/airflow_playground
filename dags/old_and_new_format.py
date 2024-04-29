from airflow.decorators import dag, python_task, bash_task
import pendulum
from airflow.operators.bash import BashOperator
import time
default_args = {
    'owner': 'airflow'
}

'''
The old way to create the DAG object
can not be used along side the new format
'''
# old_dag = DAG(
#     dag_id='old_dag_format',
#     default_args=default_args,
#     schedule_interval='* * * * *',
#     catchup=False
# )

end_task = BashOperator(
        task_id='old_format_sleep',
        bash_command="echo 'IM AWAKE!!'",  # noqa
    )


@dag(
    description='',
    schedule='* * * * *',
    start_date=pendulum.today(tz="Asia/Taipei"),
    catchup=False,
    on_success_callback=None,
    on_failure_callback=None,
    default_args=default_args
)
def old_and_new_format():
    @python_task
    def task1() -> str:
        print('sleeping....')
        time.sleep(2)

    @bash_task
    def task2() -> None:
        return "sleep 2 & echo 'ALARM!!!'"

    t1 = task1()
    t2 = task2()
    t1 >> t2
    t2.set_downstream(end_task)


old_and_new_format()
