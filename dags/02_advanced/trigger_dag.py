
import pendulum

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import python_task, dag
import random
default_args = {
    'owner': 'airflow'
}


@dag(
    start_date=pendulum.today(tz="Asia/Taipei").subtract(days=1),
    catchup=False,
    schedule="* * * * *",
    tags=["TriggerDagRun"],
    default_args=default_args
)
def trigger_controller_dag():

    message = ''

    @python_task
    def choose_a_config(dag_run=None):
        global message
        config_list = ['a', 'b', 'c']
        message = random.choice(config_list)
        return message

    config_info = choose_a_config()

    trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        # Ensure this equals the dag_id of the DAG to trigger
        trigger_dag_id="trigger_target_dag",
        conf={"message": config_info},
    )

    config_info >> trigger


trigger_controller_dag()


@dag(
    start_date=pendulum.today(tz="Asia/Taipei").subtract(days=1),
    catchup=False,
    schedule=None,
    tags=["TriggerDagRun"],
)
def trigger_target_dag():

    @python_task
    def receiving_info(dag_run=None):
        print(
            f"Recieving {dag_run.conf.get('message')} through dag_run conf")

    @python_task
    def some_func():
        print('actually doing something.....')

    receiving_info() >> some_func()


trigger_target_dag()
