from airflow.decorators import dag, python_task
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
import pendulum
import datetime

default_args = {
    'owner': 'airflow'
}


@dag(
    dag_id="external_task_marker_parent",
    schedule_interval=datetime.timedelta(minutes=1),
    start_date=pendulum.today(tz="Asia/Taipei").subtract(days=1),
    catchup=False,
    default_args=default_args
)
def parent_dag():
    @python_task
    def setup():
        return "Starting parent DAG"

    # Mark the end of the DAG as a trigger for another DAG
    end_task = ExternalTaskMarker(
        task_id="end_task",
        external_dag_id="external_task_child",  # The DAG ID of the child DAG
        external_task_id="child_task_1"
    )

    setup() >> end_task


p_dag = parent_dag()


@dag(
    dag_id="external_task_child",
    start_date=pendulum.today(tz="Asia/Taipei").subtract(days=1),
    schedule_interval=datetime.timedelta(minutes=1),
    catchup=False,
    default_args=default_args

)
def child_dag():

    child_task_1 = ExternalTaskSensor(
        task_id="child_task_1",
        external_dag_id="external_task_marker_parent",
        external_task_id="end_task",
        allowed_states=["success"],
        check_existence=True,
    )

    @python_task
    def child_task_2():
        return "Working on some other task...."

    child_task_1 >> child_task_2()


child_dag()
