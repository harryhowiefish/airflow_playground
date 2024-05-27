
# import os
from airflow.decorators import python_task, dag
import time
import pendulum
import socket
from celery import current_task
default_args = {
    'owner': 'airflow'
}


@dag(
    dag_id='show_worker_id',
    default_args=default_args,
    schedule_interval='* * * * *',
    start_date=pendulum.today(tz="Asia/Taipei")
    # catchup=False
)
def show_worker_and_sleep():

    @python_task
    def get_hostname():
        hostname = socket.gethostname()
        print(f"The hostname of the worker is: {hostname}")
        return hostname

    @python_task
    def get_worker_name():
        # Accessing the Celery task context to retrieve the worker name
        print(dir(current_task.request))
        worker_name = current_task.request.hostname
        print(f"The worker name is: {worker_name}")
        return worker_name

    @python_task
    def sleep():
        time.sleep(30)

    # show_worker >> sleep
    t1 = get_hostname()
    t2 = get_worker_name()
    t3 = sleep()
    t1 >> t2
    t2 >> t3


show_worker_and_sleep()
