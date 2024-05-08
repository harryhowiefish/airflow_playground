from datetime import datetime
from airflow.decorators import dag, python_task
from utils.discord_notifier import Simple_DC_Notifier
default_args = {
    'owner': 'airflow',
}


@dag(
    dag_id='discord_base_notifier',
    default_args=default_args,
    start_date=datetime.today(),
    on_success_callback=Simple_DC_Notifier('success!'),
    schedule_interval='0 0 1 * *',
    catchup=False
)
def discord_base_notifier():

    @python_task
    def do_nothing():
        print('just a test')
        return

    do_nothing()


discord_base_notifier()
