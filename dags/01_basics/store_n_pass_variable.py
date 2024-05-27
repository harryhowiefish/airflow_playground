'''
Sample code for setting and getting Variables
Variables are global configuration that can be used
across different DAGs and DAG runs.
When multiple value is stored, json format is preferred to limit
number of connections.
'''


from airflow.decorators import python_task, dag
from airflow.models import Variable
import pendulum
from random import randint


default_args = {
    'owner': 'airflow'
}


@dag(
    schedule='* * * * *',
    start_date=pendulum.today(tz="Asia/Taipei"),
    catchup=False,
    default_args=default_args
)
def set_and_gen_variable():

    @python_task
    def get_variable():
        var = Variable.get('random_value', default_var=None)
        print(f'Stored value is {var}')

    @python_task
    def gen_variable():
        new_var = randint(0, 10000)
        Variable.set('random_value', {'value': new_var},
                     serialize_json=True)
        print(f'New value is {new_var}')
        pass

    t1 = get_variable()
    t2 = gen_variable()
    t1 >> t2


set_and_gen_variable()
