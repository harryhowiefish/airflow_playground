from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator


default_args = {
    'owner': 'airflow',
    "email": [],  # fill in email here
    "email_on_failure": True,
    "email_on_retry": True,
}

smtp_test = DAG(
    dag_id='smtp_test',
    default_args=default_args,
    start_date=datetime.today(),
    schedule_interval='0 0 1 * *',
    catchup=False
)

start_task = BashOperator(
        task_id='nothing_task',
        bash_command='sleep 3',  # noqa
        dag=smtp_test,
    )


email = EmailOperator(
    task_id='send_email',
    to='',  # fill in email here
    subject='smtp_test success',
    html_content="""<h3>Email Test</h3>""",
    dag=smtp_test,
)

start_task >> email
