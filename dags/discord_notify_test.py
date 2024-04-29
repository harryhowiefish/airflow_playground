from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json


class Simple_DC_Hook:
    def __init__(self, url: str):
        self.url = url

    def post(
        self,
        content: str = None,
        username: str = None,
        avatar_url: str = None,
        embeds: str = None,
    ):
        if content is None:
            raise ValueError("required one of content, file, embeds")
        data = {}
        if content:
            data["content"] = content
        if username:
            data["username"] = username
        if avatar_url:
            data["avatar_url"] = avatar_url
        if embeds:
            data["embeds"] = embeds
        return requests.post(
            self.url, {"payload_json": json.dumps(data)}
        )


def notify(url: str, msg: str):
    bot = Simple_DC_Hook(url)
    bot.post(content=msg)
    return


default_args = {
    'owner': 'airflow',
}

dc_notify_test = DAG(
    dag_id='dc_notify_test',
    default_args=default_args,
    start_date=datetime.today(),
    schedule_interval='0 0 1 * *',
    catchup=False
)

start_task = BashOperator(
        task_id='nothing_task',
        bash_command='sleep 3',  # noqa
        dag=dc_notify_test,
    )


email = PythonOperator(
    task_id='discord_notify',
    python_callable=notify,
    op_kwargs={'url': 'https://discord.com/api/webhooks/1232248158143385610/k6sU8xPWEAC7_cqPOR-QvSLOZgpKvdKrMQHYZs6gNxi-XQx10meYgWtErxlCQ0mZzMUh',  # noqa
               'msg': 'This message is sent from airflow'},
    dag=dc_notify_test,
)

start_task >> email
