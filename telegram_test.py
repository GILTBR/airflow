from datetime import timedelta

from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago

from telegram import TelegramOperator

default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2),
                'depends_on_past': False,
                'email': ['giltober@gmail.com']}

dag = DAG(
    dag_id='telegram_dag',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'bash']
)
message = 'HELLO GIL'
telegram = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')), send_to=Variable.get('TELEGRAM_USER'),
                            msg=message, task_id='telegram', dag=dag)

if __name__ == "__main__":
    dag.cli()
