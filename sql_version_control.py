import datetime as dt
import os

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from telegram import TelegramOperator

# Main DAG info
DAG_NAME = 'sql_version_control'
SCHEDULE = None
DESCRIPTION = 'This DAG is used to control versioning SQL functions and procedures on a giving database.' \
              'The functions and procedures should be saves on a git repo'

# Constant variables
SQL_MAIN_FOLDER = str(Variable.get('sql_folder_path'))
SQL_TRUNCATE_FOLDER = f'{SQL_MAIN_FOLDER}/truncate'
SQL_CREATE_FOLDER = f'{SQL_MAIN_FOLDER}/create'

default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2), 'depends_on_past': False,
                'email': ['giltober@gmail.com']}

bash_command = f'cd {SQL_MAIN_FOLDER}; git pull'

with DAG(dag_id=DAG_NAME, description=DESCRIPTION, default_view='graph', default_args=default_args,
         schedule_interval=SCHEDULE, dagrun_timeout=dt.timedelta(minutes=60), tags=['git', 'sql']) as dag:
    git_pull = BashOperator(task_id='git_pull', bash_command=bash_command)

    dummy = DummyOperator(task_id='dummy')

    for file in os.listdir(SQL_TRUNCATE_FOLDER):
        truncate_sql = PostgresOperator(task_id=f'truncate_sql({file})', postgres_conn_id='postgres_prod', sql=file,
                                        autocommit=True)
        git_pull >> truncate_sql

    for file in os.listdir(SQL_CREATE_FOLDER):
        create_sql = PostgresOperator(task_id=f'create_sql({file})', postgres_conn_id='postgres_prod', sql=file,
                                      autocommit=True)
        truncate_sql >> create_sql >> dummy

    on_fail_telegram_message = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')),
                                                send_to=Variable.get('TELEGRAM_USER'),
                                                msg=f'{dt.datetime.now().replace(microsecond=0)}: {DAG_NAME} failed',
                                                task_id='on_fail_telegram_message', trigger_rule='all_failed')

    on_success_telegram_message = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')),
                                                   send_to=Variable.get('TELEGRAM_USER'),
                                                   msg=f'{dt.datetime.now().replace(microsecond=0)}: {DAG_NAME} '
                                                       f'successful',
                                                   task_id='on_success_telegram_message', trigger_rule='all_success')

dummy >> on_fail_telegram_message
dummy >> on_success_telegram_message
if __name__ == "__main__":
    dag.cli()
