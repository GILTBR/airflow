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
DAG_NAME = 'sql_version_control_v1'
SCHEDULE = None
DESCRIPTION = 'This DAG is used to control versioning sql functions and procedures on a giving database. ' \
              'Version 1: 2 Folders that each contains several .sql files.'

# Constant variables
VERSION = DAG_NAME.split('_')[-1]
SQL_MAIN_FOLDER = str(Variable.get('sql_folder_path'))
SQL_DELETE_FOLDER = f'{SQL_MAIN_FOLDER}/{VERSION}/delete'
SQL_CREATE_FOLDER = f'{SQL_MAIN_FOLDER}/{VERSION}/create'

default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2), 'depends_on_past': False,
                'email': ['giltober@gmail.com'], 'email_on_failure': False}

bash_command = f'cd {SQL_MAIN_FOLDER}; git pull'

with DAG(dag_id=DAG_NAME, description=DESCRIPTION, default_view='graph', default_args=default_args,
         schedule_interval=SCHEDULE, dagrun_timeout=dt.timedelta(minutes=60), tags=['git', 'sql']) as dag:
    git_pull = BashOperator(task_id='git_pull', bash_command=bash_command)

    dummy1 = DummyOperator(task_id='dummy1')
    dummy2 = DummyOperator(task_id='dummy2')

    for file in os.listdir(SQL_DELETE_FOLDER):
        file_name = file.split('.')[0]
        delete_sql = PostgresOperator(task_id=f'delete_sql_{file_name}', postgres_conn_id='postgres_prod',
                                      sql=f'{SQL_DELETE_FOLDER}/{file}', autocommit=True)
        git_pull >> delete_sql >> dummy1

    for file in os.listdir(SQL_CREATE_FOLDER):
        file_name = file.split('.')[0]
        create_sql = PostgresOperator(task_id=f'create_sql_{file_name}', postgres_conn_id='postgres_prod',
                                      sql=f'{SQL_CREATE_FOLDER}/{file}', autocommit=True)
        dummy1 >> create_sql >> dummy2

    on_fail_telegram_message = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')),
                                                send_to=Variable.get('TELEGRAM_USER'),
                                                msg=f'{dt.datetime.now().replace(microsecond=0)}: {DAG_NAME} failed',
                                                task_id='on_fail_telegram_message', trigger_rule='all_failed')

    on_success_telegram_message = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')),
                                                   send_to=Variable.get('TELEGRAM_USER'),
                                                   msg=f'{dt.datetime.now().replace(microsecond=0)}: {DAG_NAME} '
                                                       f'successful',
                                                   task_id='on_success_telegram_message', trigger_rule='all_success')

dummy2 >> on_fail_telegram_message
dummy2 >> on_success_telegram_message
if __name__ == "__main__":
    dag.cli()
