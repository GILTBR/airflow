import datetime as dt
import os

import pendulum
from airflow import DAG, settings
from airflow.models import Variable, Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.telegram_plugin import TelegramOperator
from airflow.utils.dates import days_ago

# Main DAG info
DAG_NAME = 'sql_version_control_v3'
SCHEDULE = None
DESCRIPTION = 'This DAG is used to control versioning sql functions and procedures on a giving database. ' \
              'Version 3: 1 Dag with multiple DBs and 2 Folders that each contains several .sql files.'

# Constant variables
VERSION = DAG_NAME.split('_')[-1]
SQL_MAIN_FOLDER = str(Variable.get('SQL_FOLDER_PATH'))
SQL_FUNCTIONS_FOLDER = f'{SQL_MAIN_FOLDER}/{VERSION}'
LOCAL_TZ = pendulum.timezone('Asia/Jerusalem')

default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2), 'depends_on_past': False,
                'email': ['giltober@gmail.com'], 'email_on_failure': False}

bash_command = f'cd {SQL_MAIN_FOLDER}; git pull'

session = settings.Session()
conns = (session.query(Connection.conn_id).filter(Connection.conn_id.like('db_%')).all())

with DAG(dag_id=DAG_NAME, description=DESCRIPTION, default_view='graph', default_args=default_args,
         template_searchpath=f'{SQL_MAIN_FOLDER}', schedule_interval=SCHEDULE,
         dagrun_timeout=dt.timedelta(minutes=60),
         tags=['git', 'sql']) as dag:
    git_pull = BashOperator(task_id='git_pull', bash_command=bash_command)
    dummy1 = DummyOperator(task_id='dummy1')
    dummy2 = DummyOperator(task_id='dummy2')

    for db_conn in conns:
        git_pull >> dummy1
        dummy3 = DummyOperator(task_id='dummy3')
        dummy4 = DummyOperator(task_id='dummy4')

        for file in os.listdir(SQL_FUNCTIONS_FOLDER):
            file_name = file.split('.')[0]
            delete_sql = PostgresOperator(task_id=f'sql_({db_conn[0]})_{file_name}', postgres_conn_id=db_conn[0],
                                          sql=f'{VERSION}/delete/{file}', autocommit=True)
            dummy1 >> delete_sql >> dummy3

    on_fail_telegram_message = TelegramOperator(telegram_conn_id='telegram_conn_id',
                                                message=f'{dt.datetime.now().replace(microsecond=0)}: {DAG_NAME} failed'
                                                , task_id='on_fail_telegram_message', trigger_rule='all_failed')
    on_success_telegram_message = TelegramOperator(telegram_conn_id='telegram_conn_id',
                                                   message=f'{dt.datetime.now().replace(microsecond=0)}: {DAG_NAME} '
                                                           f'successful',
                                                   task_id='on_success_telegram_message', trigger_rule='all_success')

dummy3 >> dummy2
dummy2 >> on_fail_telegram_message
dummy2 >> on_success_telegram_message
if __name__ == "__main__":
    dag.cli()
