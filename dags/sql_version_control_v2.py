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
DAG_NAME = 'sql_version_control_v2'
SCHEDULE = None
DESCRIPTION = 'Version 2: Dynamically create DAGs based on the number of connections that meat the criteria'

# Constant variables
VERSION = DAG_NAME.split('_')[-1]
SQL_MAIN_FOLDER = str(Variable.get('SQL_FOLDER_PATH'))
SQL_FUNCTIONS_FOLDER = f'{SQL_MAIN_FOLDER}/{VERSION}'
LOCAL_TZ = pendulum.timezone('Asia/Jerusalem')

# Default DAG arguments
default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2), 'depends_on_past': False,
                'email': ['giltober@gmail.com'], 'email_on_failure': False}


def on_success_callback_telegram(context):
    message = f"\U00002705 DAG successful!\nDAG: {context.get('task_instance').dag_id}\nExecution Time: " \
              f"{context.get('execution_date').replace(microsecond=0, tzinfo=LOCAL_TZ)}\nLog URL:\n" \
              f"{context.get('task_instance').log_url}"
    success_alert = TelegramOperator(telegram_conn_id='telegram_conn_id', task_id='telegram_success', message=message)
    return success_alert.execute(context=context)


def on_failure_callback_telegram(context):
    message = f"\U0000274C Task Failed!\nDAG: {context.get('task_instance').dag_id}\nTask: " \
              f"{context.get('task_instance').task_id}\nExecution Time: " \
              f"{context.get('execution_date').replace(microsecond=0, tzinfo=LOCAL_TZ)}\nLog URL:\n" \
              f"{context.get('task_instance').log_url}"
    failed_alert = TelegramOperator(telegram_conn_id='telegram_conn_id', task_id='telegram_failed', message=message)
    return failed_alert.execute(context=context)


# Main DAG creation
def create_dag(f_dag_id, f_schedule, f_default_args, f_conn_id):
    with DAG(dag_id=f_dag_id, description=DESCRIPTION, default_args=f_default_args,
             template_searchpath=f'{SQL_MAIN_FOLDER}', schedule_interval=f_schedule,
             dagrun_timeout=dt.timedelta(minutes=60),
             on_failure_callback=on_failure_callback_telegram, on_success_callback=on_success_callback_telegram) as dag:
        # On failure send telegram message
        git_pull = BashOperator(task_id='git_pull', bash_command=f'cd {SQL_MAIN_FOLDER}; git pull',
                                on_failure_callback=on_failure_callback_telegram)

        dummy_start = DummyOperator(task_id='dummy_start')
        dummy_end = DummyOperator(task_id='dummy_end')

        git_pull >> dummy_start

        # Loop over the SQL files folder to create tasks
        # On failure send telegram message
        for file in os.listdir(SQL_FUNCTIONS_FOLDER):
            file_name = file.split('.')[0]
            sql_function = PostgresOperator(task_id=f'sql_{file_name}', postgres_conn_id=str(f_conn_id[0]),
                                            sql=f'{VERSION}/{file}', autocommit=True)
            dummy_start >> sql_function >> dummy_end

    return dag


# Create session to Airflow DB to get connections available
session = settings.Session()
connections = (session.query(Connection.conn_id).filter(Connection.conn_id.like('db_%')).all())

# dynamically create DAGs based on connections
for db_conn in connections:
    dag_id = f'{DAG_NAME}_{db_conn[0]}'
    default_args = default_args
    schedule = SCHEDULE
    globals()[dag_id] = create_dag(dag_id, schedule, default_args, db_conn)
