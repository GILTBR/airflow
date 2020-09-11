import datetime as dt

import pendulum
from airflow import DAG, settings
from airflow.models import Variable, Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.telegram_plugin import TelegramOperator
from airflow.utils.dates import days_ago
from git import Repo

# Main DAG info
DAG_NAME = 'sql_version_control_v4'
SCHEDULE = None
DESCRIPTION = ''

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


session = settings.Session()
conns = (session.query(Connection.conn_id).filter(Connection.conn_id.like('db_%')).all())

# Main DAG creation
with DAG(dag_id=DAG_NAME, description=DESCRIPTION, default_args=default_args, template_searchpath=f'{SQL_MAIN_FOLDER}',
         schedule_interval=SCHEDULE, dagrun_timeout=dt.timedelta(minutes=60),
         on_failure_callback=on_failure_callback_telegram, on_success_callback=on_success_callback_telegram) as dag:
    repo = Repo(path='/home/giltober/airflow/airflow-dags')
    full_diff = repo.git.diff('HEAD~1..HEAD', name_only=True).split('\n')
    diff_files = [file.replace(f'sql/{VERSION}/', '') for file in full_diff if file.startswith(f'sql/{VERSION}')]

    # On failure send telegram message
    git_pull = BashOperator(task_id='git_pull', bash_command=f'cd {SQL_MAIN_FOLDER}; git pull',
                            on_failure_callback=on_failure_callback_telegram)

    dummy_start = DummyOperator(task_id='dummy_start')
    dummy_end = DummyOperator(task_id='dummy_end')

    git_pull >> dummy_start

    for db_conn in conns:
        dummy_db_start = DummyOperator(task_id=f'dummy_start_{db_conn[0]}')
        dummy_db_end = DummyOperator(task_id=f'dummy_end_{db_conn[0]}')
        dummy_start >> dummy_db_start

        for file in diff_files:
            file_name = file.split('.')[0]
            sql_function = PostgresOperator(task_id=f'sql_{db_conn[0]}_{file_name}', postgres_conn_id=db_conn[0],
                                            sql=f'{VERSION}/{file}', autocommit=True)
            dummy_db_start >> sql_function >> dummy_db_end

        dummy_db_end >> dummy_end

if __name__ == "__main__":
    dag.cli()
