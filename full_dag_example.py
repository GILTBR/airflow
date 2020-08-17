from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from telegram import TelegramOperator

default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2),
                'depends_on_past': False,
                'email': ['giltober@gmail.com']}

dag = DAG(
    dag_id='full_dag_example',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'postgres', 'telegram']
)

create_table = PostgresOperator(sql='sql.sql', postgres_conn_id='postgres_prod', autocommit=True, database='postgre',
                                task_id='create_table', dag=dag)

on_fail_telegram_message = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')),
                                            send_to=Variable.get('TELEGRAM_USER'),
                                            msg=f'{datetime.now()}: table creations failed',
                                            task_id='on_fail_telegram_message', dag=dag, trigger_rule='all_failed')

on_success_telegram_message = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')),
                                               send_to=Variable.get('TELEGRAM_USER'),
                                               msg=f'{datetime.now()}: table created successfully',
                                               task_id='on_success_telegram_message',
                                               dag=dag, trigger_rule='all_success')

create_table >> on_fail_telegram_message
create_table >> on_success_telegram_message

if __name__ == "__main__":
    dag.cli()
