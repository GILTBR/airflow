import datetime as dt

import pandas as pd
import requests
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

from telegram import TelegramOperator

default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2),
                'depends_on_past': False,
                'email': ['giltober@gmail.com']}

dag = DAG(
    dag_id='full_dag_example',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=dt.timedelta(minutes=60),
    tags=['example', 'postgres', 'telegram']
)


def to_usd(rate, base=0):
    return round(base * (1 / rate), 20)


def pull_exchange_rate():
    today = dt.date.today()
    delta = dt.timedelta(days=1)
    rate_date = today - delta

    req = requests.get(f"http://data.fixer.io/api/{rate_date}?access_key={str(Variable.get('EXCHANGE_KEY'))}&base=EUR")

    if req.json()['success']:
        con = create_engine(str(Variable.get('AIRFLOW_CONN_POSTGRES_PROD')))
        usd_currency = req.json()['rates']['USD']

        df = pd.DataFrame(req.json()).drop(['success', 'base', 'timestamp', 'historical'], axis=1).reset_index().rename(
            columns={'index': 'code', 'rates': 'rate'})

        df['rate'] = df['rate'].apply(to_usd, base=usd_currency)
        df = df[['code', 'rate', 'date']].astype({'date': 'datetime64'})

        df.to_sql('usd_exchange_rates', con=con, schema='prod', if_exists='append', index=False, chunksize=200,
                  method='multi')


create_table = PostgresOperator(sql='create_table.sql', postgres_conn_id='postgres_prod', autocommit=True,
                                database='postgres', task_id='create_table', dag=dag)

pull_exchange = PythonOperator(python_callable=pull_exchange_rate, task_id='pull_exchange', dag=dag)

on_fail_telegram_message = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')),
                                            send_to=Variable.get('TELEGRAM_USER'),
                                            msg=f'{dt.datetime.now().replace(microsecond=0)}: API call failed',
                                            task_id='on_fail_telegram_message', dag=dag, trigger_rule='all_failed')

on_success_telegram_message = TelegramOperator(bot_token=str(Variable.get('TELEGRAM_TOKEN')),
                                               send_to=Variable.get('TELEGRAM_USER'),
                                               msg=f'{dt.datetime.now().replace(microsecond=0)}: API call successful',
                                               task_id='on_success_telegram_message', dag=dag,
                                               trigger_rule='all_success')

create_table >> pull_exchange
pull_exchange >> on_fail_telegram_message
pull_exchange >> on_success_telegram_message

if __name__ == "__main__":
    dag.cli()
