from datetime import timedelta

from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2),
                'depends_on_past': False,
                'email': ['giltober@gmail.com']}

dag = DAG(
    dag_id='simple_email_dag',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'bash']
)

email_dag = EmailOperator('email_dag', dag=dag, to='giltober@gmail.com', subject='Airflow test', html_content='test')

if __name__ == "__main__":
    dag.cli()
