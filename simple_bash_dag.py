from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {'owner': 'Gil Tober', 'start_date': days_ago(2),
                'depends_on_past': False,
                'email': ['giltober@gmail.com']}

dag = DAG(
    dag_id='simple_bash_dag',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'bash']
)

bash_dag = BashOperator(task_id='bash_dag', bash_command='echo TEST_TEST_TEST', dag=dag)

if __name__ == "__main__":
    dag.cli()
