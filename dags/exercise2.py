import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_exec_date(execution_date, **context):
    print(execution_date)


args = {"owner": "Loek",
        "start_date": airflow.utils.dates.dates_ago(14)}

dag = DAG(
    dag_id="exercise2",
    default_args=args
)

print_execution_date_task = PythonOperator(
    task_id="print_exec_date",
    python_callable=print_exec_date,
    provide_context=True,
    dag=dag
)

wait_5 = BashOperator(
    task_id="wait_5",
    bash_command="sleep 5",
    dag=dag
)

wait_10 = BashOperator(
    task_id="wait_10",
    bash_command="sleep 10",
    dag=dag
)

wait_1 = BashOperator(
    task_id="wait_1",
    bash_command="sleep 1",
    dag=dag
)

the_end = DummyOperator(
    task_id="the_end",
    dag=dag
)

print_execution_date_task >> [wait_1, wait_5, wait_10] >> the_end