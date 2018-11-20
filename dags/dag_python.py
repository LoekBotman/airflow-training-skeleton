import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_exec_date(execution_date, **context):
    print(execution_date)


dag = DAG(
    dag_id="hello_airflow_python",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)

PythonOperator(
    task_id="print_exec_date",
    python_callable=print_exec_date,
    provide_context=True,
    dag=dag
)
