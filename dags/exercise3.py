import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


weekday_person_to_email = {
    "Mon": "Bob",
    "Tue": "Joe",
    "Wed": "Alice",
    "Thu": "Joe",
    "Fri": "Alice",
    "Sat": "Alice",
    "Sun": "Alice"
}


args = {"owner": "Loek",
        "start_date": airflow.utils.dates.days_ago(14)}


days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


dag = DAG(
    dag_id="exercise3",
    default_args=args
)

print_weekday = PythonOperator(
    task_id="print_weekday",
    python_callable=lambda **context: print(context["execution_date"].strftime("ddd")),
    provide_context=True,
    dag=dag
)

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=lambda **context: f'email_{weekday_person_to_email[context["execution_date"].strftime("ddd")]}',
    provide_context=True,
    dag=dag
)

send_emails = [ PythonOperator(
    task_id=f"email_{name}",
    python_callable=lambda: print((f"{name}")),
    provide_context=False,
    dag=dag
) for name in ["Bob", "Joe", "Alice"]
]

final_task = DummyOperator(
    task_id="final_task",
    dag=dag,
    trigger_rule=TriggerRule.ONE_SUCCESS
)


print_weekday >> branching >> send_emails >> final_task