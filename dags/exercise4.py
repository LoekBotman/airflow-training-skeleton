import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator


args = {"owner": "Loek",
        "start_date": airflow.utils.dates.days_ago(3)}

dag = DAG(
    dag_id="exercise4",
    default_args=args,
    schedule_interval="0 0 * * *"
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pgsl_to_gcs",
    sql = """SELECT * FROM 
    land_registry_price_paid_uk WHERE 
    transfer_date = '{{ ds }}'""",
    bucket = "airflow-training-data-loek/pgsl_to_gcs",
    filename = "daily_load_{{ ds }}.json",
    posgres_conn_id="postgres_table",
    dag=dag
)

pgsl_to_gcs