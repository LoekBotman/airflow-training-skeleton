import airflow
from airflow import DAG


args = {
    "owner": "Loek",
    "start_date", airflow.utils.dates.days_ago(3)
}


dag = DAG(
    dag_id="exercise4",
    default_args=args,
    schedule_interval="0 0 * * *"
)

pgsl_to_gcs = PostGresToGoogleCloudStorageOperator(
    task_id="pgsl_to_gcs",
    sql = f"""SELECT * FROM 
    land_registry_price_paid_uk WHERE 
    transfer_date = '{ds}'""",
    bucket = "airflow-training-data-loek",
    filename = f"{ds}.json",
    posgres_conn_id="postgres_table",
    dag=dag
)

pgsl_to_gcs