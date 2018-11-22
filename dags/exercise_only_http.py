import airflow
from airflow import DAG
from airflow_training.operators.http_to_gcs import HttpToGcsOperator

args = {"owner": "Loek",
        "start_date": airflow.utils.dates.days_ago(3)}

http_end_point = "airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=EUR"

bucket = "airflow-training-data-loek"

dag = DAG(
    dag_id="exercise_only_http",
    default_args=args,
    schedule_interval="0 0 * * *"
)

http_to_gcs = HttpToGcsOperator(
    task_id="http_to_gcs",
    dag=dag,
    endpoint=http_end_point,
    gcs_bucket=bucket,
    gcs_path="exchange_rates/"
)

http_to_gcs
