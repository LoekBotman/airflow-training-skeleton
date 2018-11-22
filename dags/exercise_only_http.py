import airflow

from dags.airflow_training.operators.http_to_gcs import HttpToGcsOperator

args = {"owner": "Loek",
        "start_date": airflow.utils.dates.days_ago(3)}

http_end_point = "https://europe-west1-gdd-airflow-training.cloudfunctions.net/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=EUR"

bucket = "airflow-training-data-loek"

dag = DAG(
    dag_id="exercise_only_http",
    default_args=args,
    schedule_interval="0 0 * * *"
)

http_to_gcs = HttpToGcsOperator(
    dag=dag,
    endpoint=http_end_point,
    gcs_bucket=bucket,
    gcs_path="exchange_rates/"
)

http_to_gcs
