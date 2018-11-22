import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.dataproc_operator import(
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator)

from airflow_training.operators.http_to_gcs import HttpToGcsOperator

args = {"owner": "Loek",
        "start_date": airflow.utils.dates.days_ago(3)}

project_id = "611e4210f1606867"

http_end_point = "https://europe-west1-gdd-airflow-training.cloudfunctions.net/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=EUR"

bucket = "airflow-training-data-loek"

dag = DAG(
    dag_id="exercise4",
    default_args=args,
    schedule_interval="0 0 * * *"
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pgsl_to_gcs",
    sql ="""SELECT * FROM 
    land_registry_price_paid_uk WHERE 
    transfer_date = '{{ ds }}'""",
    bucket=bucket,
    filename="land_registry_price_paid_uk/daily_load_{{ ds }}.json",
    postgres_conn_id="postgres_table",
    dag=dag
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    dag=dag,
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=project_id,
    num_workers=2,
    zone="europe-west4-a"
)

compute_aggregates = DataProcPySparkOperator(
    dag=dag,
    main="../other/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=["{{ ds }}"]
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    dag=dag,
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=project_id,
    trigger_rule=TriggerRule.ALL_DONE
)

http_to_gcs = HttpToGcsOperator(
    dag=dag,
    endpoint=http_end_point,
    gcs_bucket=bucket,
    gcs_path="exchange_rates/"
)


[pgsl_to_gcs, dataproc_create_cluster, http_to_gcs] >> compute_aggregates >> dataproc_delete_cluster
