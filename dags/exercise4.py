import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import(
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator)


args = {"owner": "Loek",
        "start_date": airflow.utils.dates.days_ago(3)}

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
    bucket="airflow-training-data-loek",
    filename="pgsl_to_gcs/daily_load_{{ ds }}.json",
    postgres_conn_id="postgres_table",
    dag=dag
)

# dataproc_create_cluster = DataprocClusterCreateOperator(
# #     cluster_name="analyse-pricing-{{ ds }}",
# #     project_id="Training Boldotcom - lbotman",
# #     num_workers=2,
# #     zone="europe-west4-a",)
# #
# # compute_aggregates = DataProcPySparkOperator(
# #     main="../other/build_statistics.py",
# #     cluster_name="analyse-pricing-{{ ds }}",
# #     arguments=["{{ ds }}"],)
# #
# #
# # dataproc_delete_cluster = DataprocClusterDeleteOperator(
# #     cluster_name="analyse-pricing-{{ ds }}")

pgsl_to_gcs