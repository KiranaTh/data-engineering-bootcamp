import csv
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils import timezone

doc_md = """
## 🚀 Extract data from postgres then load to GCS. Finally, load data from GCS to Bigquery

### Prerequisites

#### Connections

1. **networkrail_postgres_conn** [conn_type=`Postgres`, Host=`34.87.139.82`, Port=`5435`, Schema=`networkrail`]
1. **storage_and_bq_airflow** [conn_type=`Google Cloud`, keyfile_json=`YOUR_SERVICE_ACCOUNT_JSON`]
"""


DAGS_FOLDER = "/opt/airflow/dags"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
PROJECT_ID = "liquid-optics-384501"
GCS_BUCKET = "deb-bootcamp-100005"
BIGQUERY_DATASET = "networkrail"


def _extract_data(**context):
    ds = context["data_interval_start"].to_date_string()

    pg_hook = PostgresHook(
        postgres_conn_id="networkrail_postgres_conn",
        schema="networkrail"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f"""
        select * from movements where date(actual_timestamp) = '{ds}'
    """
    logging.info(sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    if rows:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "event_type",
                "gbtt_timestamp",
                "original_loc_stanox",
                "planned_timestamp",
                "timetable_variation",
                "original_loc_timestamp",
                "current_train_id",
                "delay_monitoring_point",
                "next_report_run_time",
                "reporting_stanox",
                "actual_timestamp",
                "correction_ind",
                "event_source",
                "train_file_address",
                "platform",
                "division_code",
                "train_terminated",
                "train_id",
                "offroute_ind",
                "variation_status",
                "train_service_code",
                "toc_id",
                "loc_stanox",
                "auto_expected",
                "direction_ind",
                "route",
                "planned_event_type",
                "next_report_stanox",
                "line_ind",
            ]
            writer.writerow(header)
            for row in rows:
                logging.info(row)
                writer.writerow(row)

        return "load_data_to_gcs"
    else:
        return "do_nothing"


def _load_data_to_gcs(**context):
    ds = context["data_interval_start"].to_date_string()
    gcs_path_file = f"{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
    hook = GCSHook(gcp_conn_id='storage_and_bq_airflow')
    hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=gcs_path_file,
        filename=f"{DAGS_FOLDER}/{DATA}-{ds}.csv"
    )


def _load_data_from_gcs_to_bigquery(**context):
    ds = context["data_interval_start"].to_date_string()

    # Your code here

    pass


default_args = {
    "owner": "YOUR_NAME",
    "start_date": timezone.datetime(2023, 5, 1),
}
with DAG(
    dag_id="networkrail_movements_to_gcs_and_then_bigquery",
    default_args=default_args,
    doc_md=doc_md,
    schedule="@hourly",  # Set the schedule here
    catchup=False,
    tags=["DEB", "2023", "networkrail"],
    max_active_runs=3,
):

    # Start
    start = EmptyOperator(task_id="start")

    # Extract data from NetworkRail Postgres Database
    extract_data = BranchPythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    # Do nothing
    do_nothing = EmptyOperator(task_id="do_nothing")

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="load_data_from_gcs_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=f"{BUSINESS_DOMAIN}/{DATA}/{{{{ds}}}}/{DATA}.csv",
        source_format="csv",
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.{DATA}",
        schema_fields=[
            {"name": "event_type", "mode": "NULLABLE", "type": "STRING"},
            {"name": "gbtt_timestamp", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "original_loc_stanox", "mode": "NULLABLE", "type": "STRING"},
            {"name": "planned_timestamp", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "timetable_variation", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "original_loc_timestamp", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "current_train_id", "mode": "NULLABLE", "type": "STRING"},
            {"name": "delay_monitoring_point", "mode": "NULLABLE", "type": "BOOLEAN"},
            {"name": "next_report_run_time", "mode": "NULLABLE", "type": "STRING"},
            {"name": "reporting_stanox", "mode": "NULLABLE", "type": "STRING"},
            {"name": "actual_timestamp", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "correction_ind", "mode": "NULLABLE", "type": "BOOLEAN"},
            {"name": "event_source", "mode": "NULLABLE", "type": "STRING"},
            {"name": "train_file_address", "mode": "NULLABLE", "type": "STRING"},
            {"name": "platform", "mode": "NULLABLE", "type": "STRING"},
            {"name": "division_code", "mode": "NULLABLE", "type": "STRING"},
            {"name": "train_terminated", "mode": "NULLABLE", "type": "BOOLEAN"},
            {"name": "train_id", "mode": "NULLABLE", "type": "STRING"},
            {"name": "offroute_ind", "mode": "NULLABLE", "type": "BOOLEAN"},
            {"name": "variation_status", "mode": "NULLABLE", "type": "STRING"},
            {"name": "train_service_code", "mode": "NULLABLE", "type": "STRING"},
            {"name": "toc_id", "mode": "NULLABLE", "type": "STRING"},
            {"name": "loc_stanox", "mode": "NULLABLE", "type": "STRING"},
            {"name": "auto_expected", "mode": "NULLABLE", "type": "BOOLEAN"},
            {"name": "direction_ind", "mode": "NULLABLE", "type": "STRING"},
            {"name": "route", "mode": "NULLABLE", "type": "STRING"},
            {"name": "planned_event_type", "mode": "NULLABLE", "type": "STRING"},
            {"name": "next_report_stanox", "mode": "NULLABLE", "type": "STRING"},
            {"name": "line_ind", "mode": "NULLABLE", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="storage_and_bq_airflow",
        time_partitioning={
            "field": "actual_timestamp",
            "type": "DAY",
        },
    )

    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery >> end
    extract_data >> do_nothing >> end