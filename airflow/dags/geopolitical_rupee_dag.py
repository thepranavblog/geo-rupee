from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

DAG_ID = "geopolitical_rupee_pipeline"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def send_failure_alert(**context):
    failed_task = context.get("task_instance").task_id
    print(f"ALERT: Task {failed_task} failed in DAG {DAG_ID}")
    # Extend here: send Slack / email notification


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["geopolitical", "rupee", "streaming"],
) as dag:

    check_silver_data_exists = S3KeySensor(
        task_id="check_silver_data_exists",
        bucket_name="geo-rupee-pipeline",
        bucket_key="silver/correlated/date={{ ds }}/",
        timeout=600,
        poke_interval=60,
    )

    run_dbt_stg_events = BashOperator(
        task_id="run_dbt_stg_events",
        bash_command="cd /dbt_project && dbt run --select staging.stg_geopolitical_events",
    )

    run_dbt_stg_market = BashOperator(
        task_id="run_dbt_stg_market",
        bash_command="cd /dbt_project && dbt run --select staging.stg_market_movement",
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command="cd /dbt_project && dbt run --select intermediate.int_correlated_events",
    )

    run_dbt_fact_correlation = BashOperator(
        task_id="run_dbt_fact_correlation",
        bash_command="cd /dbt_project && dbt run --select marts.fact_correlation",
    )

    run_dbt_dim_country = BashOperator(
        task_id="run_dbt_dim_country",
        bash_command="cd /dbt_project && dbt run --select marts.dim_country",
    )

    run_dbt_dim_event_type = BashOperator(
        task_id="run_dbt_dim_event_type",
        bash_command="cd /dbt_project && dbt run --select marts.dim_event_type",
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /dbt_project && dbt test",
    )

    notify_on_failure = PythonOperator(
        task_id="notify_on_failure",
        python_callable=send_failure_alert,
        trigger_rule="one_failed",
    )

    # Dependencies
    check_silver_data_exists >> [run_dbt_stg_events, run_dbt_stg_market]
    [run_dbt_stg_events, run_dbt_stg_market] >> run_dbt_intermediate
    run_dbt_intermediate >> [run_dbt_fact_correlation, run_dbt_dim_country, run_dbt_dim_event_type]
    [run_dbt_fact_correlation, run_dbt_dim_country, run_dbt_dim_event_type] >> run_dbt_tests
    run_dbt_tests >> notify_on_failure
