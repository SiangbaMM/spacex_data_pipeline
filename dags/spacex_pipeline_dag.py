"""
SpaceX Data Pipeline DAG

This DAG orchestrates the SpaceX data pipeline:
1. Extract data from SpaceX API using Singer tap
2. Load data into Snowflake
3. Transform data using dbt
"""
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
}


@dag(
    dag_id="spacex_pipeline",
    default_args=default_args,
    description="Extract SpaceX data, load to Snowflake, and transform with dbt",
    schedule_interval="0 0 * * *",  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spacex", "dbt", "singer"],
)
def spacex_pipeline() -> None:
    """Space data pipeline main dag"""
    # Start task
    begin = EmptyOperator(task_id="begin")

    # Extract and load data using Singer tap

    extract_load = BashOperator(
        task_id="tap_extract_load",
        bash_command="cd /usr/local/airflow && source ./env_singer/bin/activate && python3 singer_tap/runner/tap_spacex_runner.py",
    )

    transform = BashOperator(
        task_id="dbt_transform",
        bash_command="cd /usr/local/airflow && source ./env_dbt/bin/activate && dbt run -s +marts --project-dir spacex_project/ --profiles-dir spacex_project/",
    )

    test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /usr/local/airflow && source ./env_dbt/bin/activate && dbt test --project-dir spacex_project/ --profiles-dir spacex_project/",
    )

    # End task
    end = EmptyOperator(task_id="end")

    # Define task dependencies
    begin >> extract_load >> transform >> test >> end


# Create DAG instance
spacex_pipeline()
