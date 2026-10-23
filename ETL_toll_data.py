"""Airflow DAG for the Toll Data ETL project using BashOperator.

The DAG extracts selected fields from CSV, TSV, and fixed-width source files,
consolidates them into one file, uppercases the vehicle type, and writes the
transformed output into the staging directory.

Expected layout:
  $AIRFLOW_HOME/dags/ETL_toll_data.py
  $AIRFLOW_HOME/dags/finalassignment/tolldata.tgz
  $AIRFLOW_HOME/dags/finalassignment/staging/
"""

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


# Resolve Airflow's home directory from the environment variable configured
# in the terminal that starts Airflow. This keeps paths portable across users.
AIRFLOW_HOME = Path(os.environ["AIRFLOW_HOME"])
BASE_DIR = AIRFLOW_HOME / "dags" / "finalassignment"
STAGING_DIR = BASE_DIR / "staging"
ARCHIVE_PATH = BASE_DIR / "tolldata.tgz"

# Assignment settings. Replace the placeholder email with a real address if
# SMTP email is configured in Airflow. These flags alone do not configure SMTP.
default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2026, 10, 22),
    "email": ["your_email@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the daily DAG. catchup=False avoids creating a backlog of old
# scheduled runs when the DAG is first enabled or Airflow is restarted.
with DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    schedule="@daily",
    description="Apache Airflow Final Assignment: toll data ETL using BashOperator",
    catchup=False,
    max_active_runs=1,
    tags=["ETL", "BashOperator", "toll-data"],
) as dag:

    # Task 1: Extract the .tgz archive into the staging folder.
    # The source archive is kept one level above staging, so task reruns do not
    # require re-downloading it.
    unzip_data = BashOperator(
        task_id="unzip_data",
        bash_command=(
            f"mkdir -p '{STAGING_DIR}' && "
            f"tar -xzf '{ARCHIVE_PATH}' -C '{STAGING_DIR}'"
        ),
    )

    # Task 2: Extract columns 1-4 from the comma-separated vehicle file.
    extract_data_from_csv = BashOperator(
        task_id="extract_data_from_csv",
        bash_command=(
            f"cut -d',' -f1-4 '{STAGING_DIR}/vehicle-data.csv' "
            f"> '{STAGING_DIR}/csv_data.csv'"
        ),
    )

    # Task 3: Extract tab-separated fields 5-7, convert tabs to commas, and
    # remove carriage returns so the output is suitable for consolidation.
    extract_data_from_tsv = BashOperator(
        task_id="extract_data_from_tsv",
        bash_command=(
            f"cut -f5-7 '{STAGING_DIR}/tollplaza-data.tsv' "
            f"| tr '\\t' ',' | tr -d '\\r' "
            f"> '{STAGING_DIR}/tsv_data.csv'"
        ),
    )

    # Task 4: Extract the fixed-width segment used for payment and vehicle
    # codes. cut character positions are 1-based and inclusive.
    extract_data_from_fixed_width = BashOperator(
        task_id="extract_data_from_fixed_width",
        bash_command=(
            f"cut -c59-67 '{STAGING_DIR}/payment-data.txt' "
            f"| tr ' ' ',' | tr -d '\\r' "
            f"> '{STAGING_DIR}/fixed_width_data.csv'"
        ),
    )

    # Task 5: Join corresponding lines from the three extracted files into
    # one nine-field output. paste joins by line position, not by a key.
    consolidate_data = BashOperator(
        task_id="consolidate_data",
        bash_command=(
            "paste -d',' "
            f"'{STAGING_DIR}/csv_data.csv' "
            f"'{STAGING_DIR}/tsv_data.csv' "
            f"'{STAGING_DIR}/fixed_width_data.csv' "
            f"> '{STAGING_DIR}/extracted_data.csv'"
        ),
    )

    # Task 6: Uppercase field 4 (vehicle type) and save the final output.
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=(
            "awk -F',' 'BEGIN { OFS=\",\" } "
            "{$4=toupper($4); print}' "
            f"'{STAGING_DIR}/extracted_data.csv' "
            f"> '{STAGING_DIR}/transformed_data.csv'"
        ),
    )

    # The assignment's pipeline is sequential: each task starts only after
    # its upstream task succeeds. (The three extractions could be parallelized,
    # but this chain preserves the original lab's task order.)
    (
        unzip_data
        >> extract_data_from_csv
        >> extract_data_from_tsv
        >> extract_data_from_fixed_width
        >> consolidate_data
        >> transform_data
    )
