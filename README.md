# ETL Toll Data Pipeline with Apache Airflow (BashOperator)

## Project overview

This project builds a scheduled ETL workflow with Apache Airflow and `BashOperator`. It processes toll traffic data supplied in three formats, consolidates selected fields, converts vehicle types to uppercase, and writes the final data to a staging directory.

The source lab asks for CSV, TSV, and fixed-width extraction, transformation, and staging of the result. This repository version is configured for the local WSL Ubuntu setup used during development.

## Technology stack

- Apache Airflow 2.10.5: schedules, coordinates, and monitors the workflow
- Python 3.12: defines the DAG and task configuration
- `BashOperator`: executes Linux shell commands as Airflow tasks
- Linux utilities: `tar`, `cut`, `tr`, `paste`, and `awk`
- Ubuntu on WSL and VS Code: local development environment

## Workflow

```text
unzip_data
    |
extract_data_from_csv
    |
extract_data_from_tsv
    |
extract_data_from_fixed_width
    |
consolidate_data
    |
transform_data
```

1. `unzip_data`: extracts the downloaded archive into `staging/`.
2. `extract_data_from_csv`: selects fields 1-4 from `vehicle-data.csv`.
3. `extract_data_from_tsv`: selects TSV fields 5-7 and converts tabs to commas.
4. `extract_data_from_fixed_width`: selects characters 59-67 from `payment-data.txt` and converts spaces to commas.
5. `consolidate_data`: combines the three extracted files side by side with `paste`.
6. `transform_data`: uppercases column 4 (`Vehicle type`) and writes `transformed_data.csv`.

**Important:** `paste` combines rows by line position, not by joining on a key. The assignment's source files are expected to align row by row. The shell pipeline follows the lab's field positions and delimiter conversion.

## Local project paths

The project was developed inside the existing `~/IBMDEexercises/AirflowKafka` folder:

```text
~/IBMDEexercises/AirflowKafka/
├── .venv/                              # Existing Python virtual environment
└── airflow_home/
    └── dags/
        ├── ETL_toll_data.py            # This DAG file
        └── finalassignment/
            ├── tolldata.tgz             # Downloaded source archive
            └── staging/
                ├── vehicle-data.csv
                ├── tollplaza-data.tsv
                ├── payment-data.txt
                ├── csv_data.csv
                ├── tsv_data.csv
                ├── fixed_width_data.csv
                ├── extracted_data.csv
                └── transformed_data.csv
```

The data archive and generated outputs are not included in this repository bundle. The setup guide shows how to download the archive and recreate the directories.

## Quick start

See [`docs/REPRODUCE_AND_COMMANDS.md`](docs/REPRODUCE_AND_COMMANDS.md) for the full, commented setup and reproduction procedure.

Basic checks:

```bash
# Activate the existing environment and point Airflow to the project home.
cd ~/IBMDEexercises/AirflowKafka
source .venv/bin/activate
export AIRFLOW_HOME="$HOME/IBMDEexercises/AirflowKafka/airflow_home"

# Check DAG import errors and confirm that Airflow discovers the DAG.
airflow dags list-import-errors
airflow dags list | grep ETL_toll_data

# List task IDs, unpause the DAG, and manually trigger a run.
airflow tasks list ETL_toll_data
airflow dags unpause ETL_toll_data
airflow dags trigger ETL_toll_data
```

## Output

The final transformed file is saved at:

`$AIRFLOW_HOME/dags/finalassignment/staging/transformed_data.csv`

## Key learning points

- A DAG describes the workflow; a `BashOperator` task runs a shell command.
- `AIRFLOW_HOME` tells Airflow which home directory to use. It needs to be set in the terminal environment used to start Airflow.
- Airflow discovers DAG files under `$AIRFLOW_HOME/dags/` and parses the Python file to identify the DAG and tasks.
- The `>>` operator defines dependencies. A downstream task is scheduled only after its upstream task succeeds.
- `cut` selects delimited fields or character positions, `tr` replaces characters, `paste` combines files by line position, and `awk` transforms a selected field.
- The Airflow UI and task logs are used to monitor runs and troubleshoot failures.

## Notes before publishing

- Replace `your_email@example.com` in `ETL_toll_data.py` with an intended address, or adjust the email settings. Alert delivery also requires SMTP to be configured in Airflow.
- The `start_date` is set to the project build date. `catchup=False` prevents backfilling older scheduled runs; manual triggering is available at any time.
- Do not commit Airflow logs, the downloaded archive, virtual environments, or generated staging CSV files.
- Screenshots can be added under `screenshots/`; see [`screenshots/SCREENSHOT_CHECKLIST.md`](screenshots/SCREENSHOT_CHECKLIST.md).

**Project focus:** Batch ETL · Apache Airflow · BashOperator · Linux shell utilities · Workflow orchestration
