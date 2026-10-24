# ETL Pipeline with Apache Airflow (BashOperator)

## Project overview

This project builds a scheduled ETL workflow with Apache Airflow and `BashOperator`. It processes toll traffic data supplied in three formats, consolidates selected fields, converts vehicle types to uppercase, and writes the final data to a staging directory.

The source lab asks for CSV, TSV, and fixed-width extraction, transformation, and staging of the result. This repository version is configured for the local WSL Ubuntu setup used during development.

## Technology stack

- Apache Airflow: DAGs, schedules, coordinates, and monitors the workflow
- Python: defines the DAG and task configuration
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

**Important:** `paste` combines rows by line position, not by joining on a key.


## Key learning points

- A DAG describes the workflow; a `BashOperator` task runs a shell command.
- `AIRFLOW_HOME` tells Airflow which home directory to use. It needs to be set in the terminal environment used to start Airflow.
- Airflow discovers DAG files under `$AIRFLOW_HOME/dags/` and parses the Python file to identify the DAG and tasks.
- The `>>` operator defines dependencies. A downstream task is scheduled only after its upstream task succeeds.
- `cut` selects delimited fields or character positions, `tr` replaces characters, `paste` combines files by line position, and `awk` transforms a selected field.
- The Airflow UI and task logs are used to monitor runs and troubleshoot failures.

## Notes before publishing

**Project focus:** Batch ETL · Apache Airflow · BashOperator · Linux shell utilities · Workflow orchestration
