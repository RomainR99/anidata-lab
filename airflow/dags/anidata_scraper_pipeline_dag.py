from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS: dict[str, object] = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 1,
}


SCRAPER_BASE_URL = os.getenv("ANIDATA_SCRAPER_BASE_URL", "http://mock-site")
SCRAPER_OUTPUT_DIR = "/opt/airflow/data/raw"
AIRFLOW_HOME = "/opt/airflow"


with DAG(
    dag_id="anidata_scraper_pipeline",
    default_args=DEFAULT_ARGS,
    description="Scraper -> Nettoyage -> Feature engineering -> Validation -> Elasticsearch",
    start_date=datetime(2026, 4, 27),
    schedule="@daily",
    catchup=False,
    tags=["anidata", "scraper", "etl"],
) as dag:
    scrape_anidex = BashOperator(
        task_id="01_scrape_anidex",
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"mkdir -p {SCRAPER_OUTPUT_DIR} && "
            "python -m anidata_scraper.scraper "
            f"--base-url {SCRAPER_BASE_URL} "
            f"--output-dir {SCRAPER_OUTPUT_DIR}"
        ),
    )

    run_cleaning = BashOperator(
        task_id="02_nettoyage",
        bash_command=f"cd {AIRFLOW_HOME} && python /opt/airflow/scripts/03_nettoyage.py",
    )

    run_feature_engineering = BashOperator(
        task_id="03_feature_engineering",
        bash_command=f"cd {AIRFLOW_HOME} && python /opt/airflow/scripts/04_feature_engineering.py",
    )

    run_validation = BashOperator(
        task_id="04_validation",
        bash_command=f"cd {AIRFLOW_HOME} && python /opt/airflow/scripts/05_validation.py",
    )

    run_elasticsearch_indexation = BashOperator(
        task_id="05_indexation_elasticsearch",
        bash_command=f"cd {AIRFLOW_HOME} && python /opt/airflow/scripts/script_prof.py",
    )

    scrape_anidex >> run_cleaning >> run_feature_engineering >> run_validation >> run_elasticsearch_indexation
