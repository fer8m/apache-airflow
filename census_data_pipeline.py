import logging
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonVirtualenvOperator

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable


with DAG(
    dag_id="census_data_pipeline",
    schedule=None,
    tags=["example"],
) as dag:
    # [START howto_operator_python_venv]
    @task.virtualenv(
        task_id="virtualenv_python", requirements=["pandas==2.1.1", "numpy==1.26.4"], system_site_packages=False
    )
    def retrieve_dataset():
        import pandas as pd
        RAW_FILE = "~/airflow/datasets/raw_census_dataset.csv"
        csv_url = "https://raw.githubusercontent.com/practical-bootcamp/week4-assignment1-template/main/city_census.csv"
        df = pd.read_csv(csv_url, index_col=0)
        head = df.head(3)
        print(f"CSV head:\n{head}")
        df.to_csv(RAW_FILE)

    @task.virtualenv(
            task_id="virtualenv_python", requirements=["pandas==2.1.1", "numpy==1.26.4"],
            system_site_packages=False
        )
    def clean_dataset():
        import pandas as pd
        RAW_FILE = "~/airflow/datasets/raw_census_dataset.csv"
        CLEANED_DATASET = "~/airflow/datasets/cleaned_dataset.csv"
        df = pd.read_csv(RAW_FILE, index_col=0)
        df["weight"] = df["weight"].fillna(0)
        df = df[(df.age > 30) & (df.state == "Iowa")]
        df.to_csv(CLEANED_DATASET)

    @task.virtualenv(
        task_id="sqlite_persist_wine_data", requirements=["pandas==2.1.1", "numpy==1.26.4", "sqlalchemy==2.0.21"],
        system_site_packages=False
    )
    def persist_dataset():
        import pandas as pd
        from sqlalchemy import create_engine
        CLEANED_DATASET = "~/airflow/datasets/cleaned_dataset.csv"
        engine = create_engine('sqlite:////home/fer_7/airflow/tmp/census_dataset.db', echo=True)
        df = pd.read_csv(CLEANED_DATASET, index_col=0)
        df.to_sql('census_dataset', engine)
    
    retrieve_dataset() >> clean_dataset() >> persist_dataset()
    
