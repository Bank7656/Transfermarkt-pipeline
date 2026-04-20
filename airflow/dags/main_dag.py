import sys
sys.path.insert(0, '/opt/airflow')
import os
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

from scripts.download import download_dataset
from scripts.upload import upload_to_gcs
from scripts.load_bq import load_to_bigquery

# Ingestion
DATASET_NAME = "davidcariboo/player-scores"
DATA_DIR = '/opt/airflow/data'
# Google cloud storage
GCP_KEY_PATH = "/opt/airflow/secrets/google_credentials.json"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
BQ_DATASET = os.environ.get("BQ_DATASET")

# 👇 The complete rules for the Transfermarkt dataset
TABLES_CONFIG = {
    # --- FACT TABLES (Time-based, Large) ---
    "appearances": {
        "partition_col": "date", 
        "cluster_cols": ["player_id", "competition_id"]
    },
    "games": {
        "partition_col": "date", 
        "cluster_cols": ["competition_id"]
    },
    "player_valuations": {
        "partition_col": "date", 
        "cluster_cols": ["player_id"]
    },
    "game_events": {
        "partition_col": "date", 
        "cluster_cols": ["game_id", "player_id"]
    },
    "game_lineups": {
        "partition_col": "date", 
        "cluster_cols": ["game_id", "club_id"]
    },

    # --- DIMENSION TABLES (Descriptive, Smaller) ---
    "players": {
        "partition_col": None, 
        "cluster_cols": ["current_club_id"]
    },
    "clubs": {
        "partition_col": None, 
        "cluster_cols": ["club_id"]
    },
    "competitions": {
        "partition_col": None, 
        "cluster_cols": ["competition_id"]
    },
    "club_games": {
        "partition_col": None, 
        "cluster_cols": ["club_id", "game_id"]
    }
}

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='0 0 * * 1',
    catchup=False,
    tags=["Ingestion"]
)
def data_ingestion(dataset: str, data_dir: str, project_id: str, bucket_name: str, gcp_key: str, bq_dataset: str):

    # Execution order
    download_task = download_dataset(dataset, data_dir)
    upload_task = upload_to_gcs(project_id, bucket_name, gcp_key, data_dir)

    download_task >> upload_task

    run_pyspark_transform = BashOperator(
        task_id='run_pyspark_transform',
        bash_command='python /opt/airflow/scripts/transform_player_stats.py',
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/secrets/google_credentials.json",
            'GCP_BUCKET_NAME': os.getenv('GCP_BUCKET_NAME'),
            **os.environ,
        }
    )

    for table_name, config in TABLES_CONFIG.items():
        bq_load_task = load_to_bigquery(project_id, bucket_name, gcp_key, bq_dataset, table_name, config)
        upload_task >> bq_load_task >> run_pyspark_transform

data_ingestion(
    DATASET_NAME,
    DATA_DIR,
    GCP_PROJECT_ID,
    GCP_BUCKET_NAME,
    GCP_KEY_PATH,
    BQ_DATASET
)