import os
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

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
def data_ingestion(dataset :str, data_dir: str, project_id: str, bucket_name: str, gcp_key: str, bq_dataset: str):

    @task()
    def download_dataset(dataset: str, data_dir: str):
        import kaggle

        kaggle.api.authenticate()
        print(f"Downloading {dataset} to {data_dir}...")
        kaggle.api.dataset_download_files(
            dataset,
            path=data_dir,
            unzip=True
        )
        print("Download complete!")
        return f"Successfully downloaded {dataset}"
    
    @task()
    def upload_to_gcs(project_id: str, bucket_name: str, gcp_key: str, data_dir: str):
        from google.oauth2 import service_account
        from google.cloud import storage
        import glob
        import os

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key
        print(f"Connecting to Google Cloud Project: {project_id}...")
        if not os.path.exists(gcp_key):
            raise FileNotFoundError(
                f"🚨 GCP key not found at {gcp_key}! "
                "Check your docker-compose.yaml volume mounts."
            )
        print(f"Found GCP key at {gcp_key}. Authenticating...")
        credentials = service_account.Credentials.from_service_account_file(gcp_key)
        client = storage.Client(project=project_id, credentials=credentials)
        bucket = client.bucket(bucket_name)
        csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
        if not csv_files:
            print(f"No CSV files found in {data_dir}!")
            return "No files to upload."
        print(f"Found {len(csv_files)} CSV files. Starting upload...")
        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            destination_blob_name = f'data/ingested/{file_name}'
            print(f"Uploading {file_name}...")
            blob = bucket.blob(destination_blob_name)
            blob.chunk_size = 8 * 1024 * 1024
            blob.upload_from_filename(file_path, timeout=600)
        print("All files successfully uploaded to Cloud Storage!")
        return f"Uploaded {len(csv_files)} files"

    @task()
    def load_to_bigquery(project_id: str, bucket_name: str, gcp_key: str, bq_dataset: str, table_name: str, config: dict):
        from google.oauth2 import service_account
        from google.cloud import bigquery
        import os

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key
        credentials = service_account.Credentials.from_service_account_file(gcp_key)
        client = bigquery.Client(project=project_id, credentials=credentials)

        table_id = f"{project_id}.{bq_dataset}.{table_name}"
        uri = f"gs://{bucket_name}/data/ingested/{table_name}.csv"

        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1, 
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
        )

        # Apply Partitioning dynamically if it exists in the config
        if config.get("partition_col"):
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field=config["partition_col"] 
            )
            
        # Apply Clustering dynamically if it exists in the config
        if config.get("cluster_cols"):
            job_config.clustering_fields = config["cluster_cols"]

        # 👇 NEW CODE: Force delete the table if it already exists so we can recreate it cleanly
        print(f"Checking if {table_id} exists to drop it...")
        client.delete_table(table_id, not_found_ok=True) # not_found_ok=True prevents errors on the very first run

        print(f"Loading {uri} into {table_id}...")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result() 
        
        return f"Loaded {table_name}"

    # Execution order
    download_task = download_dataset(dataset, data_dir)
    upload_task = upload_to_gcs(project_id, bucket_name, gcp_key, data_dir)

    download_task >> upload_task

    run_pyspark_transform = BashOperator(
        task_id='run_pyspark_transform',
        bash_command='python /opt/airflow/scripts/transform_player_stats.py',
        # Pass the Google credentials to the Bash environment so Spark can authenticate
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/secrets/google_credentials.json",
            'GCP_BUCKET_NAME': os.getenv('GCP_BUCKET_NAME'),
            **os.environ,
        }
    )

    # 👇 Loop through the dictionary and generate tasks!
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