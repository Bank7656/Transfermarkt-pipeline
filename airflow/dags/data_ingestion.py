from airflow.sdk import dag, task
from datetime import datetime

# Ingestion
DATASET_NAME = "davidcariboo/player-scores"
DATA_DIR = '/opt/airflow/data'
# Google cloud storage
GCP_KEY_PATH = "/opt/airflow/secrets/google_credentials.json"
PROJECT_ID = "transfermarkt-pipeline"
BUCKET_NAME = "football-data-storage-6532"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='0 0 * * 1',
    catchup=False,
    tags=["Ingestion"]
)
def data_ingestion(dataset :str, data_dir: str, project_id: str, bucket_name: str, gcp_key: str):

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
        
    download_dataset(dataset, data_dir) >> upload_to_gcs(project_id, bucket_name, gcp_key, data_dir)

data_ingestion(
    DATASET_NAME,
    DATA_DIR,
    PROJECT_ID,
    BUCKET_NAME,
    GCP_KEY_PATH,
)