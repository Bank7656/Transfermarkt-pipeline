from airflow.sdk import task

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