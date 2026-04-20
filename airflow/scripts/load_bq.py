from airflow.sdk import task

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

    # Force delete the table if it already exists so we can recreate it cleanly
    print(f"Checking if {table_id} exists to drop it...")
    client.delete_table(table_id, not_found_ok=True) 

    print(f"Loading {uri} into {table_id}...")
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result() 
    
    return f"Loaded {table_name}"