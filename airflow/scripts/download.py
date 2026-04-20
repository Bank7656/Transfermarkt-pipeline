from airflow.sdk import task

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