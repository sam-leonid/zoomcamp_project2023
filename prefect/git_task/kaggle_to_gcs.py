from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from typing import List
from subprocess import Popen, PIPE, STDOUT
import zipfile
import os

@task(retries=3, log_prints=True)
def download_dataset(year:int) -> str:
    """Read flight data from web into pandas DataFrame"""
    path = "data"
    file_name = f"Combined_Flights_{year}.parquet"
    dataset_name = "robikscube/flight-delay-dataset-20182022"
    zip_file = f"{path}/{file_name}.zip"
    command = f"kaggle datasets download " + \
            f"-f {file_name} -p {path} {dataset_name}"

    process = Popen(command, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
    process.wait()
    if process.returncode != 0:
        raise Exception(process.stdout.read().decode('utf8'))

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(path)
    os.remove(zip_file)

    # df = pd.read_parquet(f"{path}/{file_name}")
    return f"{path}/{file_name}"

@task()
def write_gcs(path: str) -> None:
    """Upload local parquet file to GCS"""
    from prefect.filesystems import GCS
    gcs_block = GCS.load("zoom-gcs")
    gcs_block.put_directory(local_path=path, to_path=path)
    # gcs_block = GcsBucket.load("zoom-gcs")
    # gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def etl_web_to_gcs_main(years: List[int] = [2018,2019,2020,2021,2022]):
    for year in years:
        etl_web_to_gcs(year)

@flow()
def etl_web_to_gcs(year: int = 2020) -> None:
    """The main ETL function"""
    file_path = download_dataset(year)
    write_gcs(file_path)


if __name__ == "__main__":
    etl_web_to_gcs()