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
def download_dataset(year:int, path:str) -> str:
    """Read flight data from web into pandas DataFrame"""
    file_name = f"Combined_Flights_{year}.parquet"
    dataset_name = "robikscube/flight-delay-dataset-20182022"
    zip_file = f"{path}/{file_name}.zip"
    command = f"kaggle datasets download " + \
            f"-f {file_name} -p {path} {dataset_name}"

    print(f"{command}")
    process = Popen(command, shell=True, stdout=PIPE, stderr=STDOUT, close_fds=True)
    process.wait()
    if process.returncode != 0:
        raise Exception(process.stdout.read().decode('utf8'))

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(path)
    print(f"Path files {os.listdir(path)}")
    os.remove(zip_file)
    print(f"Path of file: {path}/{file_name}")
    return f"{path}/{file_name}"

@task()
def write_gcs(file_path: str, to_path: str) -> None:
    """Upload local parquet file to GCS"""
    from prefect.filesystems import GCS
    gcs_block = GCS.load("zoom-gcs")
    gcs_block.put_directory(local_path=from_path, to_path=to_path)
    return

@flow()
def etl_web_to_gcs_main(from_path: str,
                        to_path: str,
                        years: List[int] = [2018,2019,2020,2021,2022]):
    for year in years:
        etl_web_to_gcs(year, from_path, to_path)

@flow()
def etl_web_to_gcs(year: int, 
                   from_path: str,
                   to_path: str) -> None:
    """The main ETL function"""
    file_path = download_dataset(year, from_path)
    write_gcs(file_path, to_path)


if __name__ == "__main__":
    etl_web_to_gcs()