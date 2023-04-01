from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from typing import List

from prefect.filesystems import GitHub

@task()
def git_clone(from_path:str,
             local_path:str) -> None:
    """Clone remote file from github"""
    github_block = GitHub.load("github")
    github_block.get_directory(from_path=from_path, 
                               local_path=local_path)
    return

@flow()
def exec_clone_code(years:List[int]) -> None:
    from data.code.prefect.git_task.kaggle_to_gcs import etl_web_to_gcs_main
    etl_web_to_gcs_main(years=years)
    

@flow()
def git_flow(from_path:str = "prefect/git_task",
             local_path:str = "data/code",
             file_name:str = "kaggle_to_gcs.py",
             years:List[int] = [2018,2019,2020,2021,2022]):
    git_clone(from_path, local_path)
    exec_clone_code(years)
    

if __name__ == "__main__":
    git_flow()
                            