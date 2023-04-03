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
                               local_path=f"{local_path}/code")
    return

@flow()
def exec_clone_code(years:List[int],
                    local_path:str,
                    gcs_path:str) -> None:
    from data.code.prefect.git_task.kaggle_to_gcs import etl_web_to_gcs_main
    etl_web_to_gcs_main(years=years, 
                        from_path=local_path,
                        to_path=gcs_path)
    

@flow()
def git_flow(from_path:str = "prefect/git_task",
             local_path:str = "data",
             gcs_path:str = "data",
             file_name:str = "kaggle_to_gcs.py",
             years:List[int] = [2018,2019,2020,2021,2022]):
    git_clone(from_path, local_path)
    exec_clone_code(years, local_path, gcs_path)
    

if __name__ == "__main__":
    git_flow()
                            