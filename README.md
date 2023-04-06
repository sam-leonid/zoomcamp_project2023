# Architecture


# Problem

The aim of the project is show some statistics for flights in last 5 years.

Questions:
- What is the most frequently used airline?
- How flights are distributed in timeline in 5 years?
- What is the distribution of delays in flights?
- What is the most popular year for flights?

# Dataset

The dataset is taken from kaggle datasets. This dataset contains all flight information including cancellation and delays by airline for dates back to January 2018. There are 5 files in format Combined_Flights_XXXX.parquet in this dataset.

I need to transfer this data to GCS and then concatenate in one table.

# Project Details

Technologies used in this project are:

Google Cloud Platform: to storage raw data in GCS (data lake) and to analyze it in BigQuery.
BigQuery: datawarehouse for effective and compact storing of loaded data.
Terraform: Tool that provides Infraestructure as Code (IaC) to generate our resources in the cloud (buckets and datasets).
Prefect: to orchestrate our data pipelines in a monthly schedule.
DBT tool: tool for effective and fast data transform from raw to core and datamart layers.

# Dashboard

[Link to dashboard](https://lookerstudio.google.com/u/0/reporting/fdc6d2c5-320f-48e2-be01-8bafb8f00d2b/page/2ikLD?s=onMH9L3L6Lo)

# Reproducing setp by step:

**1**. Create infastucture in GCS using Terraform:

1. Initialize terraform:
	
  ```sh
	 terraform init
  ```

2. Check that you're creating the correct resources (GCS bucket and BigQuery dataset):
   
    ```sh
    terraform plan
    ```

3. Create the resources:
    
    ```sh
    terraform apply
    ```

**2**. Transfer data with Prefect to Google Cloud Storage:

At first I create an account on Prefect cloud and customize settings.

Then I build parametrized flow and run it:

```sh
prefect deployment build ./main.py:git_flow -n "Parameterized gitflow flight"
prefect deployment apply git_flow-deployment.yaml
prefect agent start  --work-queue default
```

### Prefect steps:

1.1. Downloading kaggle data with kaggle API
1.2. Unzipping and save local
1.3. Uploading data to GCS


**3**. Create external table from local file in GCS:

```sql
CREATE OR REPLACE EXTERNAL TABLE `flights-kaggle-project.flights_data_all.flights_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://flights_kaggle_lake/Combined_Flights_*.parquet']
);
```

**4**. Create core layer (total_flights) with DBT:

I create infrastructure in DBT cloud and customize settings.

```sh
dbt run --var 'is_test: false'
```

**5**. Create dashboard using new table 

Using table total_flights create dashboard using looker studio.