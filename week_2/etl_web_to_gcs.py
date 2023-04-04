from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcsBucket

@flow(retries=3)
def fetch(database_url: str)-> pd.DataFrame:
    """Read data from web into pandas dataframe"""
    df = pd.read_csv(database_url)
    return df

@flow(log_prints=True)
def clean(df):
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("mor-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    database_file = f"{color}/{color}_tripdata_{year}-{month:02}"
    database_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{database_file}.csv.gz"

    df = fetch(database_url)
    df_clean = clean(df)
    path = write_local(df_clean, database_file)
    write_gcs(path)

if __name__ == "__main__":
    etl_gcs_to_bq()