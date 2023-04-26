#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import pyarrow.parquet as pq
import argparse
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    parquet_name = "output.parquet"
    csv_name = "output.csv"
    # download the parquet
    os.system(f"wget {url} -O {parquet_name}")
    trips = pq.read_table(parquet_name)
    df = trips.to_pandas()
    df.to_csv(csv_name)
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.drop("Unnamed: 0", axis="columns", inplace=True)

    return df, df_iter

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df["passenger_count" != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return(df)

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, df):
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
    print("Header Inserted")
    df.to_sql(name=table_name, if_exists="append", con=engine)
    # print("First Itration Inserted")
    print("Itration Inserted")

    # try:
    #     while True:
    #         t_start = time()
    #         df = next(df_iter)
            
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.drop("Unnamed: 0", axis="columns", inplace=True)
            
    #         df.to_sql(name=table_name, if_exists="append", con=engine)
            
    #         t_end = time()
    #         print(f"Data chunk inserted in {t_end - t_start}")
    # except StopIteration as e:
    #     print(f"All itrations inserted so {e} in place")

@flow(name = "Ingest Flow")
def main_flow():
    user = "root"
    password = "root"
    host = "localhost"
    port = "5431"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"

    raw_data, itr_object = extract_data(url)
    transformed_data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, transformed_data)

if __name__=='__main__':
    main_flow()


    

