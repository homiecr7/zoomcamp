#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import pyarrow.parquet as pq
import argparse
from sqlalchemy import create_engine
from time import time



# user
# password
# host
# port
# database name
# table name
# url of the csv

def main(params):
    user, password, host, port, db, table_name, url = params.user, params.password, params.host, params.port, params.db, params.table_name, params.url
    
    parquet_name = "output.parquet"
    csv_name = "output.csv"
    # download the parquet
    os.system(f"wget {url} -O {parquet_name}")
    trips = pq.read_table(parquet_name)
    df = trips.to_pandas()
    df.to_csv(csv_name)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.drop("Unnamed: 0", axis="columns", inplace=True)

    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
    print("Header Inserted")
    df.to_sql(name=table_name, if_exists="append", con=engine)
    print("First Itration Inserted")

    try:
        while True:
            t_start = time()
            df = next(df_iter)
            
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.drop("Unnamed: 0", axis="columns", inplace=True)
            
            df.to_sql(name=table_name, if_exists="append", con=engine)
            
            t_end = time()
            print(f"Data chunk inserted in {t_end - t_start}")
    except StopIteration as e:
        print(f"All itrations inserted so {e} in place")

if __name__=='__main__':
    parser = argparse.ArgumentParser(description="Injest CSV data to database")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host name for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="table name for postgres")
    parser.add_argument("--url", help="url of the csv")

    args = parser.parse_args()
    main(args)


