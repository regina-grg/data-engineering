#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name_1 = params.table_name_1
    table_name_2 = params.table_name_2
    url1 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'
    url2 = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
    csv1 = 'trips.csv.gz'
    csv2 = 'zones.csv'

    load_csv(url1, csv1)
    load_csv(url2, csv2)

    # create db connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    insert_trip_database(csv1, table_name_1, engine)
    insert_zones_database(csv2, table_name_2, engine)

def load_csv(url, csv):
    # download the CSV files
    print('download csv files to local dir')

    if not os.path.exists(csv):
        print('downloading csv1 - trips')
        os.system(f"wget {url} -O {csv}")
    print(f"finished downloading file {url}")

def insert_trip_database(csv_name, table_name, engine):
    # trips
    print('inserting trips to database')
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    while True:
        try:
            t_start = time()
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.columns = df.columns.str.lower()
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()

            df = next(df_iter)
            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('completed')
            break

def insert_zones_database(csv_name, table_name, engine):
    # zones
    print('inserting zones to database')
    df = pd.read_csv(csv_name)
    df.columns = df.columns.str.lower()
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print('finished inserting zones to database')


if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name_1', help='name of the table for trips')
    parser.add_argument('--table_name_2', help='name of the table for zones')
    # parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)