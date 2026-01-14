#!/usr/bin/env python
# coding: utf-8

#import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pyarrow.parquet as pq
import fsspec
from tqdm import tqdm

def main():

    # Read a sample of the data
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/'
    df_zones = pd.read_csv(prefix + 'taxi_zone_lookup.csv')

    target_table_zone="green_taxi_zones"
    chunksize=100000

    # Read a sample of the data
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    df_parquet = pd.read_parquet(prefix + 'green_tripdata_2025-11.parquet')

    pg_user= "root"
    pg_pass= "root"
    pg_host="pgdatabase"
    pg_port=5432
    pg_db= "ny_green_taxi"

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_zones.head(0).to_sql(
                    name=target_table_zone,
                    con=engine,
                    if_exists='replace'
                )

    df_zones.to_sql(
                name=target_table_zone,
                con=engine,
                if_exists='append'
            )


    target_table="green_taxi_data"

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"

    first = True

    with fsspec.open(url, "rb") as f:
        pf = pq.ParquetFile(f)

        for batch in tqdm(pf.iter_batches(batch_size=chunksize)):
            df_chunk = batch.to_pandas()

            if first:
                # Create table schema only
                df_chunk.head(0).to_sql(
                    name=target_table,
                    con=engine,
                    if_exists="replace",
                    index=False
                )
                first = False

            # Append data
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append",
                index=False,
                method="multi"   # MUCH faster inserts
            )

if __name__ == "__main__":
    main()



