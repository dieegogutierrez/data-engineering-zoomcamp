import pandas as pd

import inflection

from sqlalchemy import create_engine
from time import time

import pyarrow as pa
import pyarrow.parquet as pq

def transform_files(months, AIRFLOW_HOME, OUTPUT_FILE_PATH):
    dfs = []
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float 
    }

    for month in months:
        URL_FILE_NAME = f'green_tripdata_2020-{month}.csv.gz'
        file_to_concat = f'{AIRFLOW_HOME}/{URL_FILE_NAME}'
        df = pd.read_csv(file_to_concat, sep=',', dtype=taxi_dtypes, parse_dates=parse_dates)
        dfs.append(df)
    
    data = pd.concat(dfs, ignore_index=True)

    print(f"Dataset shape before transformations: {data.shape}")

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    old_columns = data.columns.to_list()
    new_columns = [inflection.underscore(column) for column in data.columns]

    changed_columns_count = sum(1 for i in range(len(old_columns)) if old_columns[i] != new_columns[i])

    data.columns = new_columns

    print(f"The unique values in 'vendor_id' column are: {list(data['vendor_id'].unique())}")
    print(f"The number of columns that changed names is: {changed_columns_count}")

    data = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]

    print(f"Dataset shape after transformations: {data.shape}")

    assert 'vendor_id' in data.columns, "There is no column named vendor_id"
    assert (data["passenger_count"] > 0).all(), 'There are rides with zero passengers or less'
    assert (data["trip_distance"] > 0).all(), 'There are rides with zero distance or less'

    data.to_csv(OUTPUT_FILE_PATH, index=False)
    
def ingest_postgres(user, password, host, port, db, table_name, csv_file_path):
    print(table_name, csv_file_path)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    t_start = time()
    df_iter = pd.read_csv(csv_file_path, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('inserted the first chunk, took %.3f second' % (t_end - t_start))

    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))  

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    csv_file = pd.read_csv(local_file)
    table = pa.Table.from_pandas(csv_file)
    root_path = f'{bucket}/{object_name}'

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )