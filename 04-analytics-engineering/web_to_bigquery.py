import os
import dlt
import pandas as pd
from google.cloud import bigquery

def load_csv(year, service):
    if service == 'fhv':

        taxi_dtypes = {
            'dispatching_base_num': str,
            'PUlocationID': pd.Int64Dtype(),
            'DOlocationID': pd.Int64Dtype(),
            'SR_Flag': pd.Int64Dtype(),
            'Affiliated_base_number': str,
        }
    else:
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
            'congestion_surcharge': float ,
            'trip_type': pd.Int64Dtype()
        }

    if service == 'green':
        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    elif service == 'yellow':
        parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    else:
        parse_dates = ['pickup_datetime', 'dropOff_datetime']


    for i in range(12):
            
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        file_url = f"{init_url}{service}/{file_name}"

        for row in pd.read_csv(file_url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates, iterator=True):
            yield row


def web_to_gcs(year, service):
    # define the connection to load to.
    generators_pipeline = dlt.pipeline(destination='bigquery', dataset_name='ny_rides_all')


    # we can load any generator to a table at the pipeline destnation as follows:
    info = generators_pipeline.run(load_csv(year, service),
                                            table_name=f"{service}_tripdata",
                                            write_disposition="append")

    # the outcome metadata is returned by the load and we can inspect it by printing it.
    print(info)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = """
        SELECT *
        FROM f`ny_rides_all.{table_name}`
    """

    client.query(query)  # Make an API request.


# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
BUCKET = os.environ.get("GCP_GCS_BUCKET")

web_to_gcs('2019', 'green')

web_to_gcs('2020', 'green')

web_to_gcs('2019', 'yellow')

web_to_gcs('2020', 'yellow')

web_to_gcs('2019', 'fhv')
