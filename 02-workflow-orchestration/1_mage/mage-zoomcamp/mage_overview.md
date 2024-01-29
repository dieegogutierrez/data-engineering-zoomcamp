### Configuring postreges
- On mage or vs code, but on mage click on files>io_config.yaml
- Create a dev "profile" at the end and copy and paste postgres default template
- Call environment variables using jinja "{{ env_var('POSTGRES_DBNAME') }}"
- Pipelines>New pipeline>Standard(batch)
- Edit pipeline>+Data loader>SQL
- Connexion>PostgreSQL 
- Profile>dev

### Homework
#### EXTRACT DATA - DATA LOADER
- DATA LOADER>PYTHON>API
- Does not require request
- Get column data types using the code below on jupyter notebook
```bash
print(pd.io.sql.get_schema(df, name='green_taxi_data', con=engine))
```  
- To send the df to the next block is necessary to write data types using pandas and for timestamp types use parse_dates
```bash 
taxi_dtypes = {
    "VendorID": pd.Int64Dtype(),
	"store_and_fwd_flag": str,
	"RatecodeID": pd.Int64Dtype(),
	"PULocationID": pd.Int64Dtype(),
	"DOLocationID": pd.Int64Dtype(), 
	"passenger_count": pd.Int64Dtype(),
	"trip_distance": float, 
	"fare_amount": float, 
	"extra": float,
	"mta_tax": float,
	"tip_amount": float, 
	"tolls_amount": float, 
	"ehail_fee": float,
	"improvement_surcharge": float,
	"total_amount": float, 
	"payment_type": pd.Int64Dtype(),
	"trip_type": pd.Int64Dtype(), 
	"congestion_surcharge": float
    }

parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

return pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
```
#### TRANSFORM DATA - TRANSFORMER
- Next TRASNFORMER>PYTHON>NO TEMPLATE(GENERIC)
- Taking zeros out of a column
```bash
@transformer
def transform(data, *args, **kwargs):
    print(f'Preprocessing: rows with zero passengers: {data["passenger_count"].isin([0]).sum()}')

    return data[data['passenger_count'] > 0] 

@test
def test_output(output, *args):
    assert output["passenger_count"].isin([0]).sum() == 0, 'There are rides with zero passengers'
```
#### LOAD DATA - DATA EXPORTER
- DATA EXPORTER>PYTHON>POSTGRESQL
```bash
@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    schema_name = 'ny_taxi'  # Specify the name of the schema to export data to
    table_name = 'green_taxi_data'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev' # Specify the configured profile on io_config_yaml

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )
```
### Connecting to Google Cloud
- Create a storage bucket
- Create a service account
- Create a key - JSON file
- Upload to directory
- Change io_config.yaml file GOOGLE_SERVICE_ACC_KEY_FILEPATH

### Upload csv file to Google Bucket
- Drag and drop from vs code to Google bucket, or click upload file. If not possible, check VM Cloud API access scopes under STORAGE check if its Read and Write, if not stop the VM and edit it. Or use the following code in the terminal of the VM.
```bash
gsutil cp titanic_clean.csv gs://ny-rides-diegogutierrez-terra-bucket
```

### LOAD DATA FROM CLOUD TO MAGE
- DATA LOADER>PYTHON>GOOGLE CLOUD STORAGE
- Bucket name and name of file with extension

### ETL:API TO GCS
- On mage start a new pipeline, drag and drop from data_exporter and transformers the code we have did before. load_data_api and the clean_taxi_rides
- Connect blocks dragging the line between them
- Create a new DATA EXPORTER>PYTHON>GOOGLE CLOUD STORAGE
- On object key, write a name for the file be created with extension PARQUET. PARQUET is better than CSV.
- For big files is advised to load partitioned files, it can be partitioned by date for example.
- DATA EXPORTER>PYTHON>GENERIC(NO TEMPLATE)
- Exclude line from the last data loader and create a line with transformer.
- Change the code.
```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/.gc/ny-rides.json"

bucket_name = 'ny-rides-diegogutierrez-terra-bucket'
project_id = 'ny-rides-diegogutierrez'

table_name = 'nyc_taxi_data'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
```
### ETL:GCS TO BIGQUERY
- DATA LOADE - LOAD PARQUET FILE
- TRANSFORMER - Standardize column names
```python
data.columns = (data.columns
                    .str.replace(' ', '_')
                    .str.lower())
```
- DATA EXPORTER - Connexion>BigQuery, Profile>default, Schema>ny_taxi, Table>green_taxi_data
```bash
SELECT * FROM {{ df_1 }}
```
### SCHEDULES
- Go to TRIGGERS>NEW TRIGGER
- Choose which way to trigger your code, create and save it. Then enable it.

### PARAMETERIZED
```python
@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    now = kwargs.get('execution_date')
    now_fpath = now.strftime("%Y/%m/%d")

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'ny-rides-diegogutierrez-terra-bucket'
    object_key = f'{now_fpath}/daily-trips.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
```

### BACKFILL

### DEPLOY MAGE IN THE CLOUD WITH TERRAFORM
- Change variables.tf, and add credentials to both variables.tf and main.tf. Or create Google Secret Manager and upload service account key. 
- Add the following roles to the service account:
    - Artifact Registry Read
    - Artifact Registry Writer
    - Cloud Run Developer
    - Cloud SQL Admin
    - Service Account Token Creator
    - Cloud Run Admin
- Give full google api access to the VM
- Go to Cloud Run, try to use the link to access mage if get an error go to Network and give ingress control access to all.