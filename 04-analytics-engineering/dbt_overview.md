### UPLOAD CSV FILES TO BIGQUERY
- Use web_to_bigquery.py, only once, mode is on append

### CREATE PROJECT
- Create a new project
- Choose data warehouse
- Clone github repository
- Go to github repo settings, under security, add the deploy key given from dbt

### DEVELOP 
- Click on develop and the cloud IDE will open
- Create a branch to work on it
- Initiate it and it will create a folder in repo
- Go to dbt_project_yml and change project name up and down and delete the commented text below

```yml
models:
  taxi_rides_ny:
    # Applies to all files under models/example/
    # example:
    #   +materialized: table
```
### MODELS
- Create a folder 'staging' in models
- Create a file under staging called schema.yml
```yml
version: 2

sources:
  - name: staging
    database: ny-rides-diegogutierrez
    schema: ny_rides_all

    tables:
      - name: green_tripdata
      - name: yellow_tripdata
```
- After saving it, click on 'Generate model' above table name
- Tipe: "dbt build"

### MACROS
- Create in macros folder a sql file get_payment_type_description.sql
- It uses jinja to create macros
- Go to the table created before stg_green_tripdata.sql
- Add the macro created

```sql
select
        vendor_id,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        store_and_fwd_flag,
        ratecode_id,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description,
        trip_type,
        congestion_surcharge
```

### PACKAGES
- Similar to libraries
- dbthub has packages ready to use
- Go to dbt-utils package and copy the installation code
```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```
- Create a packages.yml file at the same level as the dbt_project.yml file and paste the code
- Before running type "dbt deps" to install the packages or "dbt build" will be enough, version change
- Now inside dbt_packages directory will have dbt_utils installed
- On dbt-utils github search for generate_surrogate_key(source) copy the code and paste it on stg_green_tripdata.sql
```sql
select
        {{ dbt_utils.generate_surrogate_key(['vendor_id', 'lpep_pickup_datetime']) }} as tripid,
        vendor_id,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        store_and_fwd_flag,
        ratecode_id,
        pu_location_id,
```
- It will create a primary hash key from the fields of your choice
- Type: "dbt build"
- On target folder there is what is running
- Copy and paste all the stg_green_tripdata.sql code
- You can type __ and choose the macro
- Create the stg_yellow_tripdata.sql and copy and paste code

### MASTER DATA TABLE
- Inside seeds directory create a file called taxy_zone_lookup.csv and copy and paste the file that is on data folder
- Click build
- Inside models directory create a folder called core
- Inside core create a file called dim_zones.sql and copy and paste code
- Inside core create a file called fact_trips.sql copy and paste code this will join all tables
- Build up/down
- In order to run everything type this: dbt build --select +fact_trips+ --vars '{'is_test_run': false}'

### TESTS
- Inside core create a file called dm_monthly_zone_revenue.sql copy and paste code
- Install codegen package in packages.yml in order to use generate_model_yaml(source)
- Create a new tab using the + sign and adapt the code

```yml
{% set models_to_generate = codegen.get_models(directory='staging', prefix='stg') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}
```

- Compile it so it will give a code to copy models and paste in schema.yml
- Under tripid add tests
```yml
models:
  - name: stg_green_tripdata
    description: ""
    columns:
      - name: tripid
        data_type: string
        description: ""
        tests:
          - unique:
              severity: warn
          - not_null:
              severity: warn
              
      - name: vendorid
        data_type: int64
        description: ""
```

- Under pickup_locationid and dropoff_locationid add tests

```yml
 - name: pickup_locationid
        data_type: int64
        description: ""
        tests:
          - relationships:
              field: locationid
              to: ref('taxi_zones_lookup')
              severity: warn

      - name: dropoff_locationid
        data_type: int64
        description: ""
        tests:
          - relationships:
              field: locationid
              to: ref('taxi_zones_lookup')
              severity: warn
```
- Create a variable in project.yml

```yml
vars:
  payment_type_values: [1, 2, 3, 4, 5, 6]
```
- Under payment_type add tests

```yml
- name: payment_type
        data_type: int64
        description: ""
        tests:
          - accepted_values:
              values: "{{ var('payment_type_values') }}"
              severity: warn
              quote: false
```

- Create a schema.yml for core, use the untitled file with the macro to generate the code, paste code below and compile, copy and paste in schema.yml

```yml
{% set models_to_generate = codegen.get_models(directory='core') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}
```

- Or copy and paste schema.yml with descriptions already made
- Type: dbt docs generate
- To view docs using dbt in the cloud click on the little book top left corner

### DEPLOYMENT
- Deploy tab>Environments
- Create new environment
- Add Environment name(production) and Dataset(prod)
- Create job>Deploy job
- Add Job name(Nightly), Description(This is where data hits production)
- Under Execution settings, tick boxes Generate docs and Run source
- Under Schedule choose a schedule
- Save it and run to trigger it, it is also possible to trigger it by api so is possible to trigger it using an orchestrator


