## Module 1 Homework

ATTENTION: At the very end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm` - THIS TAG


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0 - THIS VERSION
- 1.0.0
- 23.0.1
- 58.1.0


# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

```sql
SELECT 
	COUNT(1)
FROM 
	green_taxi_data
WHERE
	DATE(lpep_pickup_datetime)='2019-09-18' AND
	DATE(lpep_dropoff_datetime)='2019-09-18'
```

- 15767
- 15612 - TOTAL TRIPS
- 15859
- 89009

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.
Tip: For every trip on a single day, we only care about the trip with the longest distance. 

```sql
SELECT 
	DATE(lpep_pickup_datetime)
FROM 
	green_taxi_data
ORDER BY
	trip_distance DESC
LIMIT
	1
```


- 2019-09-18
- 2019-09-16
- 2019-09-26 - THIS DAY
- 2019-09-21


## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

```sql
SELECT 
	zpu."Borough",
    SUM(total_amount) AS total_amount_sum
FROM 
    green_taxi_data AS t
JOIN
    zones AS zpu ON t."PULocationID" = zpu."LocationID" 
WHERE
    DATE(lpep_pickup_datetime) = '2019-09-18'
GROUP BY
	zpu."Borough"
HAVING
    SUM(total_amount) > 50000
ORDER BY
    total_amount_sum DESC;
```
 
- "Brooklyn" "Manhattan" "Queens" - THESE CITIES
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"


## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

```sql
SELECT 
	zdo."Zone",
	tip_amount
FROM 
    green_taxi_data AS t
JOIN
    zones AS zpu ON t."PULocationID" = zpu."LocationID" 
JOIN
    zones AS zdo ON t."DOLocationID" = zdo."LocationID" 
WHERE
    zpu."Zone"='Astoria' AND
    EXTRACT(MONTH FROM lpep_pickup_datetime) = 9 AND
    EXTRACT(YEAR FROM lpep_pickup_datetime) = 2019
ORDER BY
	tip_amount DESC
LIMIT
	1
```

- Central Park
- Jamaica
- JFK Airport - THIS ZONE
- Long Island City/Queens Plaza



## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.

```
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the
following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.ny_rides will be created
  + resource "google_bigquery_dataset" "ny_rides" {
      + creation_time              = (known after apply)
      + dataset_id                 = "ny_rides"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "northamerica-northeast1"
      + max_time_travel_hours      = (known after apply)
      + project                    = "ny-rides-diegogutierrez"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.ny-rides-diegogutierrez-terraform-bucket will be created
  + resource "google_storage_bucket" "ny-rides-diegogutierrez-terraform-bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "NORTHAMERICA-NORTHEAST1"
      + name                        = "ny-rides-diegogutierrez-terraform-bucket"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 1
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.
```


## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET
