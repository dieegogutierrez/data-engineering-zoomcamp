## Week 3 Homework
ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>

```sql
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-diegogutierrez.ny_rides_green_2022.External table`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ny-rides-diegogutierrez-terra-bucket/green_taxi_2022/*']
);
```

Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

```sql
CREATE OR REPLACE TABLE `ny-rides-diegogutierrez.ny_rides_green_2022.green_2022_nonpartitioned_tripdata`
AS SELECT * FROM `ny-rides-diegogutierrez.ny_rides_green_2022.External table`;
```

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402 - THIS AMOUNT
- 1,936,423
- 253,647

```sql
SELECT COUNT(*) 
FROM `ny-rides-diegogutierrez.ny_rides_green_2022.green_2022_nonpartitioned_tripdata` 
```

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table - THESE AMOUNTS
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

```sql
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `ny-rides-diegogutierrez.ny_rides_green_2022.External table` 

SELECT COUNT(DISTINCT(PULocationID)) 
FROM `ny-rides-diegogutierrez.ny_rides_green_2022.green_2022_nonpartitioned_tripdata` 
```

## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622 - THIS MANY

```sql
SELECT COUNT(fare_amount) 
FROM `ny-rides-diegogutierrez.ny_rides_green_2022.green_2022_nonpartitioned_tripdata` 
WHERE fare_amount=0;
```

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID - THIS STRATEGY
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

```sql
CREATE OR REPLACE TABLE `ny-rides-diegogutierrez.ny_rides_green_2022.green_2022_partitioned_tripdata`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `ny-rides-diegogutierrez.ny_rides_green_2022.External table`
);
```

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

```sql
SELECT count(DISTINCT PULocationID) 
FROM  `ny-rides-diegogutierrez.ny_rides_green_2022.green_2022_nonpartitioned_tripdata`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'

SELECT count(DISTINCT PULocationID) 
FROM  `ny-rides-diegogutierrez.ny_rides_green_2022.green_2022_partitioned_tripdata`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
```

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table - THESE AMOUNTS
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query 
- GCP Bucket - HERE
- Big Table
- Container Registry


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False - THIS ANSWER - ONLY OVER 1GB. WITH LESS THAN THAT IT MAY OVERCHARGE AND NOT SHOW SIGNIFICANT IMPROVEMENT.


## (Bonus: Not worth points) Question 8:
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
- 0 BYTES. IT USES CACHE FOR SIMPLE QUERIES.

 
## Submitting the solutions

* Form for submitting: TBD
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: TBD


