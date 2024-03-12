# Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

```sql
CREATE MATERIALIZED VIEW trip_time AS
SELECT
    taxi_zone_pu.Zone AS pickup_zone,
    taxi_zone_do.Zone AS dropoff_zone,
    AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
    MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
    MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time
FROM
    trip_data
JOIN
    taxi_zone AS taxi_zone_pu ON trip_data.PULocationID = taxi_zone_pu.location_id
JOIN
    taxi_zone AS taxi_zone_do ON trip_data.DOLocationID = taxi_zone_do.location_id
GROUP BY
    pickup_zone,
    dropoff_zone;
```

Note that we consider the do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:
```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.
```sql
SELECT *
FROM trip_time
ORDER BY avg_trip_time DESC
LIMIT 10;
```

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

Options:
1. Yorkville East, Steinway - THIS ANSWER
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. `A -> B` and `B -> A` are considered different trips. This applies to subsequent questions as well.

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

```sql
CREATE MATERIALIZED VIEW trip_time_2 AS
SELECT
    taxi_zone_pu.Zone AS pickup_zone,
    taxi_zone_do.Zone AS dropoff_zone,
    AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
    MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
    MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time,
    COUNT(*) AS num_trips
FROM
    trip_data
JOIN
    taxi_zone AS taxi_zone_pu ON trip_data.PULocationID = taxi_zone_pu.location_id
JOIN
    taxi_zone AS taxi_zone_do ON trip_data.DOLocationID = taxi_zone_do.location_id
GROUP BY
    pickup_zone,
    dropoff_zone
ORDER BY
    avg_trip_time DESC
LIMIT 10;
```

Options:
1. 5
2. 3
3. 10
4. 1 - THIS ANSWER

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

```sql
WITH PickupCounts AS (
    SELECT
        taxi_zone.Zone AS pickup_zone,
        COUNT(*) AS pickup_count
    FROM
        trip_data
    JOIN
        taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
    WHERE
        tpep_pickup_datetime >= (SELECT MAX(tpep_pickup_datetime) FROM trip_data) - INTERVAL '17' HOUR
        AND tpep_pickup_datetime <= (SELECT MAX(tpep_pickup_datetime) FROM trip_data)
    GROUP BY
        pickup_zone
)
SELECT
    pickup_zone,
    pickup_count
FROM
    PickupCounts
ORDER BY
    pickup_count DESC
LIMIT 3;
```

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport - THIS ANSWER
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North