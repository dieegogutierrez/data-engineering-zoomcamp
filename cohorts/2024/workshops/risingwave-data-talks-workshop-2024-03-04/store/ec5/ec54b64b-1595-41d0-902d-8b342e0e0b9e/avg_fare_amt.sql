ATTACH TABLE _ UUID '4aa6bec3-d4fa-4b71-a0ad-387f120e4fbc'
(
    `avg_fare_amount_per_min` Decimal(10, 0),
    `num_rides_per_min` Int64
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (avg_fare_amount_per_min, num_rides_per_min)
ORDER BY (avg_fare_amount_per_min, num_rides_per_min)
SETTINGS index_granularity = 8192
