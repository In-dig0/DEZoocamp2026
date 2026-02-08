#HOMEWORK WEEK3 - DATAWAREHOUSE# 

**Question 1. What is count of records for the 2024 Yellow Taxi Data? (1 point)**

SELECT  count(1) FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024_ext` LIMIT 1000

**Question 2. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? (1 point)**

SELECT DISTINCT(PULocationID) FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024_ext`;

SELECT DISTINCT(PULocationID) FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024`;

**Question 4. How many records have a fare_amount of 0? (1 point)**
SELECT COUNT(1)
FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024`
WHERE fare_amount = 0;

**Question 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy) (1 point)**

CREATE OR REPLACE TABLE `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024`
)

SELECT *
FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024_partitioned`
LIMIT 10;

**Question 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the --materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the  partitioned table you created for question 5 and note the estimated bytes processed. What are these values? (1 point)**

SELECT DISTINCT(VendorID)
FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024`
WHERE DATE(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15'

SELECT DISTINCT(VendorID)
FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024_partitioned`
WHERE DATE(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15'

**Question 9. Write a "SELECT count(*)" query FROM the materialized table you created. How many bytes does it estimate will be read? Why? (not graded)**

SELECT count(*)
FROM `nodal-nirvana-485604-k6.DEZoocamp_demo_dataset.yellow_tripdata_2024`