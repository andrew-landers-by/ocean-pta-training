
-- Confirm upload to OCEAN_JOURNEY_FEATURES
SELECT TOP 100 * FROM [ocean_vessel_movement].[OCEAN_JOURNEY_FEATURES];
SELECT COUNT(*) FROM [ocean_vessel_movement].[OCEAN_JOURNEY_FEATURES];


-- Confirm upload to OCEAN_JOURNEY_RESPONSE
SELECT TOP 100 * FROM [ocean_vessel_movement].[OCEAN_JOURNEY_RESPONSE];
SELECT COUNT(*) FROM [ocean_vessel_movement].[OCEAN_JOURNEY_RESPONSE];


-- Confirm upload to OCEAN_VESSELS
SELECT TOP 100 * FROM [ocean_vessel_movement].[OCEAN_VESSELS];
SELECT COUNT(*) FROM [ocean_vessel_movement].[OCEAN_VESSELS];


-- Confirm upload to OCEAN_PORTS
SELECT TOP 100 * FROM [ocean_vessel_movement].[OCEAN_PORTS];
SELECT COUNT(*) FROM [ocean_vessel_movement].[OCEAN_PORTS];


------------------------------------------------------------
-- Confirm derived data in OCEAN_JOURNEYS (training data) --
------------------------------------------------------------

-- Row counts and first 100 records
SELECT COUNT(*) FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]; --9,016,682
SELECT TOP 100 * FROM [ocean_vessel_movement].[OCEAN_JOURNEYS] ORDER BY IMO, OD, unique_route_id, elapsed_time;


-- Frequency of various origin regions (by # of journeys)
WITH J AS (
    SELECT DISTINCT IMO, OD, unique_route_id, origin_region
    FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]
)
SELECT origin_region, count(*) AS frequency
FROM J
GROUP BY origin_region
ORDER BY count(*) DESC;

--Frequency of various destination regions (by # of journeys)
WITH J AS (
    SELECT DISTINCT IMO, OD, unique_route_id, destination_region
    FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]
)
SELECT destination_region, count(*) AS frequency
FROM J
GROUP BY destination_region
ORDER BY count(*) DESC;

--Frequency of various (origin_region, destination_region)
WITH J AS (
    SELECT DISTINCT IMO, OD, unique_route_id, origin_region, destination_region
    FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]
)
SELECT origin_region, destination_region, count(*) as frequency
FROM J
GROUP BY origin_region, destination_region
ORDER BY count(*) DESC;

--Calculate the portion of unique journeys that are within-region
WITH J AS (
    SELECT DISTINCT IMO, OD, unique_route_id, within_region_journey
    FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]
)
SELECT ROUND(100*CAST(SUM(within_region_journey) AS FLOAT)/CAST(SUM(1) AS FLOAT), 1)
FROM J;
--31.8% of journeys are within the same global region.


-- Number of distinct IMOs
SELECT COUNT(DISTINCT IMO) FROM [ocean_vessel_movement].[OCEAN_JOURNEYS];

-- Number of distinct ODs
SELECT COUNT(DISTINCT OD) FROM [ocean_vessel_movement].[OCEAN_JOURNEYS];

-- Number of distinct (IMO, OD, unique_route_id) that is, number of ocean journeys (1 IMO, 1 OD, 1 route)
SELECT COUNT(*) FROM (SELECT DISTINCT IMO, OD, unique_route_id FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]) tmp;

-- Describe
WITH JourneyObs AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY IMO, unique_route_id ORDER BY elapsed_time) AS journey_obs
    FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]
    WHERE IMO=7907984 AND unique_route_id=9 -- Subset to 1 journey to test query logic
),
LastOceanDistance AS (
    SELECT *,
       (
           SELECT ocean_distance FROM JourneyObs AS JO
            WHERE JO.IMO=JourneyObs.IMO
            AND JO.unique_route_id=JourneyObs.unique_route_id
            AND JO.journey_obs=JourneyObs.unique_route_id - 1
        ) AS last_ocean_distance
    FROM JourneyObs
)
SELECT TOP 1000 *
FROM LastOceanDistance
ORDER BY IMO, unique_route_id, journey_obs;

-- Describe the marginal distribution of remaining_lead_time
WITH A1 AS (SELECT ROUND(remaining_lead_time, 4) AS remaining_lead_time FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]),
     A2 AS (SELECT DISTINCT remaining_lead_time FROM A1),
     Percentiles AS (SELECT remaining_lead_time, PERCENT_RANK() OVER(ORDER BY remaining_lead_time) AS percent_rank FROM A2)
SELECT
    *
FROM Percentiles
ORDER BY remaining_lead_time
;

-- Describe the marginal distribution of remaining_lead_time
WITH A1 AS (SELECT ROUND(remaining_lead_time, 4) AS remaining_lead_time FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]),
     A2 AS (SELECT DISTINCT remaining_lead_time FROM A1),
     Percentiles AS (SELECT remaining_lead_time, PERCENT_RANK() OVER(ORDER BY remaining_lead_time) AS percent_rank FROM A2)
SELECT
    *
FROM Percentiles
ORDER BY remaining_lead_time
;

-- Frequency of observation, by route
SELECT OD,origin_port, destination_port, count(distinct unique_route_id) as count_journies
FROM [ocean_vessel_movement].[OCEAN_JOURNEYS]
GROUP BY OD,origin_port, destination_port
ORDER BY count_journies DESC;

--
SELECT MIN(remaining_lead_time) FROM [ocean_vessel_movement].[OCEAN_JOURNEYS];
