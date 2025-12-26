SET yarn.nodemanager.pmem-check-enabled=false;
SET yarn.nodemanager.vmem-check-enabled=false;
SET hive.mapred.mode=nonstrict;
USE crime_analysis;

WITH latest_version AS (
    SELECT MAX(versionid) AS max_vid FROM crime_incidents
),
city_level AS (
    SELECT 
        state,
        city,
        COUNT(*) AS city_crimes
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid AND city IS NOT NULL AND state IS NOT NULL
    GROUP BY state, city
),
state_level AS (
    SELECT 
        state,
        COUNT(*) AS state_crimes,
        SUM(COALESCE(victims, 0)) AS state_victims,
        COUNT(DISTINCT city) AS cities_affected,
        COUNT(DISTINCT zip_code) AS zip_codes_affected,
        ROUND(AVG(COALESCE(victims, 0)), 2) AS avg_victims_per_crime,
        ROUND(STDDEV(COALESCE(victims, 0)), 2) AS victim_stddev
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid AND state IS NOT NULL
    GROUP BY state
),
state_rankings AS (
    SELECT 
        state,
        state_crimes,
        state_victims,
        cities_affected,
        zip_codes_affected,
        avg_victims_per_crime,
        victim_stddev,
        RANK() OVER (ORDER BY state_crimes DESC) AS state_crime_rank,
        RANK() OVER (ORDER BY state_victims DESC) AS state_victim_rank,
        RANK() OVER (ORDER BY cities_affected DESC) AS state_city_count_rank
    FROM state_level
),
top_city_per_state AS (
    SELECT 
        state,
        city AS top_city_in_state,
        city_crimes AS top_city_crimes,
        ROW_NUMBER() OVER (PARTITION BY state ORDER BY city_crimes DESC) AS rn
    FROM city_level
),
max_city_crimes AS (
    SELECT MAX(city_crimes) AS max_crimes FROM city_level
),
overall_top_city_agg AS (
    SELECT 
        MAX(city) AS overall_top_city,
        MAX(city_crimes) AS overall_top_city_crimes
    FROM city_level
    WHERE city_crimes = (SELECT max_crimes FROM max_city_crimes)
    LIMIT 1
),
max_state_crimes AS (
    SELECT MAX(state_crimes) AS max_crimes FROM state_level
),
highest_crime_state_agg AS (
    SELECT 
        MAX(state) AS highest_crime_state,
        MAX(state_crimes) AS highest_state_crimes
    FROM state_level
    WHERE state_crimes = (SELECT max_crimes FROM max_state_crimes)
    LIMIT 1
),
state_avg_crimes AS (
    SELECT AVG(state_crimes) AS avg_crimes FROM state_level
)
SELECT 
    s.state,
    s.state_crimes,
    s.state_victims,
    s.cities_affected,
    s.zip_codes_affected,
    s.avg_victims_per_crime,
    s.victim_stddev,
    s.state_crime_rank,
    s.state_victim_rank,
    s.state_city_count_rank,
    tc.top_city_in_state,
    tc.top_city_crimes,
    ot.overall_top_city,
    ot.overall_top_city_crimes,
    hc.highest_crime_state,
    hc.highest_state_crimes,
    CASE 
        WHEN s.state_crimes > sa.avg_crimes * 1.5 THEN 'High'
        WHEN s.state_crimes > sa.avg_crimes THEN 'Medium'
        ELSE 'Low'
    END AS crime_level
FROM state_rankings s
LEFT JOIN top_city_per_state tc ON s.state = tc.state AND tc.rn = 1
CROSS JOIN overall_top_city_agg ot
CROSS JOIN highest_crime_state_agg hc
CROSS JOIN state_avg_crimes sa
ORDER BY s.state_crimes DESC;
