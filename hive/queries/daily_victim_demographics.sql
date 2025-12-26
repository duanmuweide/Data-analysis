SET yarn.nodemanager.pmem-check-enabled=false;
SET yarn.nodemanager.vmem-check-enabled=false;
SET hive.mapred.mode=nonstrict;
USE crime_analysis;

WITH latest_version AS (
    SELECT MAX(versionid) AS max_vid FROM crime_incidents
),
victim_distribution AS (
    SELECT 
        CASE 
            WHEN victims IS NULL THEN 'Unknown'
            WHEN victims = 0 THEN 'Zero'
            WHEN victims = 1 THEN 'Single'
            WHEN victims BETWEEN 2 AND 5 THEN 'Small_Group'
            WHEN victims BETWEEN 6 AND 10 THEN 'Medium_Group'
            ELSE 'Large_Group'
        END AS victim_category,
        COUNT(*) AS incident_count,
        SUM(COALESCE(victims, 0)) AS total_victims,
        ROUND(AVG(COALESCE(victims, 0)), 2) AS avg_victims,
        MIN(victims) AS min_victims,
        MAX(victims) AS max_victims
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid
    GROUP BY 
        CASE 
            WHEN victims IS NULL THEN 'Unknown'
            WHEN victims = 0 THEN 'Zero'
            WHEN victims = 1 THEN 'Single'
            WHEN victims BETWEEN 2 AND 5 THEN 'Small_Group'
            WHEN victims BETWEEN 6 AND 10 THEN 'Medium_Group'
            ELSE 'Large_Group'
        END
),
state_victim_stats AS (
    SELECT 
        state,
        SUM(COALESCE(victims, 0)) AS state_total_victims,
        ROUND(AVG(COALESCE(victims, 0)), 2) AS state_avg_victims
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid AND state IS NOT NULL
    GROUP BY state
),
overall_stats AS (
    SELECT 
        COUNT(*) AS total_incidents,
        SUM(COALESCE(victims, 0)) AS total_victims,
        ROUND(AVG(COALESCE(victims, 0)), 2) AS overall_avg_victims,
        MIN(victims) AS overall_min_victims,
        MAX(victims) AS overall_max_victims,
        PERCENTILE(CAST(COALESCE(victims, 0) AS BIGINT), 0.5) AS median_victims,
        PERCENTILE(CAST(COALESCE(victims, 0) AS BIGINT), 0.9) AS percentile_90
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid
),
highest_victim_state AS (
    SELECT 
        state,
        state_total_victims,
        state_avg_victims
    FROM state_victim_stats
    ORDER BY state_total_victims DESC
    LIMIT 1
)
SELECT 
    d.victim_category,
    d.incident_count,
    d.total_victims,
    d.avg_victims,
    COALESCE(d.min_victims, 0) AS min_victims,
    COALESCE(d.max_victims, 0) AS max_victims,
    ROUND(d.incident_count * 100.0 / o.total_incidents, 2) AS category_percentage,
    ROUND(d.total_victims * 100.0 / o.total_victims, 2) AS victim_percentage,
    h.state AS highest_victim_state,
    h.state_avg_victims AS highest_state_avg_victims,
    o.overall_avg_victims,
    COALESCE(o.overall_min_victims, 0) AS overall_min_victims,
    COALESCE(o.overall_max_victims, 0) AS overall_max_victims,
    ROUND(o.median_victims, 2) AS overall_median_victims,
    ROUND(o.percentile_90, 2) AS overall_90th_percentile
FROM victim_distribution d
CROSS JOIN overall_stats o
CROSS JOIN highest_victim_state h
ORDER BY d.incident_count DESC;
