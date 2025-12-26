SET yarn.nodemanager.pmem-check-enabled=false;
SET yarn.nodemanager.vmem-check-enabled=false;
SET hive.mapred.mode=nonstrict;

USE crime_analysis;

WITH latest_version AS (
    SELECT MAX(versionid) AS max_vid FROM crime_incidents
),
hourly_pattern AS (
    SELECT 
        HOUR(start_time) AS hour_of_day,
        COUNT(*) AS hourly_crimes,
        SUM(COALESCE(victims, 0)) AS hourly_victims,
        ROUND(AVG(COALESCE(victims, 0)), 2) AS avg_victims_per_hour,
        COUNT(DISTINCT state) AS states_affected
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid AND start_time IS NOT NULL
    GROUP BY HOUR(start_time)
),
weekly_pattern AS (
    SELECT 
        DAYOFWEEK(start_time) AS day_of_week,
        CASE DAYOFWEEK(start_time)
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END AS day_name,
        COUNT(*) AS daily_crimes
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid AND start_time IS NOT NULL
    GROUP BY DAYOFWEEK(start_time)
),
quarterly_pattern AS (
    SELECT 
        QUARTER(start_time) AS quarter,
        CONCAT('Q', QUARTER(start_time)) AS quarter_name,
        COUNT(*) AS quarterly_crimes
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid AND start_time IS NOT NULL
    GROUP BY QUARTER(start_time)
),
peak_hours AS (
    SELECT 
        hour_of_day,
        hourly_crimes,
        hourly_victims,
        avg_victims_per_hour,
        states_affected,
        RANK() OVER (ORDER BY hourly_crimes DESC) AS crime_rank,
        RANK() OVER (ORDER BY hourly_victims DESC) AS victim_rank
    FROM hourly_pattern
),
peak_day_info AS (
    SELECT day_name, daily_crimes
    FROM weekly_pattern
    ORDER BY daily_crimes DESC
    LIMIT 1
),
peak_quarter_info AS (
    SELECT quarter_name AS peak_quarter, quarterly_crimes
    FROM quarterly_pattern
    ORDER BY quarterly_crimes DESC
    LIMIT 1
),
hourly_avg AS (
    SELECT AVG(hourly_crimes) AS avg_hourly_crimes FROM hourly_pattern
)
SELECT 
    h.hour_of_day,
    h.hourly_crimes,
    h.hourly_victims,
    h.avg_victims_per_hour,
    h.states_affected,
    h.crime_rank AS hour_crime_rank,
    h.victim_rank AS hour_victim_rank,
    p.day_name AS peak_day_name,
    p.daily_crimes AS peak_day_crimes,
    q.peak_quarter,
    q.quarterly_crimes AS peak_quarter_crimes,
    CASE 
        WHEN h.hour_of_day >= 6 AND h.hour_of_day < 12 THEN 'Morning'
        WHEN h.hour_of_day >= 12 AND h.hour_of_day < 18 THEN 'Afternoon'
        WHEN h.hour_of_day >= 18 AND h.hour_of_day < 24 THEN 'Evening'
        ELSE 'Night'
    END AS time_period,
    CASE 
        WHEN h.hourly_crimes > a.avg_hourly_crimes THEN 'Above Average'
        WHEN h.hourly_crimes < a.avg_hourly_crimes THEN 'Below Average'
        ELSE 'Average'
    END AS crime_intensity
FROM peak_hours h
CROSS JOIN hourly_avg a
CROSS JOIN peak_day_info p
CROSS JOIN peak_quarter_info q
ORDER BY h.hour_of_day;
