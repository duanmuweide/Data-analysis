SET yarn.nodemanager.pmem-check-enabled=false;
SET yarn.nodemanager.vmem-check-enabled=false;
SET hive.mapred.mode=nonstrict;

USE crime_analysis;

WITH latest_version AS (
    SELECT MAX(versionid) AS max_vid FROM crime_incidents
),
base AS (
    SELECT 
        victims,
        city,
        state,
        start_time
    FROM crime_incidents, latest_version
    WHERE versionid = max_vid
      AND start_time IS NOT NULL
),
monthly_stats AS (
    SELECT 
        TRUNC(start_time, 'MM') AS report_month,
        COUNT(*) AS total_crimes,
        SUM(COALESCE(victims, 0)) AS total_victims,
        ROUND(AVG(COALESCE(victims, 0)), 2) AS avg_victims_per_crime,
        COUNT(DISTINCT city) AS affected_cities,
        COUNT(DISTINCT state) AS affected_states
    FROM base
    GROUP BY TRUNC(start_time, 'MM')
),
daily_in_month AS (
    SELECT 
        TRUNC(start_time, 'MM') AS report_month,
        TO_DATE(start_time) AS day_date,
        COUNT(*) AS daily_crimes
    FROM base
    GROUP BY TRUNC(start_time, 'MM'), TO_DATE(start_time)
),
peak_days AS (
    SELECT 
        report_month,
        MAX(daily_crimes) AS peak_day_crimes,
        MIN(daily_crimes) AS lowest_day_crimes
    FROM daily_in_month
    GROUP BY report_month
),
with_prev AS (
    SELECT *,
        LAG(total_crimes) OVER (ORDER BY report_month) AS prev_crimes,
        LAG(total_victims) OVER (ORDER BY report_month) AS prev_victims
    FROM monthly_stats
)
SELECT 
    w.report_month,
    w.total_crimes,
    w.total_victims,
    w.avg_victims_per_crime,
    w.affected_cities,
    w.affected_states,
    CASE 
        WHEN w.prev_crimes IS NULL THEN 'N/A'
        WHEN w.prev_crimes = 0 THEN 'Infinity'
        ELSE CONCAT(ROUND((w.total_crimes - w.prev_crimes) * 100.0 / w.prev_crimes, 2), '%')
    END AS crime_growth_rate,
    CASE 
        WHEN w.prev_victims IS NULL THEN 'N/A'
        WHEN w.prev_victims = 0 THEN 'Infinity'
        ELSE CONCAT(ROUND((w.total_victims - w.prev_victims) * 100.0 / w.prev_victims, 2), '%')
    END AS victim_growth_rate,
    p.peak_day_crimes,
    p.lowest_day_crimes
FROM with_prev w
JOIN peak_days p ON w.report_month = p.report_month
ORDER BY w.report_month;
