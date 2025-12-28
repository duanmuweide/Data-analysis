-- HBase综合犯罪风险分析查询
-- 用法: hive -f query_hbase.sql

WITH crime_stats AS (
  SELECT 
    state,
    city,
    timestamp AS incident_date,
    COUNT(*) OVER (PARTITION BY state, city) AS city_total_crimes,
    SUM(victims) OVER (PARTITION BY state, city) AS city_total_victims,
    COUNT(*) OVER (PARTITION BY state) AS state_total_crimes,
    SUM(victims) OVER (PARTITION BY state) AS state_total_victims,
    victims
  FROM hive_crime_incidents
),
daily_stats AS (
  SELECT 
    state,
    city,
    incident_date,
    city_total_crimes,
    city_total_victims,
    state_total_crimes,
    state_total_victims,
    victims,
    ROUND(city_total_victims * 1.0 / city_total_crimes, 2) AS city_avg_victims,
    ROUND(state_total_victims * 1.0 / state_total_crimes, 2) AS state_avg_victims
  FROM crime_stats
),
risk_analysis AS (
  SELECT 
    state,
    city,
    incident_date,
    city_total_crimes,
    city_total_victims,
    state_total_crimes,
    state_total_victims,
    victims,
    city_avg_victims,
    state_avg_victims,
    CASE 
      WHEN city_total_crimes >= 3 AND city_avg_victims >= 2.0 THEN 'CRITICAL'
      WHEN city_total_crimes >= 2 AND city_avg_victims >= 1.5 THEN 'HIGH'
      WHEN city_total_crimes >= 2 OR city_avg_victims >= 1.5 THEN 'MEDIUM'
      ELSE 'LOW'
    END AS city_risk_level,
    CASE 
      WHEN state_total_crimes >= 5 AND state_avg_victims >= 2.0 THEN 'CRITICAL'
      WHEN state_total_crimes >= 3 AND state_avg_victims >= 1.5 THEN 'HIGH'
      WHEN state_total_crimes >= 2 OR state_avg_victims >= 1.5 THEN 'MEDIUM'
      ELSE 'LOW'
    END AS state_risk_level
  FROM daily_stats
)
SELECT 
  state,
  city,
  incident_date,
  city_total_crimes AS city_crime_count,
  city_total_victims AS city_victim_count,
  city_avg_victims AS city_avg_victims_per_crime,
  city_risk_level,
  state_total_crimes AS state_crime_count,
  state_total_victims AS state_victim_count,
  state_avg_victims AS state_avg_victims_per_crime,
  state_risk_level,
  CASE 
    WHEN city_risk_level = 'CRITICAL' OR state_risk_level = 'CRITICAL' THEN 'CRITICAL'
    WHEN city_risk_level = 'HIGH' OR state_risk_level = 'HIGH' THEN 'HIGH'
    WHEN city_risk_level = 'MEDIUM' OR state_risk_level = 'MEDIUM' THEN 'MEDIUM'
    ELSE 'LOW'
  END AS overall_risk_level,
  victims AS incident_victims
FROM risk_analysis
ORDER BY 
  CASE overall_risk_level
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
    ELSE 4
  END,
  city_total_crimes DESC,
  incident_date DESC;
