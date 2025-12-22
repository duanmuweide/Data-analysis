-- =======================================================================
-- 五个简单Hive SQL查询
-- =======================================================================

-- 查询 1: 按版本统计犯罪类型分布
SELECT 
    versionid,
    crime_name1,
    COUNT(*) as incident_count,
    AVG(victims) as avg_victims
FROM crime_analysis_simple.crime_incidents
WHERE year >= 2020 
    AND versionid IN (1, 2, 3)
    AND crime_name1 IS NOT NULL
GROUP BY versionid, crime_name1
ORDER BY versionid, incident_count DESC
LIMIT 20;

-- 查询 2: 各城市犯罪情况对比
SELECT 
    city,
    versionid,
    COUNT(*) as total_crimes,
    SUM(victims) as total_victims,
    AVG(victims) as avg_victims_per_crime
FROM crime_analysis_simple.crime_incidents
WHERE year BETWEEN 2020 AND 2022
    AND versionid IN (1, 2, 3)
    AND city IS NOT NULL
GROUP BY city, versionid
ORDER BY city, versionid;

-- 查询 3: 版本间执法机构表现
SELECT 
    versionid,
    agency,
    COUNT(*) as case_count,
    AVG(victims) as avg_victims,
    MAX(victims) as max_victims
FROM crime_analysis_simple.crime_incidents
WHERE year = 2021
    AND versionid IN (1, 2, 3)
    AND agency IS NOT NULL
GROUP BY versionid, agency
ORDER BY versionid, case_count DESC
LIMIT 15;

-- 查询 4: 不同警区犯罪分析
SELECT 
    police_district,
    versionid,
    crime_name1,
    COUNT(*) as crime_count,
    ROUND(AVG(victims), 2) as avg_victims
FROM crime_analysis_simple.crime_incidents
WHERE year >= 2020
    AND versionid IN (1, 2, 3)
    AND police_district IS NOT NULL
    AND crime_name1 IS NOT NULL
GROUP BY police_district, versionid, crime_name1
HAVING COUNT(*) >= 5
ORDER BY versionid, crime_count DESC
LIMIT 25;

-- 查询 5: 时间维度犯罪趋势
SELECT 
    versionid,
    YEAR(start_time) as year,
    MONTH(start_time) as month,
    COUNT(*) as monthly_incidents,
    AVG(victims) as avg_monthly_victims
FROM crime_analysis_simple.crime_incidents
WHERE start_time IS NOT NULL
    AND year >= 2020
    AND versionid IN (1, 2, 3)
GROUP BY versionid, YEAR(start_time), MONTH(start_time)
ORDER BY versionid, year, month;



