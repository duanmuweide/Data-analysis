-- 【HQL-1】高风险犯罪热点城市分析（每日执行版）
-- 基于 quary1_High-risk crime.sql 修改，添加百分比计算

-- YARN内存检测配置（解决任务被kill问题）
SET yarn.nodemanager.pmem-check-enabled=false;
SET yarn.nodemanager.vmem-check-enabled=false;
SET hive.mapred.mode=nonstrict;


-- 加载UDF JAR包 (路径需根据实际情况调整)
ADD JAR /home/master/risk-udf-1.0.0.jar;
CREATE TEMPORARY FUNCTION risk_level AS 'com.qdu.hive.RiskLevelUDF';

USE crime_analysis;

WITH latest_version AS (
    SELECT MAX(versionid) AS max_vid
    FROM crime_incidents
),
high_risk_filtered AS (
    SELECT 
        UPPER(city) AS city_upper,
        city, -- 保留原始 city 用于后续关联
        state,
        zip_code,
        victims,
        start_time,
        latitude,
        longitude
    FROM crime_incidents, latest_version
    WHERE 
        versionid = max_vid
        AND year >= 2020          -- 从Q1继承的关键过滤
        AND city IS NOT NULL
        AND victims IS NOT NULL
        AND risk_level(victims) = 'High' -- 使用UDF
),
-- 按城市统计高风险事件（主结果）
grouped_high_risk AS (
    SELECT 
        city_upper AS city,
        state,
        zip_code,
        COUNT(*) AS high_risk_count, -- 字段名改为与Daily脚本一致
        AVG(victims) AS avg_victims,
        MAX(start_time) AS latest_incident,
        ROUND(AVG(latitude), 6) AS center_lat, -- 字段名改为与Daily脚本一致
        ROUND(AVG(longitude), 6) AS center_lon  -- 字段名改为与Daily脚本一致
    FROM high_risk_filtered
    GROUP BY city_upper, state, zip_code
),
-- 统计每个城市的总事件数（不分风险等级），并应用相同的年份和非空过滤
city_total_counts AS (
    SELECT 
        UPPER(city) AS city_upper,
        COUNT(*) AS total_incidents -- 字段名改为与Daily脚本一致
    FROM crime_incidents, latest_version
    WHERE 
        versionid = max_vid
        AND year >= 2020          -- 必须加上，保证分母一致
        AND city IS NOT NULL
    GROUP BY UPPER(city)
)
-- 最终结果：JOIN 两个聚合表，并计算百分比
SELECT 
    g.city,
    g.state,
    g.zip_code,
    g.high_risk_count,
    ROUND(g.avg_victims, 2) AS avg_victims, -- 保留Q1的AVG，但格式化为2位小数
    g.latest_incident,
    g.center_lat,
    g.center_lon,
    c.total_incidents,
    ROUND(g.high_risk_count * 100.0 / c.total_incidents, 2) AS high_risk_percentage -- 新增百分比
FROM grouped_high_risk g
JOIN city_total_counts c ON g.city = c.city_upper
WHERE g.high_risk_count >= 3 -- 采用Daily版本的筛选条件 (>=3)
ORDER BY g.high_risk_count DESC
LIMIT 50;