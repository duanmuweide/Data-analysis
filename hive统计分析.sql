-- Hive统计分析SQL语句（带版本控制和增量更新）

-- 使用的数据库
USE crime_analysis;

-- 创建版本控制表
CREATE TABLE IF NOT EXISTS analysis_version (
    version_id INT AUTO_INCREMENT,
    analysis_name STRING,
    last_processed_date TIMESTAMP,
    data_date_from TIMESTAMP,
    data_date_to TIMESTAMP,
    record_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (version_id)
) COMMENT '记录每次分析的版本信息，用于增量更新';

-- 创建结果表：按城市和犯罪类型统计
CREATE TABLE IF NOT EXISTS crime_by_city_type (
    city STRING,
    crime_name1 STRING,
    crime_count INT,
    avg_victims DOUBLE,
    max_victims INT,
    version_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, crime_name1, version_id)
) COMMENT '按城市和犯罪类型统计的犯罪数量';

-- 1. 按城市和犯罪类型统计犯罪数量（使用分组和内置函数）
-- 统计每个城市中各主要犯罪类型的发生次数，并按犯罪数量降序排列
WITH latest_version AS (
    SELECT MAX(version_id) as last_version
    FROM analysis_version
    WHERE analysis_name = 'crime_by_city_type'
),
new_data AS (
    SELECT *
    FROM crime_incidents
    WHERE 
        (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version)) IS NULL
        OR dispatch_time > (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version))
)
INSERT INTO crime_by_city_type
SELECT 
    city, 
    crime_name1, 
    COUNT(*) as crime_count,
    AVG(victims) as avg_victims,
    MAX(victims) as max_victims,
    (SELECT COALESCE(last_version, 0) + 1 FROM latest_version) as version_id
FROM new_data
GROUP BY city, crime_name1;

-- 记录新版本信息
INSERT INTO analysis_version
SELECT 
    (SELECT COALESCE(MAX(version_id), 0) + 1 FROM analysis_version),
    'crime_by_city_type',
    CURRENT_TIMESTAMP,
    (SELECT MIN(dispatch_time) FROM new_data),
    (SELECT MAX(dispatch_time) FROM new_data),
    (SELECT COUNT(*) FROM new_data),
    CURRENT_TIMESTAMP;

-- 查询最新版本结果
SELECT 
    city, 
    crime_name1, 
    crime_count,
    avg_victims,
    max_victims,
    version_id,
    created_at
FROM crime_by_city_type
WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_by_city_type')
ORDER BY city, crime_count DESC;

-- 创建结果表：每月犯罪率最高的前5个城市
CREATE TABLE IF NOT EXISTS top_crime_cities_monthly (
    year INT,
    month INT,
    city STRING,
    crime_count INT,
    rank INT,
    version_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year, month, city, version_id)
) COMMENT '每月犯罪率最高的前5个城市';

-- 2. 统计每月犯罪率最高的前5个城市（使用子查询和内置函数）
-- 计算每个月每个城市的犯罪率，并找出每月犯罪率最高的前5个城市
WITH latest_version AS (
    SELECT MAX(version_id) as last_version
    FROM analysis_version
    WHERE analysis_name = 'top_crime_cities_monthly'
),
new_data AS (
    SELECT *
    FROM crime_incidents
    WHERE 
        (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version)) IS NULL
        OR dispatch_time > (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version))
),
monthly_crime AS (
    SELECT 
        year, 
        month, 
        city, 
        COUNT(*) as crime_count,
        ROW_NUMBER() OVER(PARTITION BY year, month ORDER BY COUNT(*) DESC) as rank
    FROM new_data
    GROUP BY year, month, city
)
INSERT INTO top_crime_cities_monthly
SELECT 
    year, 
    month, 
    city, 
    crime_count,
    rank,
    (SELECT COALESCE(last_version, 0) + 1 FROM latest_version) as version_id
FROM monthly_crime
WHERE rank <= 5;

-- 记录新版本信息
INSERT INTO analysis_version
SELECT 
    (SELECT COALESCE(MAX(version_id), 0) + 1 FROM analysis_version),
    'top_crime_cities_monthly',
    CURRENT_TIMESTAMP,
    (SELECT MIN(dispatch_time) FROM new_data),
    (SELECT MAX(dispatch_time) FROM new_data),
    (SELECT COUNT(*) FROM new_data),
    CURRENT_TIMESTAMP;

-- 查询最新版本结果
SELECT year, month, city, crime_count, rank
FROM top_crime_cities_monthly
WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'top_crime_cities_monthly')
ORDER BY year, month, rank;

-- 创建结果表：犯罪时间模式分析
CREATE TABLE IF NOT EXISTS crime_time_patterns (
    hour_of_day INT,
    crime_count INT,
    affected_cities INT,
    avg_duration_minutes DOUBLE,
    version_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hour_of_day, version_id)
) COMMENT '按小时统计的犯罪时间模式';

-- 3. 分析犯罪时间模式（使用内置函数和分组）
-- 按小时统计犯罪发生次数，分析犯罪的时间分布规律
WITH latest_version AS (
    SELECT MAX(version_id) as last_version
    FROM analysis_version
    WHERE analysis_name = 'crime_time_patterns'
),
new_data AS (
    SELECT *
    FROM crime_incidents
    WHERE 
        (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version)) IS NULL
        OR dispatch_time > (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version))
)
INSERT INTO crime_time_patterns
SELECT 
    HOUR(dispatch_time) as hour_of_day,
    COUNT(*) as crime_count,
    COUNT(DISTINCT city) as affected_cities,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)) as avg_duration_minutes,
    (SELECT COALESCE(last_version, 0) + 1 FROM latest_version) as version_id
FROM new_data
GROUP BY HOUR(dispatch_time);

-- 记录新版本信息
INSERT INTO analysis_version
SELECT 
    (SELECT COALESCE(MAX(version_id), 0) + 1 FROM analysis_version),
    'crime_time_patterns',
    CURRENT_TIMESTAMP,
    (SELECT MIN(dispatch_time) FROM new_data),
    (SELECT MAX(dispatch_time) FROM new_data),
    (SELECT COUNT(*) FROM new_data),
    CURRENT_TIMESTAMP;

-- 查询最新版本结果
SELECT 
    hour_of_day,
    crime_count,
    affected_cities,
    avg_duration_minutes
FROM crime_time_patterns
WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_time_patterns')
ORDER BY hour_of_day;

-- 创建结果表：分桶表统计结果
CREATE TABLE IF NOT EXISTS crime_bucketed_stats (
    city STRING,
    crime_name1 STRING,
    crime_count INT,
    version_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, crime_name1, version_id)
) COMMENT '使用分桶表统计的犯罪类型分布';

-- 4. 使用分桶表进行高效查询（使用分桶和表连接）
-- 统计每个城市的犯罪类型分布，并与该城市的基本信息进行连接
WITH latest_version AS (
    SELECT MAX(version_id) as last_version
    FROM analysis_version
    WHERE analysis_name = 'crime_bucketed_stats'
),
new_data AS (
    SELECT c.*
    FROM crime_incidents c
    WHERE 
        (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version)) IS NULL
        OR c.dispatch_time > (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version))
)
INSERT INTO crime_bucketed_stats
SELECT 
    b.city, 
    c.crime_name1, 
    COUNT(*) as crime_count,
    (SELECT COALESCE(last_version, 0) + 1 FROM latest_version) as version_id
FROM new_data c
JOIN crime_incidents_bucketed b ON c.city = b.city
GROUP BY b.city, c.crime_name1;

-- 记录新版本信息
INSERT INTO analysis_version
SELECT 
    (SELECT COALESCE(MAX(version_id), 0) + 1 FROM analysis_version),
    'crime_bucketed_stats',
    CURRENT_TIMESTAMP,
    (SELECT MIN(dispatch_time) FROM new_data),
    (SELECT MAX(dispatch_time) FROM new_data),
    (SELECT COUNT(*) FROM new_data),
    CURRENT_TIMESTAMP;

-- 查询最新版本结果
SELECT 
    city, 
    crime_name1, 
    crime_count
FROM crime_bucketed_stats
WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_bucketed_stats')
ORDER BY city, crime_count DESC;

-- 创建结果表：犯罪地点类型分析
CREATE TABLE IF NOT EXISTS crime_by_place_type (
    place STRING,
    crime_count INT,
    avg_victims DOUBLE,
    total_victims INT,
    total_yearly_crime INT,
    percentage DOUBLE,
    version_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (place, version_id)
) COMMENT '按地点类型统计的犯罪分析';

-- 5. 分析犯罪地点类型（使用子查询和内置函数）
-- 统计不同地点类型的犯罪发生次数，并计算每种地点类型的平均受害者数量
WITH latest_version AS (
    SELECT MAX(version_id) as last_version
    FROM analysis_version
    WHERE analysis_name = 'crime_by_place_type'
),
new_data AS (
    SELECT *
    FROM crime_incidents
    WHERE 
        (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version)) IS NULL
        OR dispatch_time > (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version))
),
total_stats AS (
    SELECT COUNT(*) as total_crime
    FROM crime_incidents
    WHERE year = YEAR(CURRENT_DATE)
)
INSERT INTO crime_by_place_type
SELECT 
    place, 
    COUNT(*) as crime_count,
    AVG(victims) as avg_victims,
    SUM(victims) as total_victims,
    (SELECT total_crime FROM total_stats) as total_yearly_crime,
    (COUNT(*) / (SELECT total_crime FROM total_stats)) * 100 as percentage,
    (SELECT COALESCE(last_version, 0) + 1 FROM latest_version) as version_id
FROM new_data
GROUP BY place
HAVING COUNT(*) > 100;

-- 记录新版本信息
INSERT INTO analysis_version
SELECT 
    (SELECT COALESCE(MAX(version_id), 0) + 1 FROM analysis_version),
    'crime_by_place_type',
    CURRENT_TIMESTAMP,
    (SELECT MIN(dispatch_time) FROM new_data),
    (SELECT MAX(dispatch_time) FROM new_data),
    (SELECT COUNT(*) FROM new_data),
    CURRENT_TIMESTAMP;

-- 查询最新版本结果
SELECT 
    place, 
    crime_count,
    avg_victims,
    total_victims,
    total_yearly_crime,
    percentage
FROM crime_by_place_type
WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_by_place_type')
ORDER BY crime_count DESC;

-- 创建结果表：犯罪与受害者数量关系分析
CREATE TABLE IF NOT EXISTS crime_victims_analysis (
    victims INT,
    case_count INT,
    crime_types STRING,
    affected_cities INT,
    version_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (victims, version_id)
) COMMENT '不同受害者数量的犯罪案件分布';

-- 6. 分析犯罪与受害者数量的关系（使用内置函数和分组）
-- 统计不同受害者数量的犯罪案件分布
WITH latest_version AS (
    SELECT MAX(version_id) as last_version
    FROM analysis_version
    WHERE analysis_name = 'crime_victims_analysis'
),
new_data AS (
    SELECT *
    FROM crime_incidents
    WHERE 
        (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version)) IS NULL
        OR dispatch_time > (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version))
)
INSERT INTO crime_victims_analysis
SELECT 
    victims, 
    COUNT(*) as case_count,
    STRING_AGG(DISTINCT crime_name1, ', ') as crime_types,
    COUNT(DISTINCT city) as affected_cities,
    (SELECT COALESCE(last_version, 0) + 1 FROM latest_version) as version_id
FROM new_data
WHERE victims IS NOT NULL
GROUP BY victims;

-- 记录新版本信息
INSERT INTO analysis_version
SELECT 
    (SELECT COALESCE(MAX(version_id), 0) + 1 FROM analysis_version),
    'crime_victims_analysis',
    CURRENT_TIMESTAMP,
    (SELECT MIN(dispatch_time) FROM new_data),
    (SELECT MAX(dispatch_time) FROM new_data),
    (SELECT COUNT(*) FROM new_data),
    CURRENT_TIMESTAMP;

-- 查询最新版本结果
SELECT 
    victims, 
    case_count,
    crime_types,
    affected_cities
FROM crime_victims_analysis
WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_victims_analysis')
ORDER BY victims;

-- 创建结果表：基于分区的犯罪趋势分析
CREATE TABLE IF NOT EXISTS crime_trend_analysis (
    year INT,
    month INT,
    city STRING,
    monthly_crime INT,
    previous_month INT,
    change INT,
    change_percentage DOUBLE,
    version_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year, month, city, version_id)
) COMMENT '每个城市的犯罪数量月度变化趋势';

-- 7. 基于分区的犯罪趋势分析（使用分区和窗口函数）
-- 分析每个城市的犯罪数量月度变化趋势
WITH latest_version AS (
    SELECT MAX(version_id) as last_version
    FROM analysis_version
    WHERE analysis_name = 'crime_trend_analysis'
),
new_data AS (
    SELECT *
    FROM crime_incidents
    WHERE 
        (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version)) IS NULL
        OR dispatch_time > (SELECT data_date_to FROM analysis_version WHERE version_id = (SELECT last_version FROM latest_version))
),
monthly_crime AS (
    SELECT 
        year, 
        month, 
        city, 
        COUNT(*) as monthly_crime,
        LAG(COUNT(*), 1) OVER(PARTITION BY city ORDER BY year, month) as previous_month
    FROM new_data
    GROUP BY year, month, city
)
INSERT INTO crime_trend_analysis
SELECT 
    year, 
    month, 
    city, 
    monthly_crime,
    previous_month,
    (monthly_crime - previous_month) as change,
    CASE 
        WHEN previous_month IS NOT NULL AND previous_month > 0 
        THEN ((monthly_crime - previous_month) / previous_month) * 100 
        ELSE NULL 
    END as change_percentage,
    (SELECT COALESCE(last_version, 0) + 1 FROM latest_version) as version_id
FROM monthly_crime;

-- 记录新版本信息
INSERT INTO analysis_version
SELECT 
    (SELECT COALESCE(MAX(version_id), 0) + 1 FROM analysis_version),
    'crime_trend_analysis',
    CURRENT_TIMESTAMP,
    (SELECT MIN(dispatch_time) FROM new_data),
    (SELECT MAX(dispatch_time) FROM new_data),
    (SELECT COUNT(*) FROM new_data),
    CURRENT_TIMESTAMP;

-- 查询最新版本结果
SELECT 
    year, 
    month, 
    city, 
    monthly_crime,
    previous_month,
    change,
    change_percentage
FROM crime_trend_analysis
WHERE version_id = (SELECT MAX(version_id) FROM analysis_version WHERE analysis_name = 'crime_trend_analysis')
ORDER BY city, year, month;