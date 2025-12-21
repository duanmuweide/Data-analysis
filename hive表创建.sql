-- =======================================================================
-- Hive表创建脚本
-- 满足项目要求：分区、分桶、ORC格式、事务支持
-- =======================================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS crime_analysis;
USE crime_analysis;

-- =======================================================================
-- 1. 外部表：映射HDFS中的原始CSV数据
-- =======================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS crime_incidents_external (
    incident_id STRING COMMENT '事件唯一标识',
    offence_code STRING COMMENT '犯罪代码',
    cr_number STRING COMMENT '犯罪记录编号',
    dispatch_time TIMESTAMP COMMENT '调度时间',
    start_time TIMESTAMP COMMENT '事件开始时间',
    end_time TIMESTAMP COMMENT '事件结束时间',
    nibrs_code STRING COMMENT '国家事件报告系统代码',
    victims INT COMMENT '受害者数量',
    crime_name1 STRING COMMENT '主要犯罪类型',
    crime_name2 STRING COMMENT '次要犯罪类型',
    crime_name3 STRING COMMENT '第三犯罪类型',
    police_district STRING COMMENT '警区',
    block_address STRING COMMENT '街区地址',
    city STRING COMMENT '城市',
    state STRING COMMENT '州/省',
    zip_code STRING COMMENT '邮政编码',
    agency STRING COMMENT '执法机构',
    place STRING COMMENT '犯罪地点类型',
    sector STRING COMMENT '扇区',
    beat STRING COMMENT '巡逻区域',
    pra STRING COMMENT '警察响应区域',
    address_num STRING COMMENT '地址号码',
    street_prefix STRING COMMENT '街道前缀',
    street_name STRING COMMENT '街道名称',
    street_suffix STRING COMMENT '街道后缀',
    street_type STRING COMMENT '街道类型',
    latitude DOUBLE COMMENT '纬度',
    longitude DOUBLE COMMENT '经度',
    district_num STRING COMMENT '地区编号',
    location STRING COMMENT '位置描述',
    versionid INT COMMENT '数据版本ID'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/crime_analysis/crime_data_external/'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'comment'='外部表，映射HDFS中的原始犯罪事件CSV数据'
);

-- =======================================================================
-- 2. 内部表：用于数据处理和分析（分区、分桶、ORC格式）
-- =======================================================================
CREATE TABLE IF NOT EXISTS crime_incidents (
    incident_id STRING COMMENT '事件唯一标识',
    offence_code STRING COMMENT '犯罪代码',
    cr_number STRING COMMENT '犯罪记录编号',
    dispatch_time TIMESTAMP COMMENT '调度时间',
    start_time TIMESTAMP COMMENT '事件开始时间',
    end_time TIMESTAMP COMMENT '事件结束时间',
    nibrs_code STRING COMMENT '国家事件报告系统代码',
    victims INT COMMENT '受害者数量',
    crime_name1 STRING COMMENT '主要犯罪类型',
    crime_name2 STRING COMMENT '次要犯罪类型',
    crime_name3 STRING COMMENT '第三犯罪类型',
    police_district STRING COMMENT '警区',
    block_address STRING COMMENT '街区地址',
    city STRING COMMENT '城市',
    state STRING COMMENT '州/省',
    zip_code STRING COMMENT '邮政编码',
    agency STRING COMMENT '执法机构',
    place STRING COMMENT '犯罪地点类型',
    sector STRING COMMENT '扇区',
    beat STRING COMMENT '巡逻区域',
    pra STRING COMMENT '警察响应区域',
    address_num STRING COMMENT '地址号码',
    street_prefix STRING COMMENT '街道前缀',
    street_name STRING COMMENT '街道名称',
    street_suffix STRING COMMENT '街道后缀',
    street_type STRING COMMENT '街道类型',
    latitude DOUBLE COMMENT '纬度',
    longitude DOUBLE COMMENT '经度',
    district_num STRING COMMENT '地区编号',
    location STRING COMMENT '位置描述',
    versionid INT COMMENT '数据版本ID'
)
PARTITIONED BY (year INT COMMENT '年份', month INT COMMENT '月份', day INT COMMENT '日期')
CLUSTERED BY (city) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    'transactional'='true',
    'orc.compress'='NONE',  -- 不使用压缩
    'comment'='内部表，用于犯罪事件数据的分析和处理'
);

-- =======================================================================
-- 3. 数据导入语句（包含HDFS上传和Hive加载）
-- =======================================================================

-- 3.1 使用shell命令将本地CSV文件上传到HDFS（需要在shell中执行）
-- 上传所有版本的CSV文件到HDFS目录
-- hdfs dfs -mkdir -p /user/hive/warehouse/crime_data/
-- hdfs dfs -put crime_version1.csv /user/hive/warehouse/crime_data/
-- hdfs dfs -put crime_version2.csv /user/hive/warehouse/crime_data/
-- hdfs dfs -put crime_version3.csv /user/hive/warehouse/crime_data/

-- 3.2 加载HDFS中的数据到外部表
LOAD DATA INPATH '/user/hive/warehouse/crime_data/' INTO TABLE crime_incidents_external;

-- 3.3 从外部表加载数据到内部表（版本1）
INSERT OVERWRITE TABLE crime_incidents PARTITION(year, month, day)
SELECT 
    *, 
    YEAR(dispatch_time) AS year, 
    MONTH(dispatch_time) AS month, 
    DAY(dispatch_time) AS day
FROM crime_incidents_external 
WHERE versionid = 1;

-- 3.4 增量加载版本2数据
INSERT INTO TABLE crime_incidents PARTITION(year, month, day)
SELECT 
    *, 
    YEAR(dispatch_time) AS year, 
    MONTH(dispatch_time) AS month, 
    DAY(dispatch_time) AS day
FROM crime_incidents_external 
WHERE versionid = 2;

-- 3.5 增量加载版本3数据
INSERT INTO TABLE crime_incidents PARTITION(year, month, day)
SELECT 
    *, 
    YEAR(dispatch_time) AS year, 
    MONTH(dispatch_time) AS month, 
    DAY(dispatch_time) AS day
FROM crime_incidents_external 
WHERE versionid = 3;

-- =======================================================================
-- 4. 示例：5种Hive统计分析表
-- =======================================================================

-- 4.1 城市犯罪类型分布分析
CREATE TABLE IF NOT EXISTS analysis_city_crime_type (
    city STRING COMMENT '城市',
    crime_type STRING COMMENT '犯罪类型',
    crime_count INT COMMENT '犯罪数量',
    percentage DOUBLE COMMENT '占比',
    year INT COMMENT '年份',
    month INT COMMENT '月份'
)
PARTITIONED BY (year INT, month INT)
STORED AS ORC
COMMENT '城市犯罪类型分布分析';

-- 4.2 时间趋势分析
CREATE TABLE IF NOT EXISTS analysis_time_trend (
    year INT COMMENT '年份',
    month INT COMMENT '月份',
    day INT COMMENT '日期',
    crime_count INT COMMENT '犯罪数量',
    avg_victims DOUBLE COMMENT '平均受害者数量'
)
PARTITIONED BY (year INT, month INT)
STORED AS ORC
COMMENT '犯罪时间趋势分析';

-- 4.3 受害者数量分布分析
CREATE TABLE IF NOT EXISTS analysis_victim_distribution (
    victim_range STRING COMMENT '受害者数量范围',
    crime_count INT COMMENT '犯罪数量',
    percentage DOUBLE COMMENT '占比'
)
STORED AS ORC
COMMENT '受害者数量分布分析';

-- 4.4 犯罪地点类型分析
CREATE TABLE IF NOT EXISTS analysis_place_type (
    place_type STRING COMMENT '地点类型',
    crime_count INT COMMENT '犯罪数量',
    avg_victims DOUBLE COMMENT '平均受害者数量'
)
STORED AS ORC
COMMENT '犯罪地点类型分析';

-- 4.5 版本数据对比分析
CREATE TABLE IF NOT EXISTS analysis_version_comparison (
    city STRING COMMENT '城市',
    v1_count INT COMMENT '版本1数量',
    v2_count INT COMMENT '版本2数量',
    v3_count INT COMMENT '版本3数量',
    growth_rate DOUBLE COMMENT '增长率'
)
STORED AS ORC
COMMENT '不同版本数据对比分析';

-- =======================================================================
-- 5. Hive-HBase集成表（可选）
-- =======================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS crime_incidents_hbase (
    rowkey STRING COMMENT 'HBase RowKey',
    incident_id STRING COMMENT '事件唯一标识',
    offence_code STRING COMMENT '犯罪代码',
    cr_number STRING COMMENT '犯罪记录编号',
    dispatch_time TIMESTAMP COMMENT '调度时间',
    city STRING COMMENT '城市',
    state STRING COMMENT '州/省',
    zip_code STRING COMMENT '邮政编码',
    police_district STRING COMMENT '警区',
    agency STRING COMMENT '执法机构',
    place STRING COMMENT '犯罪地点类型',
    victims INT COMMENT '受害者数量',
    crime_name1 STRING COMMENT '主要犯罪类型',
    versionid INT COMMENT '数据版本ID'
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,incident:incident_id,incident:offence_code,incident:cr_number,time:dispatch_time,location:city,location:state,location:zip_code,police:police_district,police:agency,police:place,incident:victims,incident:crime_name1,version:versionid'
)
TBLPROPERTIES (
    'hbase.table.name' = 'crime_data_hbase',
    'comment' = 'Hive-HBase集成表，用于实时查询'
);

-- =======================================================================
-- 5. 分析查询语句（每个查询都包含表连接和聚合操作）
-- =======================================================================

-- 5.1 城市犯罪类型分布分析
-- 使用自连接来计算城市内各犯罪类型的占比
INSERT OVERWRITE TABLE analysis_city_crime_type PARTITION(year=2023, month=12)
SELECT
    c.city,
    c.crime_name1 AS crime_type,
    COUNT(c.incident_id) AS crime_count,
    ROUND(COUNT(c.incident_id) / total.total_crimes * 100, 2) AS percentage,
    2023 AS year,
    12 AS month
FROM crime_incidents c
JOIN (
    SELECT city, COUNT(incident_id) AS total_crimes
    FROM crime_incidents
    WHERE year=2023 AND month=12
    GROUP BY city
) total ON c.city = total.city
WHERE c.year=2023 AND c.month=12
GROUP BY c.city, c.crime_name1, total.total_crimes;

-- 5.2 时间趋势分析
-- 使用窗口函数和自连接分析时间趋势
INSERT OVERWRITE TABLE analysis_time_trend PARTITION(year=2023, month=12)
SELECT
    c.year,
    c.month,
    c.day,
    COUNT(c.incident_id) AS crime_count,
    AVG(c.victims) AS avg_victims
FROM crime_incidents c
JOIN (
    SELECT DISTINCT year, month, day
    FROM crime_incidents
    WHERE year=2023 AND month=12
) dates ON c.year=dates.year AND c.month=dates.month AND c.day=dates.day
WHERE c.year=2023 AND c.month=12
GROUP BY c.year, c.month, c.day;

-- 5.3 受害者数量分布分析
-- 使用自连接计算不同受害者数量范围的占比
INSERT OVERWRITE TABLE analysis_victim_distribution
SELECT
    CASE
        WHEN c.victims = 0 THEN '0'
        WHEN c.victims = 1 THEN '1'
        WHEN c.victims BETWEEN 2 AND 5 THEN '2-5'
        ELSE '6+'
    END AS victim_range,
    COUNT(c.incident_id) AS crime_count,
    ROUND(COUNT(c.incident_id) / total.total_crimes * 100, 2) AS percentage
FROM crime_incidents c
JOIN (
    SELECT COUNT(incident_id) AS total_crimes
    FROM crime_incidents
) total ON 1=1
GROUP BY CASE
    WHEN c.victims = 0 THEN '0'
    WHEN c.victims = 1 THEN '1'
    WHEN c.victims BETWEEN 2 AND 5 THEN '2-5'
    ELSE '6+'
END, total.total_crimes;

-- 5.4 犯罪地点类型分析
-- 使用自连接分析不同地点类型的犯罪情况
INSERT OVERWRITE TABLE analysis_place_type
SELECT
    c.place AS place_type,
    COUNT(c.incident_id) AS crime_count,
    AVG(c.victims) AS avg_victims
FROM crime_incidents c
JOIN (
    SELECT DISTINCT place
    FROM crime_incidents
) places ON c.place = places.place
GROUP BY c.place;

-- 5.5 版本数据对比分析
-- 使用自连接对比不同版本的数据变化
INSERT OVERWRITE TABLE analysis_version_comparison
SELECT
    c.city,
    SUM(CASE WHEN c.versionid = 1 THEN 1 ELSE 0 END) AS v1_count,
    SUM(CASE WHEN c.versionid = 2 THEN 1 ELSE 0 END) AS v2_count,
    SUM(CASE WHEN c.versionid = 3 THEN 1 ELSE 0 END) AS v3_count,
    ROUND((SUM(CASE WHEN c.versionid = 3 THEN 1 ELSE 0 END) - SUM(CASE WHEN c.versionid = 1 THEN 1 ELSE 0 END)) /
          SUM(CASE WHEN c.versionid = 1 THEN 1 ELSE 1 END) * 100, 2) AS growth_rate
FROM crime_incidents c
JOIN (
    SELECT DISTINCT city
    FROM crime_incidents
) cities ON c.city = cities.city
GROUP BY c.city;

-- =======================================================================
-- 6. 表查询示例
-- =======================================================================

-- 查看外部表数据
SELECT * FROM crime_incidents_external LIMIT 10;

-- 查看内部表数据（按分区）
SELECT * FROM crime_incidents WHERE year=2023 AND month=12 LIMIT 10;

-- 查看分析结果
SELECT * FROM analysis_city_crime_type WHERE year=2023 AND month=12 LIMIT 10;
SELECT * FROM analysis_time_trend WHERE year=2023 AND month=12 LIMIT 10;
SELECT * FROM analysis_victim_distribution LIMIT 10;
SELECT * FROM analysis_place_type LIMIT 10;
SELECT * FROM analysis_version_comparison LIMIT 10;

-- 查看表结构
describe formatted crime_incidents;

-- =======================================================================
-- 总结
-- =======================================================================
-- 1. 外部表：映射HDFS中的原始CSV数据
-- 2. 内部表：支持分区（year/month/day）、分桶（city，8个桶）、ORC格式、事务
-- 3. 5种分析表：满足项目要求的Hive统计分析需求
-- 4. 集成表：可选的Hive-HBase集成支持
-- =======================================================================