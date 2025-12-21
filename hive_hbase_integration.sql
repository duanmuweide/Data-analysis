-- Hive-HBase集成表创建脚本
-- 用于将Hive表与HBase表关联，支持版本控制

-- 使用的数据库
CREATE DATABASE IF NOT EXISTS crime_analysis;
USE crime_analysis;

-- 创建Hive-HBase集成表，映射到HBase的crime_data_hbase表
CREATE EXTERNAL TABLE IF NOT EXISTS crime_incidents_hbase (
    rowkey STRING,
    -- incident列族
    incident_id STRING,
    offence_code STRING,
    cr_number STRING,
    nibrs_code STRING,
    victims INT,
    crime_name1 STRING,
    crime_name2 STRING,
    crime_name3 STRING,
    -- time列族
    dispatch_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    -- location列族
    block_address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    address_num STRING,
    street_prefix STRING,
    street_name STRING,
    street_suffix STRING,
    street_type STRING,
    -- police列族
    police_district STRING,
    agency STRING,
    place STRING,
    sector STRING,
    beat STRING,
    pra STRING,
    district_num STRING,
    -- version列族
    versionid INT
) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = "
        :key,
        incident:incident_id,
        incident:offence_code,
        incident:cr_number,
        incident:nibrs_code,
        incident:victims,
        incident:crime_name1,
        incident:crime_name2,
        incident:crime_name3,
        time:dispatch_time,
        time:start_time,
        time:end_time,
        location:block_address,
        location:city,
        location:state,
        location:zip_code,
        location:latitude,
        location:longitude,
        location:address_num,
        location:street_prefix,
        location:street_name,
        location:street_suffix,
        location:street_type,
        police:police_district,
        police:agency,
        police:place,
        police:sector,
        police:beat,
        police:pra,
        police:district_num,
        version:versionid"
)
TBLPROPERTIES (
    "hbase.table.name" = "crime_data_hbase"
);

-- 示例：从集成表查询数据（按版本过滤）
SELECT city, crime_name1, COUNT(*) as crime_count
FROM crime_incidents_hbase
WHERE versionid = 1
GROUP BY city, crime_name1
ORDER BY crime_count DESC
LIMIT 10;

-- 示例：版本对比查询
SELECT 
    city,
    SUM(CASE WHEN versionid = 1 THEN 1 ELSE 0 END) as version_1_crimes,
    SUM(CASE WHEN versionid = 2 THEN 1 ELSE 0 END) as version_2_crimes,
    SUM(CASE WHEN versionid = 3 THEN 1 ELSE 0 END) as version_3_crimes
FROM crime_incidents_hbase
GROUP BY city
ORDER BY version_3_crimes DESC
LIMIT 10;