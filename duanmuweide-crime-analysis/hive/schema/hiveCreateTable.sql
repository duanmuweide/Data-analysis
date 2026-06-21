-- create_tables_fixed.hql

-- 动态分区配置
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=500;

CREATE DATABASE IF NOT EXISTS crime_analysis;
USE crime_analysis;

-- 删除旧表（确保干净）
DROP TABLE IF EXISTS crime_incidents_external;
DROP TABLE IF EXISTS crime_incidents;

-- 外部表：所有字段 STRING！CSV 只能安全读为字符串
CREATE EXTERNAL TABLE crime_incidents_external (
    incident_id STRING,
    offence_code STRING,
    cr_number STRING,
    dispatch_time STRING,   -- ← 必须 STRING
    start_time STRING,      -- ← 必须 STRING
    end_time STRING,        -- ← 必须 STRING
    nibrs_code STRING,
    victims STRING,         -- ← 先 STRING，后续 CAST
    crime_name1 STRING,
    crime_name2 STRING,
    crime_name3 STRING,
    police_district STRING,
    block_address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    agency STRING,
    place STRING,
    sector STRING,
    beat STRING,
    pra STRING,
    address_num STRING,
    street_prefix STRING,
    street_name STRING,
    street_suffix STRING,
    street_type STRING,
    latitude STRING,        -- ← 先 STRING
    longitude STRING,       -- ← 先 STRING
    district_num STRING,
    location STRING,
    versionid STRING        -- ← 先 STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/crime_analysis/crime_data_external/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 内部表（保持不变）
CREATE TABLE crime_incidents (
    incident_id STRING,
    offence_code STRING,
    cr_number STRING,
    dispatch_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    nibrs_code STRING,
    victims INT,
    crime_name1 STRING,
    crime_name2 STRING,
    crime_name3 STRING,
    police_district STRING,
    block_address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    agency STRING,
    place STRING,
    sector STRING,
    beat STRING,
    pra STRING,
    address_num STRING,
    street_prefix STRING,
    street_name STRING,
    street_suffix STRING,
    street_type STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    district_num STRING,
    location STRING,
    versionid INT
)
PARTITIONED BY (year INT, month INT, day INT)
CLUSTERED BY (city) INTO 8 BUCKETS
STORED AS ORC;