-- 创建数据库
CREATE DATABASE IF NOT EXISTS crime_analysis;
USE crime_analysis;

-- 创建外部表
CREATE EXTERNAL TABLE IF NOT EXISTS crime_incidents_external (
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/crime_data';

-- 创建ORC格式的内部表以提高查询性能
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
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');