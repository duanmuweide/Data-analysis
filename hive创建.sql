-- =======================================================================
-- 简化版Hive表创建脚本（非ACID表）
-- =======================================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS crime_analysis_simple;
USE crime_analysis_simple;

-- 动态分区设置
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

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
LOCATION '/user/hive/warehouse/crime_analysis_simple/crime_data_external/'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'comment'='外部表，映射HDFS中的原始犯罪事件CSV数据'
);

-- =======================================================================
-- 2. 内部表：用于数据处理和分析（分区、分桶、ORC格式）- 非ACID
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
    'orc.compress'='SNAPPY',  -- 使用压缩
    'comment'='内部表，用于犯罪事件数据的分析和处理（非ACID）'
);