USE crime_analysis;

-- ==============================
-- 1. 内存配置（防 OOM）
-- ==============================
SET mapreduce.map.memory.mb=2048;
SET mapreduce.map.java.opts=-Xmx1536m;
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.reduce.java.opts=-Xmx3072m;

-- ==============================
-- 2. 分桶配置（必须）
-- ==============================
SET hive.enforce.bucketing=true;

-- ==============================
-- 3. 动态分区配置（关键：提高上限）
-- ==============================
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=5000;      
SET hive.exec.max.dynamic.partitions.pernode=1000; 

-- ==============================
-- 4. Reduce 数 = 分桶数（假设是 8）
-- ==============================
SET mapreduce.job.reduces=8;

-- ==============================
-- 5. 插入数据（可选：加时间范围更安全）
-- ==============================
INSERT OVERWRITE TABLE crime_incidents
PARTITION (year, month, day)
SELECT
    incident_id,
    offence_code,
    cr_number,
    CASE WHEN dispatch_time IS NOT NULL AND dispatch_time != '' AND dispatch_time != 'NaT' 
         THEN CAST(dispatch_time AS TIMESTAMP) END AS dispatch_time,
    CASE WHEN start_time IS NOT NULL AND start_time != '' AND start_time != 'NaT' 
         THEN CAST(start_time AS TIMESTAMP) END AS start_time,
    CASE WHEN end_time IS NOT NULL AND end_time != '' AND end_time != 'NaT' 
         THEN CAST(end_time AS TIMESTAMP) END AS end_time,
    nibrs_code,
    CASE WHEN victims IS NOT NULL AND victims != '' THEN CAST(victims AS INT) END AS victims,
    crime_name1, crime_name2, crime_name3,
    police_district, block_address, city, state, zip_code, agency, place,
    sector, beat, pra, address_num, street_prefix, street_name,
    street_suffix, street_type,
    CASE WHEN latitude IS NOT NULL AND latitude != '' THEN CAST(latitude AS DOUBLE) END AS latitude,
    CASE WHEN longitude IS NOT NULL AND longitude != '' THEN CAST(longitude AS DOUBLE) END AS longitude,
    district_num,
    location,
    CASE WHEN versionid IS NOT NULL AND versionid != '' THEN CAST(versionid AS INT) END AS versionid,
    YEAR(CAST(start_time AS TIMESTAMP)) AS year,
    MONTH(CAST(start_time AS TIMESTAMP)) AS month,
    DAY(CAST(start_time AS TIMESTAMP)) AS day
FROM crime_incidents_external
WHERE
    start_time IS NOT NULL
    AND start_time != ''
    AND start_time != 'NaT'
    AND CAST(start_time AS TIMESTAMP) IS NOT NULL

    AND start_time >= '2023-01-01'
    AND start_time < '2024-01-01'
;