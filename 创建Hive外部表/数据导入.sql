-- 加载数据到外部表
LOAD DATA INPATH '/user/hadoop/crime_final.csv' 
OVERWRITE INTO TABLE crime_incidents_external
PARTITION (year=2023, month=12, day=9);

-- 从外部表插入到内部表
INSERT OVERWRITE TABLE crime_incidents PARTITION(year, month, day)
SELECT 
    incident_id,
    offence_code,
    cr_number,
    dispatch_time,
    start_time,
    end_time,
    nibrs_code,
    victims,
    crime_name1,
    crime_name2,
    crime_name3,
    police_district,
    block_address,
    city,
    state,
    zip_code,
    agency,
    place,
    sector,
    beat,
    pra,
    address_num,
    street_prefix,
    street_name,
    street_suffix,
    street_type,
    latitude,
    longitude,
    district_num,
    location,
    YEAR(dispatch_time) as year,
    MONTH(dispatch_time) as month,
    DAY(dispatch_time) as day
FROM crime_incidents_external
WHERE dispatch_time IS NOT NULL;