CREATE database dataanalysis;
use dataanalysis;
--建立初始表
CREATE TABLE house_info (
    district STRING COMMENT '市区',
    community STRING COMMENT '小区',
    layout STRING COMMENT '户型',
    orientation STRING COMMENT '朝向',
    floor_num INT COMMENT '楼层',  -- 改为floor_num
    decoration STRING COMMENT '装修情况',
    elevator STRING COMMENT '电梯',
    area INT COMMENT '面积(㎡)',
    price INT COMMENT '价格(万元)',
    build_year STRING COMMENT '年份'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");
--插入数据
LOAD DATA INPATH '/user/master/dataanalysis/data.csv' 
OVERWRITE INTO TABLE house_info;
--建立清理后的表
CREATE TABLE IF NOT EXISTS house_info_clean (
    rowkey STRING COMMENT '唯一标识:市区_小区_序号',
    district STRING COMMENT '市区',
    community STRING COMMENT '小区',
    layout STRING COMMENT '户型',
    orientation STRING COMMENT '朝向',
    floor_num INT COMMENT '楼层',
    decoration STRING COMMENT '装修情况',
    elevator_int INT COMMENT '电梯:1有/0无',
    area INT COMMENT '面积(㎡)',
    price INT COMMENT '价格(万元)',
    price_per_sqm INT COMMENT '单价(元/㎡)',
    build_year STRING COMMENT '建造年份',
    house_age INT COMMENT '房龄(年)'
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
--插入清理后的数据
INSERT OVERWRITE TABLE house_info_clean
SELECT 
    -- 生成rowkey（去掉特殊字符）
    CONCAT(district, '_', 
           regexp_replace(community, '[^\\w\\u4e00-\\u9fff]', '_'), 
           '_', 
           cast(row_number() OVER (ORDER BY district, community) as string)) as rowkey,
    
    -- 原始字段
    district,
    community,
    layout,
    orientation,
    floor_num,
    decoration,
    
    -- 电梯字段转换
    CASE 
        WHEN elevator = '有电梯' THEN 1
        WHEN elevator = '无电梯' THEN 0
        ELSE 0
    END as elevator_int,
    
    area,
    price,
    
    -- 单价取整（万元/㎡）
    CAST(ROUND(price * 10000.0 / area) as INT) as price_per_sqm,
    
    build_year,
    
    -- 计算房龄（假设当前年份为2024）
    (2024 - CAST(build_year AS INT)) as house_age
    
FROM house_info
WHERE district IS NOT NULL 
  AND community IS NOT NULL 
  AND area > 0 
  AND price > 0
  AND build_year REGEXP '^[0-9]{4}$';

-- 北京每个区平均房价表
CREATE TABLE IF NOT EXISTS dataanalysis.district_house_price_analysis (
    district STRING COMMENT '市区名称',
    avg_price_per_sqm INT COMMENT '平均房价(元/平方米)',
    house_count INT COMMENT '房屋数量',
    min_price INT COMMENT '最低单价',
    max_price INT COMMENT '最高单价',
    median_price INT COMMENT '中位数单价',
    price_variance INT COMMENT '价格方差',
    std_price INT COMMENT '价格标准差',
    avg_house_age INT COMMENT '平均房龄(年)',
    avg_area INT COMMENT '平均面积(㎡)'
)
    COMMENT '北京市区房价统计分析表（全INT类型，已移除new_house_ratio列）'
    PARTITIONED BY (load_date STRING COMMENT '数据加载日期')
    CLUSTERED BY (avg_price_per_sqm)
    SORTED BY (district ASC)
    INTO 5 BUCKETS
    STORED AS ORC
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'orc.create.index' = 'true',
                      'orc.bloom.filter.columns' = 'district',
                      'orc.row.index.stride' = '10000',
                      'transactional' = 'false'
                  );