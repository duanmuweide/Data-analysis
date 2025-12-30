CREATE database cjz;
use cjz;
-- 建立模拟批次的源数据表
--建立初始表
CREATE TABLE house_info_checkid(
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
STORED AS TEXTFILE;
--插入数据
LOAD DATA INPATH '/user/master/dataanalysis/data3.csv'
OVERWRITE INTO TABLE house_info_checkid;
--建立清理后的表
CREATE TABLE IF NOT EXISTS house_info_clean_checkid (
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
                                                        house_age INT COMMENT '房龄(年)',
                                                        checkid INT COMMENT '模拟多次查询的批次'
)
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");
--插入清理后的数据
INSERT INTO TABLE house_info_clean_checkid
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
    (2024 - CAST(build_year AS INT)) as house_age,

    3 as checkid

FROM house_info_checkid
WHERE district IS NOT NULL
  AND community IS NOT NULL
  AND area > 0
  AND price > 0
  AND build_year REGEXP '^[0-9]{4}$';


-- 创建房屋年份分析主表（分区表）
CREATE TABLE IF NOT EXISTS house_year_analysis (
                                                   year_range STRING COMMENT '房屋年份区间',
                                                   house_count INT COMMENT '房子数量',
                                                   elevator_count INT COMMENT '电梯数量',
                                                   small_layout_count INT COMMENT '小户型数量(1-2室)',
                                                   medium_layout_count INT COMMENT '中户型数量(3-4室)',
                                                   large_layout_count INT COMMENT '大户型数量(5室以上及其他)',
                                                   premium_decoration_count INT COMMENT '精装数量',
                                                   simple_decoration_count INT COMMENT '简装数量',
                                                   rough_decoration_count INT COMMENT '毛坯数量',
                                                   analysis_time TIMESTAMP COMMENT '分析时间',
                                                   checkid INT COMMENT '批次ID'
)
    COMMENT '房屋建造年份与户型、电梯、装修情况关联分析主表'
    PARTITIONED BY (pt_date STRING COMMENT '分区日期')
    CLUSTERED BY (year_range) INTO 4 BUCKETS
    STORED AS ORC
    TBLPROPERTIES (
                      "orc.compress" = "SNAPPY",
                      "transactional" = "false",
                      "bucketing_version" = "2"
                  );

-- 注册 classify_layout 永久函数
CREATE FUNCTION classify_layout AS 'com.qdu.udf.LayoutClassifyUDF'
USING JAR 'hdfs:///user/master/dataanalysis/DataAnalysis-1.0-SNAPSHOT.jar';

-- 注册 classify_decoration 永久函数
CREATE FUNCTION classify_decoration AS 'com.qdu.udf.DecorationClassifyUDF'
USING JAR 'hdfs:///user/master/dataanalysis/DataAnalysis-1.0-SNAPSHOT.jar';

CREATE TABLE IF NOT EXISTS house_analysis_result (
                                                     layout_category STRING COMMENT '户型分类：小/中/大',
                                                     elevator_int INT COMMENT '有无电梯：1有/0无',
                                                     decoration_category STRING COMMENT '装修分类：精装/简装/毛坯/其它',
                                                     house_count INT COMMENT '该组合下的房子数量',
                                                     avg_price_per_sqm INT COMMENT '该组合下的平均单价（元/㎡）',
                                                     checkid INT COMMENT '批次ID'
)
    CLUSTERED BY (layout_category) INTO 4 BUCKETS
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");

CREATE TABLE IF NOT EXISTS area_price_analysis (
                                                   area_range STRING COMMENT '面积范围',
                                                   house_count INT COMMENT '房屋数量',
                                                   avg_price_per_sqm INT COMMENT '平均单价(元/㎡)',
                                                   min_price INT COMMENT '最低单价(元/㎡)',
                                                   max_price INT COMMENT '最高单价(元/㎡)',
                                                   median_price INT COMMENT '中位数单价(元/㎡)',
                                                   price_variance INT COMMENT '价格方差',
                                                   price_stddev INT COMMENT '价格标准差',
                                                   avg_house_age INT COMMENT '平均房龄(年)',
                                                   load_date STRING COMMENT '加载日期',
                                                   price_level STRING COMMENT '价格等级',
                                                   area_ratio DECIMAL(5,2) COMMENT '面积占比',
    checkid INT COMMENT '数据批次ID'
    )
    PARTITIONED BY (pt_date STRING)
    CLUSTERED BY (area_range) INTO 4 BUCKETS
    STORED AS ORC
    TBLPROPERTIES (
                      "orc.compress"="SNAPPY",
                      "orc.create.index"="true",
                      "orc.bloom.filter.columns"="area_range",
                      "comment"="面积区间房价分析表（含批次）"
                  );

-- 北京每个区平均房价表（含 checkid 批次标识）
CREATE TABLE IF NOT EXISTS district_house_price_analysis (
                                                             district STRING COMMENT '市区名称',
                                                             avg_price_per_sqm INT COMMENT '平均房价(元/平方米)',
                                                             house_count INT COMMENT '房屋数量',
                                                             min_price INT COMMENT '最低单价',
                                                             max_price INT COMMENT '最高单价',
                                                             median_price INT COMMENT '中位数单价',
                                                             price_variance INT COMMENT '价格方差',
                                                             std_price INT COMMENT '价格标准差',
                                                             avg_house_age INT COMMENT '平均房龄(年)',
                                                             avg_area INT COMMENT '平均面积(㎡)',
                                                             checkid INT COMMENT '数据批次ID'
)
    COMMENT '北京市区房价统计分析表（带批次）'
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

CREATE TABLE IF NOT EXISTS best_value_housing (
                                                  district STRING COMMENT '市区名称',
                                                  area_range STRING COMMENT '面积范围',
                                                  layout_category STRING COMMENT '户型分类',
                                                  decoration_category STRING COMMENT '装修分类',
                                                  elevator_int INT COMMENT '有无电梯：1有/0无',
                                                  avg_price_per_sqm INT COMMENT '该组合下的平均单价（元/㎡）',
                                                  value_score INT COMMENT '性价比评分（越高越好）',
    checkid INT COMMENT '批次ID'
    )
    PARTITIONED BY (pt_date STRING COMMENT '分区日期')
    CLUSTERED BY (district) INTO 4 BUCKETS
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");


show databases;
use cjz;
CREATE TABLE IF NOT EXISTS house_year_analysis (
                                                   year_range VARCHAR(255) COMMENT '房屋年份区间',
    house_count INT COMMENT '房子数量',
    elevator_count INT COMMENT '电梯数量',
    small_layout_count INT COMMENT '小户型数量(1-2室)',
    medium_layout_count INT COMMENT '中户型数量(3-4室)',
    large_layout_count INT COMMENT '大户型数量(5室以上及其他)',
    premium_decoration_count INT COMMENT '精装数量',
    simple_decoration_count INT COMMENT '简装数量',
    rough_decoration_count INT COMMENT '毛坯数量',
    analysis_time DATETIME COMMENT '分析时间',
    checkid INT COMMENT '批次ID',
    pt_date VARCHAR(20) COMMENT '分区日期',

    INDEX idx_pt_date (pt_date),
    INDEX idx_year_range (year_range)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='房屋建造年份与户型、电梯、装修情况关联分析主表';

CREATE TABLE IF NOT EXISTS house_analysis_result (
                                                     layout_category VARCHAR(50) COMMENT '户型分类：小/中/大',
    elevator_int TINYINT COMMENT '有无电梯：1有/0无',
    decoration_category VARCHAR(50) COMMENT '装修分类：精装/简装/毛坯/其它',
    house_count INT COMMENT '该组合下的房子数量',
    avg_price_per_sqm INT COMMENT '该组合下的平均单价（元/㎡）',
    checkid INT COMMENT '批次ID',

    INDEX idx_layout (layout_category),
    INDEX idx_checkid (checkid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS area_price_analysis (
                                                   area_range VARCHAR(100) COMMENT '面积范围',
    house_count INT COMMENT '房屋数量',
    avg_price_per_sqm INT COMMENT '平均单价(元/㎡)',
    min_price INT COMMENT '最低单价(元/㎡)',
    max_price INT COMMENT '最高单价(元/㎡)',
    median_price INT COMMENT '中位数单价(元/㎡)',
    price_variance INT COMMENT '价格方差',
    price_stddev INT COMMENT '价格标准差',
    avg_house_age INT COMMENT '平均房龄(年)',
    load_date VARCHAR(20) COMMENT '加载日期',
    price_level VARCHAR(50) COMMENT '价格等级',
    area_ratio DECIMAL(5,2) COMMENT '面积占比',
    checkid INT COMMENT '数据批次ID',
    pt_date VARCHAR(20) COMMENT '分区日期',

    INDEX idx_area_range (area_range),
    INDEX idx_pt_date (pt_date),
    INDEX idx_checkid (checkid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='面积区间房价分析表（含批次）';

CREATE TABLE IF NOT EXISTS district_house_price_analysis (
                                                             district VARCHAR(100) COMMENT '市区名称',
    avg_price_per_sqm INT COMMENT '平均房价(元/平方米)',
    house_count INT COMMENT '房屋数量',
    min_price INT COMMENT '最低单价',
    max_price INT COMMENT '最高单价',
    median_price INT COMMENT '中位数单价',
    price_variance INT COMMENT '价格方差',
    std_price INT COMMENT '价格标准差',
    avg_house_age INT COMMENT '平均房龄(年)',
    avg_area INT COMMENT '平均面积(㎡)',
    checkid INT COMMENT '数据批次ID',
    load_date VARCHAR(20) COMMENT '数据加载日期',

    PRIMARY KEY (district, checkid),  -- 可选：如果每个区每天一条记录
    INDEX idx_district (district),
    INDEX idx_load_date (load_date),
    INDEX idx_checkid (checkid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='北京市区房价统计分析表（带批次）';
-- 1. 删除现有主键（MySQL 要求显式 DROP PRIMARY KEY）
ALTER TABLE district_house_price_analysis DROP PRIMARY KEY;

-- 2. 添加新联合主键 (district, checkid)
ALTER TABLE district_house_price_analysis
    ADD PRIMARY KEY (district, checkid);

CREATE TABLE IF NOT EXISTS best_value_housing (
                                                  district VARCHAR(100) COMMENT '市区名称',
    area_range VARCHAR(100) COMMENT '面积范围',
    layout_category VARCHAR(50) COMMENT '户型分类',
    decoration_category VARCHAR(50) COMMENT '装修分类',
    elevator_int TINYINT COMMENT '有无电梯：1有/0无',
    avg_price_per_sqm INT COMMENT '该组合下的平均单价（元/㎡）',
    value_score INT COMMENT '性价比评分（越高越好）',
    checkid INT COMMENT '批次ID',
    pt_date VARCHAR(20) COMMENT '分区日期',

    INDEX idx_district (district),
    INDEX idx_pt_date (pt_date),
    INDEX idx_value_score (value_score)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS community_price_analysis (
                                                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                                                        checkid INT NOT NULL COMMENT '分析批次ID',
                                                        district VARCHAR(255) NOT NULL COMMENT '区县',
    community VARCHAR(255) NOT NULL COMMENT '小区名称',
    house_count INT NOT NULL COMMENT '房屋数量',
    avg_price_per_sqm INT NOT NULL COMMENT '平均单价（元/平方米）',
    build_year VARCHAR(20) COMMENT '建造年份（原始字符串）',

    -- 可选：记录同步时间
    sync_time DATETIME DEFAULT CURRENT_TIMESTAMP,

    -- 联合唯一索引：确保同一批次下同一小区只有一条记录（幂等）
    UNIQUE KEY uk_checkid_district_community (checkid, district, community),

    -- 普通索引加速查询
    INDEX idx_district (district),
    INDEX idx_community (community),
    INDEX idx_checkid (checkid),
    INDEX idx_avg_price (avg_price_per_sqm)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='小区房价分析结果表';