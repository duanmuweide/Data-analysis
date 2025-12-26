#!/bin/bash
# load_data.sh

# 1. 创建简化后的 HDFS 目录
hdfs dfs -mkdir -p /user/hive/warehouse/crime_analysis/crime_data_external/

# 2. 删除目录中已有的文件（如果存在）
hdfs dfs -rm -r /user/hive/warehouse/crime_analysis/crime_data_external/*

# 3. 上传两个 CSV 文件（请确保这些文件存在于 /home/master/）
hdfs dfs -put /home/master/crime_version1.csv /user/hive/warehouse/crime_analysis/crime_data_external/
hdfs dfs -put /home/master/crime_version2.csv /user/hive/warehouse/crime_analysis/crime_data_external/
hdfs dfs -put /home/master/crime_version3.csv /user/hive/warehouse/crime_analysis/crime_data_external/

# 3. （可选）验证上传
echo "Uploaded files:"
hdfs dfs -ls /user/hive/warehouse/crime_analysis/crime_data_external/

# 4. 加载到内部表（自动分区）
hive -f hiveCreateTable.sql

hive -f load_data.hql
