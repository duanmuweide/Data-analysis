-- Hive external table mapping to HBase table
-- Usage: hive -f create_hive_external_table.sql

-- Drop existing external table if needed
DROP TABLE IF EXISTS hive_crime_incidents;

-- Create Hive external table mapping to HBase table
CREATE EXTERNAL TABLE hive_crime_incidents (
  rowkey STRING,
  incident_id STRING,
  state STRING,
  city STRING,
  victims INT,
  `timestamp` STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = ":key,cf:incident_id,cf:state,cf:city,cf:victims,cf:timestamp"
)
TBLPROPERTIES (
  "hbase.table.name" = "crime_incidents",
  "hbase.mapred.output.outputtable" = "crime_incidents"
);

-- Verify table creation
SHOW TABLES;
DESCRIBE hive_crime_incidents;

-- Query all data to verify connection
SELECT * FROM hive_crime_incidents LIMIT 10;
