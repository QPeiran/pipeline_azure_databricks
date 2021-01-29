-- Databricks notebook source
USE sandbox;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables

-- COMMAND ----------

DROP TABLE IF EXISTS dc_data_raw;
CREATE TABLE dc_data_raw
USING parquet
OPTIONS(
      PATH "dbfs:/FileStore/tables/data_centers_q2_q3_snappy.parquet"
);

-- COMMAND ----------

SELECT * FROM dc_data_raw LIMIT 3

-- COMMAND ----------

DESCRIBE dc_data_raw

-- COMMAND ----------

SELECT [sensor-igauge]
FROM dc_data_raw

-- COMMAND ----------

  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM dc_data_raw

-- COMMAND ----------

WITH explore AS (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM dc_data_raw
)
SELECT value.description FROM explore

-- COMMAND ----------

select EXPLODE(dates) from databricks_blog

-- COMMAND ----------

DROP TABLE IF EXISTS device_data;

CREATE TABLE device_data 
USING delta
PARTITIONED BY (device_type)
WITH explode_source
AS
  (
  SELECT 
  dc_id,
  to_date(date) AS date,
  EXPLODE (source)
  FROM dc_data_raw
  )
SELECT 
  dc_id,
  key `device_type`, 
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level
  
FROM explode_source;

SELECT * FROM device_data

-- COMMAND ----------

describe extended device_data

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/sandbox.db/

-- COMMAND ----------

show tables

-- COMMAND ----------

describe extended databricks_blog

-- COMMAND ----------

cache table device_data

-- COMMAND ----------

SELECT 
  dc_id,
  device_type, 
  temps,
  TRANSFORM (temps, t -> ((t * 9) div 5) + 32 ) AS `temps_F`
FROM device_data;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW co2_levels_temporary
AS
  SELECT
    dc_id, 
    device_type,
    co2_level,
    REDUCE(co2_level, 0, (adding, added) -> adding + added) as sum_co2_level,
    REDUCE(co2_level, 0, (adding, added) -> adding + added, added ->(added div size(co2_level))) as avg_co2_level
  FROM device_data
  SORT BY avg_co2_level DESC;
  
SELECT * FROM co2_levels_temporary

-- COMMAND ----------

select * from 
(SELECT device_type, avg_co2_level FROM co2_levels_temporary)
PIVOT (
  ROUND(AVG(avg_co2_level), 2) AS avg_co2 
  FOR device_type IN ('sensor-ipad', 'sensor-inest', 
    'sensor-istick', 'sensor-igauge')
  );

-- COMMAND ----------

SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(avg_co2_level))  AS avg_co2_level
FROM co2_levels_temporary
GROUP BY ROLLUP (dc_id, device_type)
ORDER BY dc_id, device_type

-- COMMAND ----------

SELECT 
  COALESCE(dc_id, "All data centers") AS dc_id,
  COALESCE(device_type, "All devices") AS device_type,
  ROUND(AVG(avg_co2_level))  AS avg_co2_level
FROM co2_levels_temporary
GROUP BY CUBE (dc_id, device_type)
ORDER BY dc_id, device_type;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS avg_temps
USING delta
PARTITIONED BY (device_type)
AS
  SELECT
    dc_id,
    date,
    temps,
    REDUCE(temps, 0, (t, acc) -> t + acc, acc ->(acc div size(temps))) as avg_daily_temp_C,
    device_type
  FROM device_data;
  
SELECT * FROM avg_temps;

-- COMMAND ----------

DESCRIBE EXTENDED avg_temps

-- COMMAND ----------

