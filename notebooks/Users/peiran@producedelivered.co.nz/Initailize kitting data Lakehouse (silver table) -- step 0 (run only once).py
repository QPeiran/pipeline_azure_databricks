# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 0: Mount Azure Blob Storage service

# COMMAND ----------

# MAGIC %md
# MAGIC **Mounting my Lakehouse blob storage**

# COMMAND ----------

# dbutils.fs.mount(
#   source = "wasbs://lakehouse@rawkittingdatablobonly.blob.core.windows.net",
#   mount_point = "/mnt/lakehouse",
#   extra_configs = {"fs.azure.account.key.rawkittingdatablobonly.blob.core.windows.net":dbutils.secrets.get(scope = "kitting_data", key = "rawBlobDataAccessKey")})

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls mnt/kitting_gsheet

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Initialize kitting data lakehouse -- in Delta Lake format

# COMMAND ----------

# MAGIC %md
# MAGIC **Define Schema** and test inserts

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([ \
    StructField("Start_Time", TimestampType(),nullable = True), \
    StructField("Finish_Time", TimestampType(),nullable = True), \
    StructField("Activity", StringType(),True), \
    StructField("Seq_Code", StringType(), True), \
    StructField("Recipe_Name", StringType(), True), \
    StructField("Break_Reasons", StringType(), True), \
    StructField("Missing_Ingredients", StringType(), True), \
    StructField("Kitting_Line", StringType(), True), \
    StructField("Assembly_Batch", StringType(), True), \
    StructField("Event_Shift", StringType(), True), \
    StructField("Team_Leader", StringType(), True), \
    StructField("Pickers_Count", IntegerType(), True), \
    StructField("Time_Consumption", FloatType(), True), \
    StructField("Week", StringType(), True),\
    StructField("Activities_Detail", StringType(), True),\
    StructField("Week/Recipe", StringType(), True),\
    StructField("Adjusted_time", FloatType(), True),\
    StructField("Paid_time_in_mins", FloatType(), True)
  ]) # same as bigquery

# COMMAND ----------

df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
# df.printSchema()

# COMMAND ----------

if dbutils.fs.ls("/mnt/lakehouse/silver/"):
  pass
else:
  df.write.format("delta").save("/mnt/lakehouse/silver/")

# COMMAND ----------

## removing delta table storage folder for reset options
# dbutils.fs.rm('/mnt/lakehouse/silver/', True) 
# dbutils.fs.rm('/mnt/lakehouse/gold/', True) 

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS silver
          USING DELTA 
          LOCATION '/mnt/lakehouse/silver'
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Initailze Packguide data
# MAGIC 
# MAGIC **(ingredients in each recipe)**
# MAGIC 1. Picks count per recipe

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Initailze Production Plan (general) data
# MAGIC 
# MAGIC 1. Box Production target in general
# MAGIC 1. Production target by recipe and by (delivery) day
# MAGIC 1. Mks/create

# COMMAND ----------

