# Databricks notebook source
# MAGIC %md
# MAGIC ## Raw data transfermation then merge to silver (bronze -> silver)

# COMMAND ----------

# MAGIC %sql  SHOW VIEWS IN global_temp

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2.1: raw data (bronze table) preparations: missing imputation, conflicts cleaning, enrichments

# COMMAND ----------

df_temp = spark.sql("""
                    SELECT * 
                    FROM global_temp.bronze
                    """)
display(df_temp)

# COMMAND ----------

# MAGIC %md 
# MAGIC ** Data cleaning rules**
# MAGIC 
# MAGIC 1. Excluding events we are not interest:
# MAGIC     1. "Activity" -- "Factory Closed", "TBD"
# MAGIC     1. "Break_Reasons": -- "move to other line" & "shift break"
# MAGIC 1. Fill in missing values in "Break_Reason" with **"Unselected reason"** 
# MAGIC 1. Fill in missing values in "Missing Ingredients" with **"Unselected ingredient"**
# MAGIC 1. Adjust the abnormal "time consumption" of each event:
# MAGIC     
# MAGIC     **Breaks:**
# MAGIC     1. "10 mins break" -- 15 min maximum;
# MAGIC     1. "30 mins break" --  45 min maximun;
# MAGIC     1. "machine down" -- 30 mins maximum;
# MAGIC     1. "no dolly/crate" -- 20 mins maximum;
# MAGIC     1. "missing products" -- if the recorded "time consumption" is greater than 120 min, then set it to 20 min;
# MAGIC     1. "rework previous mks" -- if the recorded "time consumption" is greater that 240 min, then set it to 60 min;
# MAGIC     1. "Unselected" -- 15 mins maximum;
# MAGIC     1. "shortage of staff" -- if the recorded "time consumption" is greater that 40 min, then set it to 15 min;
# MAGIC     
# MAGIC    **Activities other than Break:**
# MAGIC     1. "Production" -- 15 mins maximum, 1 mins minimum;
# MAGIC     1. "Preparation/Changeover" -- 30 min maximum 
# MAGIC    
# MAGIC **Data Enrichment**
# MAGIC 
# MAGIC 1. Add a new column "Activity Details" -- break down "Breaks" into "Break Reasons"(same granularity of all events);
# MAGIC 1. Merge "Week" and "Recipe" to a new column "Week/Recipe";
# MAGIC 1. New column "Paid Time (mins)" -- the time consumption of current activity multiples current staff participating

# COMMAND ----------

df_temp.select("Activity").distinct().show()

# COMMAND ----------

# before any transformation
df_temp.count()

# COMMAND ----------

import pyspark.sql.functions as F

# Excluding unwanted "Activity"
df_temp = df_temp.filter((F.col('Activity') != "Factory Closed") & 
                         (F.col('Activity') != "TBD")
                        )
df_temp.count()

# COMMAND ----------

df_temp.groupBy("Break_Reasons").count().show()

# COMMAND ----------

# New column "Activities_Detial" base on other columns 
df_temp = df_temp.withColumn("Activities_Detail", F.when(F.col("Activity") == "Break", F.col("Break_Reasons"))\
                                         .otherwise(F.col("Activity")))

# Fill NA Break Reasons
df_temp = df_temp.fillna("Unselected", subset=["Activities_Detail"])

# COMMAND ----------

df_temp.where(F.col("Activities_Detail").isNull()).count() # validation

# COMMAND ----------

# Excluding unwanted "Break_Reasons"
df_temp = df_temp.filter((F.col('Activities_Detail') != "move to other line") &
               (F.col('Activities_Detail') != "shift break")
              )
print(df_temp.count())

# COMMAND ----------

display(df_temp.filter((df_temp["Break_Reasons"] == "missing products")&(df_temp["Missing_Ingredients"].isNull())))

# COMMAND ----------

# fillin "missing ingerdients" without input ingredients name
df_temp = df_temp.withColumn("Missing_Ingredients", F.when(((F.col("Break_Reasons") == "missing products") &
                                                         (F.col("Missing_Ingredients").isNull())), "Unselected Ingredient")
                                                 .otherwise(F.col("Missing_Ingredients"))
                         )
df_temp.groupBy("Missing_Ingredients").count().show(100,truncate = False)

# COMMAND ----------

# Merge "Week" and "Recipe" to a new column "Week/Recipe"
df_temp = df_temp.withColumn('Week/Recipe', F.concat_ws(' ', 'Week', 'Recipe_Name'))

# COMMAND ----------

# time adjustments
df_temp = df_temp.withColumn("Adjusted_time", F.when(((F.col("Activities_Detail") == "Production") & (F.col("Time_Consumption") < 1)), 1) \
                                               .when(((F.col("Activities_Detail") == "Production") & (F.col("Time_Consumption") > 15)), 15) \
                                               .when(((F.col("Activities_Detail") == "Preparation/Changeover") & (F.col("Time_Consumption") > 30)), 30)\
                                               .when(((F.col("Activities_Detail") == "10 mins break") & (F.col("Time_Consumption") > 15)), 15)\
                                               .when(((F.col("Activities_Detail") == "30 mins break") & (F.col("Time_Consumption") > 45)), 45)\
                                               .when(((F.col("Activities_Detail") == "machine down") & (F.col("Time_Consumption") > 30)), 30)\
                                               .when(((F.col("Activities_Detail") == "no dolly/crate") & (F.col("Time_Consumption") > 20)), 20)\
                                               .when(((F.col("Activities_Detail") == "Unselected") & (F.col("Time_Consumption") > 15)), 15)\
                                               .when(((F.col("Activities_Detail") == "missing products") & (F.col("Time_Consumption") > 120)), 20)\
                                               .when(((F.col("Activities_Detail") == "rework previous mks") & (F.col("Time_Consumption") > 240)), 60)\
                                               .when(((F.col("Activities_Detail") == "shortage of staff") & (F.col("Time_Consumption") > 40)), 15)\
                                               .otherwise(F.col("Time_Consumption")))

# COMMAND ----------

## a new cloumn "Paid time (mins)"
df_temp = df_temp.withColumn('Paid_time_in_mins', F.col("Adjusted_time") * F.col("Pickers_Count"))

# COMMAND ----------

display(df_temp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Append the new week data to the historical prepared data (silver table)

# COMMAND ----------

## the new bronze view
df_temp.createOrReplaceGlobalTempView("bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver
# MAGIC USING global_temp.bronze
# MAGIC ON (silver.Start_Time = bronze.Start_Time AND silver.Finish_Time = bronze.Finish_Time)
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver -- validating update history

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT week, COUNT(1) AS count
# MAGIC FROM silver
# MAGIC GROUP BY Week
# MAGIC -- validating

# COMMAND ----------

## Restore to version 1 for running experiments
# import delta.tables as dlt
# bronzeTable = dlt.DeltaTable.forPath(spark, "dbfs:/mnt/lakehouse/delta")
# bronzeTable.restoreToVersion(2)

# COMMAND ----------

