# Databricks notebook source
# MAGIC %md 
# MAGIC #### Select kitting data by weeks

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS silver
          USING DELTA 
          LOCATION '/mnt/lakehouse/silver'
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Select the data that we want load to Power BI**

# COMMAND ----------


cols = "Start_Time, Finish_Time, Activities_Detail, Seq_Code, Break_Reasons, Missing_Ingredients, Kitting_Line, Assembly_Batch, Event_Shift, Team_Leader, Pickers_Count, Adjusted_time, Paid_time_in_mins, Week"

df_using = spark.sql(f"""
                     SELECT {cols} FROM silver
                     WHERE week IN ('2021-04', '2021-05')
                     """)

# COMMAND ----------

display(df_using)

# COMMAND ----------

# try:
#   dbutils.fs.ls('/mnt/lakehouse/gold/')
df_using.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/mnt/lakehouse/gold/") \
# #   .saveAsTable("gold")
# except:
#   df_using.write.format("delta")\
#   .option("path", "/mnt/lakehouse/gold/") \
#   .partitionBy('Week')    
# #   .saveAsTable("gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS gold
          USING DELTA 
          LOCATION '/mnt/lakehouse/gold'
          """)

# COMMAND ----------

# MAGIC %sql SELECT Team_Leader, SUM(Adjusted_time) AS Work_in_mins 
# MAGIC      FROM gold 
# MAGIC      GROUP BY Team_Leader
# MAGIC      HAVING Work_in_mins > 100
# MAGIC      ORDER BY Work_in_mins DESC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### TODOS
# MAGIC 
# MAGIC 1. ingest "Production Plan" data and register Hive -- mk/crate, box target, mealkits target (overall and by recipes)
# MAGIC 1. ingest "Pack Guide" data and register Hive -- Picks by recipes for now

# COMMAND ----------

