# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 0.1: Check required modules

# COMMAND ----------

# install on cluster for permanant
%pip list

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0.2: Mounting my azure blob sorage
# MAGIC #### valid mount points

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/kitting_gsheet/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0.3: packages in use

# COMMAND ----------

import requests as rq
import pandas as pd
import datetime as dt
import re
from pytz import timezone

# COMMAND ----------

calendar = dt.datetime.now(timezone('Pacific/Auckland')).isocalendar()
year = calendar[0]
week = calendar[1]
if (week < 10):
  production_week = str(year) +'-0'+ str(week)
else:
  production_week = str(year) + '-' + str(week)
  
print(production_week)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 1.1 : Download Excel file
# MAGIC 
# MAGIC TODOS: 
# MAGIC Replace publishing gsheet as excel to google sheet credential configuration

# COMMAND ----------

url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSIIN0WgaQuPDzgWVqpsAaz6mzzhJUPmkpauNKVJqbdnFm_RjbEbupY47iP7whhDD1ftgQuZvM4AdPT/pub?output=xlsx" # my gsheet
resp = rq.get(url)

# COMMAND ----------

local_path = "/mnt/kitting_gsheet/" # my blob storage path
file_path = "/dbfs" + local_path + f'{production_week}_raw.xlsx'
print(file_path)  # the location of raw data

# COMMAND ----------

# dbutils.fs.rm("/mnt/kitting_gsheet/2020-50", recurse = True)

# COMMAND ----------

# MAGIC %md
# MAGIC validating the data path (to be deleted in production env)
# MAGIC by listing

# COMMAND ----------

with open(file_path, 'wb') as f: ## excel format wb
  f.write(resp.content)
  f.truncate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.2: Stage data

# COMMAND ----------

# read file into data frame
data = {}
with pd.ExcelFile(file_path) as xlsx: #Change the file path here
    data['KL1'] = pd.read_excel(xlsx,'Kit line 1',index_col = None,na_values='NA')
    data['KL2'] = pd.read_excel(xlsx,'Kit line 2',index_col = None,na_values='NA')#, skiprows=1)
    data['KL3'] = pd.read_excel(xlsx,'Kit line 3',index_col = None,na_values='NA')
    data['KL4'] = pd.read_excel(xlsx,'Kit line 4',index_col = None,na_values='NA')
    data['KL5'] = pd.read_excel(xlsx,'Kit line 5',index_col = None,na_values='NA')
    data['KL6'] = pd.read_excel(xlsx,'Kit line 6',index_col = None,na_values='NA')
    data['KL7'] = pd.read_excel(xlsx,'Kit line 7',index_col = None,na_values='NA')
    data['KL8'] = pd.read_excel(xlsx,'Kit line 8',index_col = None,na_values='NA')
    data['KL9'] = pd.read_excel(xlsx,'Kit line 9',index_col = None,na_values='NA')
    data['KL10'] = pd.read_excel(xlsx,'Kit line 10',index_col = None,na_values='NA')
    data['KL11'] = pd.read_excel(xlsx,'Kit line 11',index_col = None,na_values='NA')
    data['KL12'] = pd.read_excel(xlsx,'Kit line 12',index_col = None,na_values='NA')
    data['KL13'] = pd.read_excel(xlsx,'Kit line 13',index_col = None,na_values='NA')
    data['KL14'] = pd.read_excel(xlsx,'Kit line 14',index_col = None,na_values='NA')
    data['KL15'] = pd.read_excel(xlsx,'Kit line 15',index_col = None,na_values='NA')
    data['KL16'] = pd.read_excel(xlsx,'Kit line 16',index_col = None,na_values='NA')
    data['KL17'] = pd.read_excel(xlsx,'Kit line 17',index_col = None,na_values='NA')
    data['KL18'] = pd.read_excel(xlsx,'Kit line 18',index_col = None,na_values='NA')
    data['KL19'] = pd.read_excel(xlsx,'Kit line 19',index_col = None,na_values='NA')
    data['KL20'] = pd.read_excel(xlsx,'Kit line 20',index_col = None,na_values='NA')

# Join all kitting lines' data to one
df = pd.DataFrame(data=None)   # df is the frame containing all raw data
for KL in data:
    sLength = len(data[KL])
    KL_array = [KL for i in range(sLength)]
    data[KL]['kittingline'] = KL_array               #Meanwhile add a new column with the kitting line's name
    df = pd.concat([df,data[KL]], sort = False)

# COMMAND ----------

# rename all the column index
df.columns = ['Barcode', 'Production Batch', 'Recipe and P', 'Timestamp', 'Date', 'Seq Code', 'Week', 'Team Leader', 'Replenisher', 'Pickers', 'Break Reasons', 'Missing Products', 'Kitting Line']
df.sort_values(by=['Timestamp'], ascending=False).head(10)

# COMMAND ----------

# Check timestamps & date whether have the right format
def check_data_type(dataframe, col):
    for i in range(len(dataframe)):
        if type(dataframe[col].iloc[i]) not in [dt.time, pd._libs.tslibs.timestamps.Timestamp]:
            print("check the {}th data".format(i))
            print(dataframe.iloc[i])
            return
    print("Pass Check -- {}".format(col))

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Add a function that can email me which data need to be manually corrected

# COMMAND ----------

check_data_type(df, 'Timestamp')
check_data_type(df, 'Date')

# COMMAND ----------

def combine_date_time(df, datecol, timecol):
    return df.apply(lambda row: row[datecol].replace(
                                hour=row[timecol].hour,
                                minute=row[timecol].minute,
                                second=row[timecol].second),
                    axis=1)

new_c = combine_date_time(df, 'Date', 'Timestamp')
new_c

# COMMAND ----------

df['Timestamp and Date'] = new_c
df = df.drop_duplicates(subset = None, keep='last') # kept 'last record' for a reason

# COMMAND ----------

for KL in data:
    df_new = df.loc[df['Kitting Line'] == KL] # df_new is the sliced raw data of "Kitting Line(KL) Name"
    # 1.(after dropping duplicates) for every kitting line it has to contain batch code [1:7]
    i = pd.Categorical(df_new['Production Batch'])
    for j in range (1,8):
        if j not in i.categories:
            print("Warning: %r , production batch %s is not included" %(KL,j))
        else:
            print("%r production batch %s included" %(KL,j))

# COMMAND ----------

df.head()

# COMMAND ----------

staged_file_path = '/dbfs'+local_path + f'{production_week}_staged.csv'
print(staged_file_path) ## the location of "staged data"
df.to_csv(staged_file_path,index=False)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.3: Transform timestamp data to discrete event data

# COMMAND ----------

#the event is depending on the barcode scanned afterwoards

def activity_dependency(barcode):
    switcher = {
        'Production Start': 'Factory Closed',
        'New Recipe Start': 'Preparation/Changeover',
        'Break End': 'Break',
        'Production Finish': 'TBD'
    }
    return switcher.get(barcode, 'Production')
  
# 7 days of Assembly

def batchcode_dependency(batchcoded):
    switcher = {
        1: 'Friday Assembly',
        2: 'Saturday Assembly',
        3: 'Sunday Assembly',
        4: 'Monday Assembly',
        5: 'Tuesday Assembly',
        6: 'Wednesday Assembly',
        7: 'Thursday Assembly'
    }
    return switcher.get(batchcoded, '')  
  
# morning shift -- 5:00 to 14:00
# afternoon shift -- 14:00 to 23:00
# Error -- Events compeleted in other time periods

def categorize_event_shift(timestamp):
    weekday_index = pd.Timestamp(timestamp).weekday()
    switcher = {
        0: 'Monday',
        1: 'Tuesday',
        2: 'Wednesday',
        3: 'Thursday',
        4: 'Friday',
        5: 'Saturday',
        6: 'Sunday'
    }
    weekday = switcher.get(weekday_index,'')
    hour = pd.Timestamp(timestamp).hour
    if hour in range(5,14):
        shift = 'Morning'
    elif hour in range(14,23):
        shift = 'Afternoon'
    else:
        shift = 'Error'
    return weekday +' '+ shift  
  
#calculting time cost to sec

def time_consumption(timestamp1,timestamp2):
    t1 = pd.to_datetime(timestamp1)
    t2 = pd.to_datetime(timestamp2)
    InMin = pd.Timedelta(t2-t1).seconds/60.0
    return format(InMin, '.2f')

# count how many pickers for ever events (+2 to get the total amount of people on this kitting line)  
def count_pickers(picker):
    pattern = re.compile(r';|_|,')  ## <- temporary solutions overhere, may change according to the labels printed
    picker_array = pattern.findall(picker)  
    return len(picker_array)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### read in staged data

# COMMAND ----------

staged_df = pd.read_csv(staged_file_path)
staged_df.head()

# COMMAND ----------

# copy raw df
df_final = pd.DataFrame(data=None) #df is the event data frame

def main(df_temp_raw):
    df_temp_event = pd.DataFrame(columns=('Start Time', 'Finish Time', 'Activity', 'Seq Code' ,'Recipe Name', 'Break Reasons', 'Missing Ingredients', 'Kitting Line', 'Assembly Batch', 'Event Shift', 'Team Leader', 'Pickers Count', 'Time Consumption'))
    index_align = df_temp_raw.first_valid_index() # pandas is using the df_temp_raw's frame index whenever df_temp_raw is called
    df_temp_event['Finish Time'] = df_temp_raw['Timestamp and Date']
    df_temp_event['Seq Code'] = df_temp_raw['Seq Code']
    df_temp_event['Start Time'].loc[index_align] = '2019-1-1 00:00:00'
    #df_temp_event['Recipe Name'][1] = df_temp_raw['Recipe and P'].loc[df_temp_raw['Recipe and P'].first_valid_index()]
    #print(df_temp_event)
    df_temp_raw.loc[index_align, 'Recipe and P'] = df_temp_raw['Recipe and P'].loc[df_temp_raw['Recipe and P'].first_valid_index()] #ignore the warning -- data type is not changing, the return is in VIEW

    for i in range (len(df_temp_raw)):
        df_temp_event['Start Time'].loc[index_align+i+1] = df_temp_raw['Timestamp and Date'].loc[index_align+i]
        df_temp_event['Activity'].loc[index_align+i] = activity_dependency(df_temp_raw['Barcode'].loc[index_align+i])
        df_temp_event['Break Reasons'].loc[index_align+i+1] = df_temp_raw['Break Reasons'].loc[index_align+i]
        df_temp_event['Missing Ingredients'].loc[index_align+i+1] = df_temp_raw['Missing Products'].loc[index_align+i]
        df_temp_event['Kitting Line'].loc[index_align+i+1] = df_temp_raw['Kitting Line'].loc[index_align+i]
        df_temp_event['Assembly Batch'].loc[index_align+i] = batchcode_dependency(df_temp_raw['Production Batch'].loc[index_align+i])
        df_temp_event['Time Consumption'].loc[index_align+i] = time_consumption(df_temp_event['Start Time'].loc[index_align+i], df_temp_event['Finish Time'].loc[index_align+i])
        df_temp_event['Event Shift'].loc[index_align+i] = categorize_event_shift(df_temp_event['Finish Time'].loc[index_align+i])
        df_temp_event['Team Leader'].loc[index_align+i] = df_temp_raw['Team Leader'].loc[index_align+i]
        df_temp_event['Pickers Count'].loc[index_align+i] = count_pickers(df_temp_raw['Pickers'].loc[index_align+i])
        #print(df_temp_event)
        """
        for j in range (len(df_temp_raw)):  #############################################
            print(df_temp_raw['Recipe and P'][::-1].loc[(index_align + j):])
            reversed_index = df_temp_raw['Recipe and P'][::-1].loc[(index_align + j):].first_valid_index()
        """
        #df_temp_raw['Recipe and P'][index_align] = df_temp_raw['Recipe and P'].loc[df_temp_raw['Recipe and P'].first_valid_index()]
        recipe_index = df_temp_raw['Recipe and P'].loc[0:index_align+i].last_valid_index()
        #print(recipe_index)
        df_temp_event['Recipe Name'].loc[index_align+i] = df_temp_raw['Recipe and P'].loc[recipe_index]
        #print(df_temp_raw['Recipe and P'].loc[recipe_index])
    #print(df_temp_event)
    return df_temp_event

# COMMAND ----------

for kl in range(1,21):
    df_temp_raw = staged_df.loc[staged_df['Kitting Line'] == 'KL%s'%kl]
    if df_temp_raw.empty:
        print("Kitting Line %r is not included!" %(kl))  # if data frame is empty
    #print(df_temp_raw)
    else:
        seg = main(df_temp_raw)
        #print(seg)
        df_final = pd.concat([df_final, seg], sort = False)
        print("Kitting Line %r Completed!" %(kl))

# COMMAND ----------

## Attach a column to identify the Week
## Update weekly
df_final['Week'] = production_week
df_final.tail()

# COMMAND ----------

prepared_file_path = '/dbfs'+ local_path + f'{production_week}_prepared.csv'
print(prepared_file_path) # the location of prepared data
df_final.to_csv(prepared_file_path, index = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1.4 Create temporary (bronze) table to ghold newly arrived data

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
    StructField("Week", StringType(), True)
  ]) # same as bigquery

trimed_path = local_path + f'{production_week}_prepared.csv'
df_new = spark.read.csv(trimed_path, header = True, schema = schema)
# new_column_name_list = list(map(lambda x: x.replace(" ", "_"), df.columns))
# df_new = df_new.toDF(*new_column_name_list)
df_new.createOrReplaceGlobalTempView("bronze") # register in global_temp

# COMMAND ----------

display(df_new)

# COMMAND ----------

