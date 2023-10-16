# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql import functions
from pyspark.sql.functions import sum, max, when
from pyspark.sql.types import IntegerType

storage_end_point = "chakristorageaccount.dfs.core.windows.net" 
my_scope = "Chakri-ADB"
my_key = "ChakriADB-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

# Replace the container name (assign-1-blob) and storage account name (assign1storage) in the uri.
uri = "abfss://chakricontainer@chakristorageaccount.dfs.core.windows.net/"

cleaned_parquet_data = spark.read.parquet(uri+"output/partition/Parquet")

custMeter_csv_data = spark.read.csv(uri+'InputData/CustMeter.csv', header=True)

# COMMAND ----------

# Question 1
# 1. What's the total electrical usage for the day?

total_electrical_usage_day = cleaned_parquet_data.agg({"IntervalValue": "sum"}).collect()[0]['sum(IntervalValue)']

print(f"total electrical usage day is :" + str(total_electrical_usage_day))


# COMMAND ----------

# Question 2
# 2. What's the total electrical usage for 'Residental' customers for the day?

total_usage_for_day_residential = cleaned_parquet_data.filter(cleaned_parquet_data["ServiceType"] == "Residential").agg({"IntervalValue": "sum"}).collect()[0]['sum(IntervalValue)']

print(f"total electrical usage for 'Residental' customers for the day: " +str(total_usage_for_day_residential))

# COMMAND ----------

# Question 3
# 3. What's the total electrical usage for hour 7 of the day?

total_usage_for_7_hour = cleaned_parquet_data.filter(cleaned_parquet_data["IntervalHour"] == "7").agg({"IntervalValue": "sum"}).collect()[0]['sum(IntervalValue)']

print("the total electrical usage for hour 7 of the day: " + str(total_usage_for_7_hour))



# COMMAND ----------

# Question 4
#  What are the top 5 meters in terms of usage for the day and how much power did they use?

meter_usage = cleaned_parquet_data.groupBy("Meter Number").agg(functions.sum("IntervalValue").alias("TotalUsage"))

# Find the top 5 meters with the highest total usage
top_5_meters = meter_usage.orderBy(functions.desc("TotalUsage")).limit(5)

# Collect the results into a list
top_meters_list = top_5_meters.collect()

print("The list of top 5 meters with their electrical usage")
top_5_meters.show()


# COMMAND ----------

# Question 5
#5. Which hour had the most usage for the day and what was the total electrical usage?

filtered_data = cleaned_parquet_data.select("IntervalHour", cleaned_parquet_data["IntervalValue"].cast(IntegerType()).alias("IntervalValue"))

hourly_usage = filtered_data.groupBy("IntervalHour").agg(sum("IntervalValue").alias("TotalUsage"))

# Finding the hour with the max total usage
max_hour = hourly_usage.select("IntervalHour").filter(hourly_usage["TotalUsage"] == hourly_usage.agg(max("TotalUsage")).collect()[0][0]).collect()[0][0]

# Find the total electrical usage for the hour with the highest total usage
max_usage = hourly_usage.agg(max("TotalUsage")).collect()[0][0]

print("Hour with maximum electrical usage is: "+str(max_hour)+ ", and total electrical usage was : "+str(max_usage))


# COMMAND ----------

# Question 6
# 6.How many meters are in CustMeter.csv dataset that didn't have any valid readings for the day after cleaning the data?  
# count of distinct meters in clean parquet file
meter_count_from_cleanedParquet = cleaned_parquet_data.select("Meter Number").distinct().count()

# count of distinct meters from CustMeter.csv(uncleaned CustMeter data)
meter_count_from_CustMeter = custMeter_csv_data.select("Meter Number").distinct().count()

display(meter_count_from_CustMeter - meter_count_from_cleanedParquet)

# COMMAND ----------

# Question 7

# Create a new column for each field to check if it's not empty
df = cleaned_parquet_data.withColumn("Customer Account Number", when(col("Customer Account Number") != "", 1).otherwise(0))
df = cleaned_parquet_data.withColumn("Meter Number", when(col("Meter Number") != "", 1).otherwise(0))
df = cleaned_parquet_data.withColumn("Data Type", when(col("Data Type") != "", 1).otherwise(0))

# Calculate the count of combinations where at least one field has data, but not all three
count = df.filter((col("Customer Account Number") + col("Meter Number") + col("Data Type") > 0) & (col("Customer Account Number") + col("Meter Number") + col("Data Type") < 3)).count()

print("Number of combinations with some data but not all:", count)
