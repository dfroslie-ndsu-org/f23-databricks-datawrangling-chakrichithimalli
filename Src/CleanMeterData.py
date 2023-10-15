from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, expr
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable
from pyspark.sql.functions import col, substring
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, concat, asc

storage_end_point = "chakristorageaccount.dfs.core.windows.net" 
my_scope = "Chakri-ADB"
my_key = "ChakriADB-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

# uri for the storage account
uri = "abfss://chakricontainer@chakristorageaccount.dfs.core.windows.net/"

# reading the data from DailyMeterData.dat 
DailyMeterData_df = spark.read.option("delimiter", "|").csv(uri+"InputData/DailyMeterData.dat", header=True)

# reading the data from CustMeter.csv
custMeter_csv_data = spark.read.csv(uri+'InputData/CustMeter.csv', header=True)

# joining the two dataframes
dat_df_csv_datafile= custMeter_csv_data.join(DailyMeterData_df.drop("Meter Number"), on="Customer Account Number", how = "inner" )

# columns to keep
common_columns = ['Customer Account Number', 'Meter Number', 'ServiceType', 'DT', 'Serial Number', 'Port', 'Channel', 'Conversion Factor', 'Data Type', 'Start Date', 'Start Time']

# list of columns to convert them to rows(QC_columns)
QC_columns = ["QC#1", "QC#2", "QC#3", "QC#4", "QC#5", "QC#6", "QC#7", "QC#8", "QC#9", "QC#10","QC#11", "QC#12", "QC#13", "QC#14", "QC#15", "QC#16", "QC#17", "QC#18", "QC#19", "QC#20"
,"QC#21", "QC#22", "QC#23", "QC#24"]

# list of columns to convert them to rows(Interval_columns)
Interval_columns = ["Interval#1", "Interval#2", "Interval#3", "Interval#4", "Interval#5", "Interval#6", "Interval#7",        
 "Interval#8", "Interval#9", "Interval#10","Interval#11", "Interval#12", "Interval#13", "Interval#14", "Interval#15", "Interval#16", "Interval#17", "Interval#18", "Interval#19", "Interval#20","Interval#21", "Interval#22", "Interval#23", "Interval#24"]
 
def process_data(common_columns, Interval_columns, QC_columns, dat_df_csv_datafile):

    # Unpivot for Interval columns
    df_pivot1 = dat_df_csv_datafile.unpivot(common_columns, QC_columns, "IntervalHour", "QCCode")

    # Slice the "IntervalHour" column to get the number
    df_pivot2 = dat_df_csv_datafile.unpivot(common_columns, Interval_columns, "IntervalHour", "IntervalValue")

    # Slice first 9 characters of "IntervalHour" column to get the number and casting it to Integer
    df1 = df_pivot2.withColumn("IntervalHour", substring("IntervalHour", 10, 2).cast("int"))

    df2 = df_pivot1.withColumn("IntervalHour", substring("IntervalHour", 4, 2).cast("int"))

    combined_df = df1.join(df2, on=common_columns + ["IntervalHour"], how="inner")

    # Drop duplicates
    df_removed_duplicates = combined_df.dropDuplicates()

    # Filter the data
    cleaned_df = df_removed_duplicates.filter(
        (df_removed_duplicates["Data Type"] != "Reverse Energy in Wh") &
        (df_removed_duplicates["Data Type"] != "Net Energy in WH") &
        (df_removed_duplicates["QCCode"] == 3)
    )

    # Sort the filtered DataFrame 
    dataframe_filtered_sorted = cleaned_df.orderBy(
        *[asc(col) for col in common_columns],
        asc("Data Type"),
        asc("Meter Number"),
        asc("Start Date"),
        asc("IntervalHour")
    )

    return df_filtered_sorted

# method call to perform the conversion and cleaning of data
df_filtered_sorted = process_data(common_columns, Interval_columns, QC_columns, dat_df_csv_datafile)  

# saving the parquet file to the storage account output folder
df_filtered_sorted.write.mode('overwrite').parquet(uri+"output/Parquet")


# saving long cleaned csv and parquet files to the storage account output folder using coalesce
df_filtered_sorted.coalesce(1).write.option('header', True).mode('overwrite').parquet(uri+"output/coalesce/Parquet")
df_filtered_sorted.coalesce(1).write.option('header', True).mode('overwrite').csv(uri+"output/CSV")

# saving long cleaned parquet files to the storage account output folder using partition
partitions_count = 1
df_filtered_sorted = df_filtered_sorted.repartition(partitions_count)
# Write the DataFrame to Parquet format
df_filtered_sorted.write.mode('overwrite').parquet(uri+"output/partition/Parquet")

# there are some extra operations while saving the files in output,  multiple ways to perform it were available and I did couple of them, and as per the office hour recording I have considered your statement(on coalesce) and tried a different way of doing it, also did the way instructed in tutorial.