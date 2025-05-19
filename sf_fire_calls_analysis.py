# SF Fire Department Calls Analysis using PySpark (Databricks)

# Step 1: Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Step 2: Initialize Spark Session
spark = SparkSession.builder.appName("SF Fire Calls Analysis").getOrCreate()

# Step 3: Read CSV into DataFrame
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/FileStore/tables/SF_Fire_Dept_Calls.csv")  # Update path as needed

# Step 4: Initial View
df.show()
# (In Databricks notebooks, display(df) can be used for rich display)

# Step 5: Rename Columns for Consistency
rename_df = df.withColumnRenamed("Call Number", "CallNumber") \
              .withColumnRenamed("Unit Type", "UnitType") \
              .withColumnRenamed("Call Type", "CallType") \
              .withColumnRenamed("Call Date", "CallDate") \
              .withColumnRenamed("Watch Date", "WatchDate") \
              .withColumnRenamed("Received DtTm", "ReceivedDtTm") \
              .withColumnRenamed("Entry DtTm", "EntryDtTm") \
              .withColumnRenamed("Dispatch DtTm", "DispatchDtTm") \
              .withColumnRenamed("Response DtTm", "ResponseDtTm") \
              .withColumnRenamed("On Scene DtTm", "OnSceneDtTm") \
              .withColumnRenamed("Call Final Disposition", "FinalDisposition") \
              .withColumnRenamed("Available DtTm", "AvailableDtTm") \
              .withColumnRenamed("Zipcode of Incident", "ZipcodeofIncident")

rename_df.show()

# Q1 & Q2: How many and what are the distinct types of calls?
q1_df = rename_df.where("CallType is not null") \
                .select("CallType").distinct()

q1_df.show()

# Q3: Find all response delays > 5 minutes
q3_df = rename_df.where("Delay > 5") \
                .select("CallNumber", "Delay")

q3_df.show()

# Q4: Most common call types
q4_df = rename_df.where("CallType is not null") \
                .groupBy("CallType") \
                .count() \
                .orderBy(desc("count"))

q4_df.show()

# Q5: Zip codes with the most common calls
q5_df = rename_df.where("ZipcodeofIncident is not null") \
                .groupBy("ZipcodeofIncident") \
                .count() \
                .orderBy(desc("count"))

q5_df.show()

# Q6: Neighborhoods in zip codes 94102 and 94103
q6_df = rename_df.filter(col("ZipcodeofIncident").isin(94102, 94103)) \
                .select("Neighborhood", "ZipcodeofIncident").distinct()

q6_df.show()

# Q7: Sum, Average, Min, and Max of Delay
q7_df = rename_df.select(
    sum("Delay").alias("Total_Delay"),
    avg("Delay").alias("Average_Delay"),
    min("Delay").alias("Min_Delay"),
    max("Delay").alias("Max_Delay")
)

q7_df.show()

# Q8: Distinct years in dataset
q8_df = rename_df.select(year("CallDate").alias("Year")).distinct().orderBy("Year")

q8_df.show()

# Q9: Week of 2018 with most calls
q9_df = rename_df.filter(year("CallDate") == 2018) \
                .groupBy(weekofyear("CallDate").alias("Week")) \
                .count() \
                .orderBy(desc("count"))

q9_df.show()

# Q10: Worst response time neighborhoods in 2018
q10_df = rename_df.filter(year("CallDate") == 2018) \
                  .groupBy("Neighborhood") \
                  .agg(avg("Delay").alias("Avg_ResponseTime")) \
                  .orderBy(desc("Avg_ResponseTime"))

q10_df.show()

# Step 6: Stop Spark session
spark.stop()

