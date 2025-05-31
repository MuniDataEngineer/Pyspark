from pyspark.sql import *
from pyspark.sql.functions import to_timestamp,count,hour,date_format
import re

#creating variables and funtions
data = []
log_pattern = '(\S+) - - \[(.*)\] \"(.*)\" (\d{3}) (\d+)'

def logFileAnalyzer(value):
  matched = re.match(log_pattern,value)
  if bool(matched):
    data.append({"IP":matched.group(1),"Timestamp":matched.group(2),"Request":matched.group(3),"Status":matched.group(4),"Size":matched.group(5)})
  else:
    None
    
#creating spark session
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()

#reading file
raw_data = spark.read.text("/content/Pyspark/access_log_sample.log")

#calling funtion
for i in raw_data.collect():
  a = logFileAnalyzer(i["value"])
#original_file
print("Raw_data")
raw_data.show(truncate=False)
#Final_daaframe
print("Transformed_Data")
df = spark.createDataFrame(data)
df.show(truncate=False)

#top visited pages 
Top_visited_page = df.groupBy("Request").agg(count("Request").alias("Total_No_Of_Time_Visited")).orderBy("Total_No_Of_Time_Visited", ascending = False)
print("Top visited pages")
Top_visited_page.show(truncate=False)

#Top accessed IP's
Top_Accessed_IP = df.groupBy("IP").agg(count("IP").alias("Total_No_Of_Time_accessed")).orderBy("Total_No_Of_Time_accessed",ascending = False)
print("Top accessed IP's")
Top_Accessed_IP.show(truncate=False)

#per hour level details
df_timestamp_caste = df.withColumn("Timestamp",to_timestamp("Timestamp","dd/MMM/yyyy:HH:mm:ss Z")).withColumn("hour",hour("Timestamp")).withColumn("date",date_format("Timestamp","MM/dd/yyyy"))
df_hour_level_page = df_timestamp_caste.groupBy("Request","date","hour").agg(count("Request").alias("No_Of_Time_Visited")).orderBy("No_Of_Time_Visited", ascending = False)
df_hour_level_Ip = df_timestamp_caste.groupBy("IP","date","hour").agg(count("IP").alias("No_Of_Time_accessed")).orderBy("No_Of_Time_accessed", ascending = False)
df_hour_level_both = df_timestamp_caste.groupBy("date","hour","IP","Request").agg(count("IP").alias("No_Of_Time_accessed")).orderBy("No_Of_Time_accessed", ascending = False)

print("Ip accessed per day&hour")
df_hour_level_Ip.show(truncate=False)
print("pages visited per day&hour")
df_hour_level_page.show(truncate=False)
print("No Of Time accessed day&hour")
df_hour_level_both.show(truncate=False)
