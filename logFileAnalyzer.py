from pyspark.sql import *
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






