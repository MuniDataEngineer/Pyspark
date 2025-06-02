from pyspark.sql import *
from DataType_Detection import detect_type
import re
from collections import Counter

inferred_types = {}

#Sparksession
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()

#Reading a file
Raw_data = spark.read.format("csv").option("Header",True).load("/content/Pyspark/sample.csv")

#validating the field names
new_cols = [re.sub(r"[^a-zA-Z0-9_\s]", "", col).strip().lower().replace(" ","_") for col in Raw_data.columns]
df = Raw_data.toDF(*new_cols)

#sample data to get the datatype
sample_df = df.limit(100)

#to detect datatype
for cols in sample_df.columns:
  my_list = [row[cols] for row in sample_df.select(cols).collect()]
  detected = [detect_type(val) for val in my_list]
  type_counts = Counter(detected)
  detected_type = type_counts.most_common(1)[0][0]
  inferred_types[cols] = detected_type
  df = df.withColumn(cols,col(cols).cast(detected_type))

#results
print("Raw_data")
Raw_data.show(truncate=False)

print("Transformed_data")
df.show(truncate=False)

print("compare datatype")
print("Raw_data")
print(Raw_date.schema)
print("Transformed_data")
print(df.schema)



