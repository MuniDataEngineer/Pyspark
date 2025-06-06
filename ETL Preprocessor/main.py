from pyspark.sql import *
from DataType_Detection import detect_type
import re
from collections import Counter
from pyspark.sql.functions import col
from Type_detection_using_genai import type_Detection
from Field_name_validation import fieldNameValidation

inferred_types = {}

#get the API key 
api_key = input("paste your gemini api key here...")

#get file from the user
file_path = input("Provide the file path:")
file_type = file_path.lower().split(".")[-1]

#Sparksession
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()

#Reading a file
if file_type == "csv":
  Raw_data = spark.read.format(file_type).option("Header",True).load(file_path)
elif file_type == "json" or file_type == "parquet":
  Raw_data = spark.read.format(file_type).load(file_path)

#validating the field names
fields = [col for col in Raw_data.columns]
new_fields = fieldNameValidation(fields)
df = Raw_data.toDF(*new_fields)

#sample data to get the datatype
sample_df = df.limit(100)

#to detect datatype
inferred_types = type_Detection(api_key,sample_df)
for cols,types in inferred_types.items():
  df = df.withColumn(cols,col(cols).cast(types))

#results
print("Raw_data")
Raw_data.show(truncate=False)

print("Transformed_data")
df.show(truncate=False)

print("compare datatype")
print("Raw_data")
print(Raw_data.schema)
print("Transformed_data")
print(df.schema)


