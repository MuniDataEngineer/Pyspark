from pyspark.sql import *
from jsonHandler import jsonHandler
#sparksession
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()

#Get the json file:
file_path = input("Provide the file path:")
file_type = file_path.lower().split(".")[-1]

if file_type == "json":
  df = spark.read.option("multiline", True).json(file_path)
  df = jsonHandler(df)
  df.show()
else:
  print("This is not a json file")
