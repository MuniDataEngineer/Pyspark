from pyspark.sql import *
from pyspark.sql.functions import col,round,cast,sum,count

#sparkSession
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()

#Reading_file
df_raw = spark.read.format("csv").option("header",True).load("/content/Pyspark/Retail_Sales_Analysis/sales_data.csv")                         

#Adding revenue field
df_with_revenue = df_raw.withColumn("revenue",round(col("quantity")*col("price"),2))

#Calculating no of each products sold per region
df_top_sold = df_with_revenue.groupBy("product_name","region").agg(sum("quantity").cast("int").alias("total_sold")).orderBy("region","total_sold",ascending=False)

#Calculating total revenue per region
df_total_revenue = df_with_revenue.groupBy("region").agg(round(sum("revenue"),2).alias("total_revenue")).orderBy("region")

#frequent buyers
df_frequent_buyers_list = df_with_revenue.groupBy("buyer").agg(count("order_id").alias("No_Of_Oders")).orderBy("No_Of_Oders",ascending=False)

#Result

print("raw_data")
df_raw.show(truncate=False)

print("No of each products sold per region")
df_top_sold.show(truncate=False)
  
print("Total revenue per region")
df_total_revenue.show(truncate=False)

print("frequent buyers list")
df_frequent_buyers_list.show(truncate=False)
