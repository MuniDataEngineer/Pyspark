
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder         .appName("BasicPySparkApp")         .getOrCreate()

    #data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    #columns = ["Name", "Age"]
    df = spark.read.json("/content/Pyspark/titanic.json")
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()
