from pyspark.sql import SparkSession

from pyspark.sql.functions import col, sum

# Create Spark session
spark = SparkSession.builder \
.appName("CrudeOilAnalysis") \
.getOrCreate()

# Load Crude oil dataset
df = spark.read\
    .option("header", True)\
    .option("inferSchema", True)\
    .option("quote", "\"")\
    .option("escape", "\"")\
    .csv("file:///app/data.csv")


df = df.filter(col('originName') == 'United Kingdom')
df = df.withColumn("quantity", col("quantity").cast("int"))
df = df.groupBy('destinationName').agg(sum("quantity").alias("total_quantity"))
df = df.filter(col('total_quantity')>100000)
df.show(20, False)
spark.stop()
