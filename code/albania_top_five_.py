from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

LIMIT = 5

spark = SparkSession.builder \
    .appName("CrudeOilAnalysis") \
    .getOrCreate()

df = spark.read.csv("file:///app/data.csv", header=True, inferSchema=True)
df = df.filter(col('originName') == 'Albania')
df = df.withColumn("quantity", df["quantity"].cast("int"))
df = df.groupBy('destinationName').agg(sum("quantity").alias("total"))
df = df.orderBy(col("total").desc()).limit(LIMIT)

# Show the result
df.show(LIMIT, False)


df.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.default.albania_top_five")

# Stop Spark session
spark.stop()
