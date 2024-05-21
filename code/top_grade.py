from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window
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


df = df.withColumn("quantity", col("quantity").cast("int"))
df = df.groupBy('originName', 'year', 'gradeName').agg(sum("quantity").alias("total_quantity"))


window_spec = Window.partitionBy( col("originName"), col('year')).orderBy(col("total_quantity").desc())
df = df.withColumn("rank", rank().over(window_spec))

df=df.filter(col('rank')==1).select('originName','year', 'gradeName')
#df=df.filter(col('year')==2009)
df.show(200, False)
spark.stop()
