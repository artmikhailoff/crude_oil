
I had to create spark-iceberg image to run it with iceberg libs,
though I was getting exception due to metastore, so I reached 2hr limit with this task and stopped,

numbers could be seen in logs directory



Build Docker image 
docker build -t spark-iceberg .

How to Run tasks 
#1
docker run -it --rm     -v /home/admin/code:/app     spark-iceberg     --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,org.apache.iceberg:iceberg-aws-bundle:1.4.0     /app/albania_top_five_.py 2>&1 > albania.log

#2
docker run -it --rm     -v /home/admin/code:/app     spark-iceberg     --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,org.apache.iceberg:iceberg-aws-bundle:1.4.0     /app/top_grade.py  2>&1 top_grade.log


#3
docker run -it --rm     -v /home/admin/code:/app     spark-iceberg     --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0,org.apache.iceberg:iceberg-aws-bundle:1.4.0     /app/uk_quantity.py 2>&1 >uk_quantity.log

Have a nice day!
