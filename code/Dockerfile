# Use an official openjdk runtime as a parent image
FROM openjdk:8-jdk

# Set environment variables
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3

# Download and install Apache Spark
RUN curl -o /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar zxvf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set Spark home environment variable
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Create a directory for the application
RUN mkdir -p /app

# Set working directory
WORKDIR /app

# Add Iceberg jars
RUN mkdir -p $SPARK_HOME/jars && \
    curl -L -o $SPARK_HOME/jars/iceberg-spark-runtime-3.4_2.12-1.4.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.0/iceberg-spark-runtime-3.4_2.12-1.4.0.jar && \
    curl -L -o $SPARK_HOME/jars/iceberg-aws-bundle-1.4.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.0/iceberg-aws-bundle-1.4.0.jar

# Copy application code to the container
COPY . /app

# Set entrypoint
ENTRYPOINT ["spark-submit"]
