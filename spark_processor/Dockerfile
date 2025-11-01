FROM apache/spark:3.5.0

USER root

# Create Ivy cache directory with proper permissions
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.0.0 \
    kafka-python==2.0.2

# Create app directory
WORKDIR /app

# Copy processor script
COPY process.py .

# Set ownership
RUN chown -R spark:spark /app

# Set user back to spark
USER spark

# Run the processor
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "/app/process.py"]
