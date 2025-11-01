"""
Spark Processor for CareerRadar
Consumes job data from Kafka, enriches with skill extraction, writes to Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, array_distinct, size, 
    current_timestamp, to_date, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType
)
from delta import configure_spark_with_delta_pip
import time
import re

# Technical skills to extract (aligned with your resume and target roles)
SKILLS_DATABASE = [
    # Programming Languages
    "Python", "C++", "Java", "JavaScript", "TypeScript", "Go", "Rust", "SQL", "R", "Scala",
    
    # Data Engineering
    "Apache Spark", "Spark", "PySpark", "Kafka", "Delta Lake", "PostgreSQL", "MySQL", 
    "MongoDB", "Redis", "Cassandra", "Hadoop", "Hive", "Presto", "Snowflake",
    
    # Cloud & Infrastructure
    "AWS", "Azure", "GCP", "Docker", "Kubernetes", "Terraform", "Ansible",
    
    # AI/ML Technologies
    "Machine Learning", "Deep Learning", "NLP", "Computer Vision", "LLM", 
    "Generative AI", "GPT", "OpenAI", "RAG", "Vector Databases", "Embeddings",
    "LangChain", "Prompt Engineering", "Fine-tuning", "Hugging Face",
    
    # ML Frameworks
    "TensorFlow", "PyTorch", "scikit-learn", "Keras", "XGBoost", "LightGBM",
    
    # Data Science
    "Pandas", "NumPy", "Matplotlib", "Seaborn", "Jupyter", "Data Analysis",
    
    # Backend & APIs
    "Node.js", "Express", "Flask", "FastAPI", "Django", "REST API", "GraphQL", "gRPC",
    
    # Frontend
    "React", "Angular", "Vue.js", "HTML", "CSS",
    
    # DevOps & Tools
    "Git", "GitHub", "GitLab", "CI/CD", "Jenkins", "GitHub Actions",
    
    # Specialized
    "Distributed Systems", "Microservices", "ETL", "Data Pipeline", 
    "Data Warehouse", "Big Data", "Stream Processing", "Batch Processing"
]

def extract_skills(description, skills_list):
    """
    Extract technical skills from job description
    Uses case-insensitive matching with word boundaries
    """
    if not description:
        return []
    
    description_lower = description.lower()
    found_skills = []
    
    for skill in skills_list:
        # Create pattern with word boundaries for exact matching
        pattern = r'\b' + re.escape(skill.lower()) + r'\b'
        if re.search(pattern, description_lower):
            found_skills.append(skill)
    
    return list(set(found_skills))  # Remove duplicates

def count_skills(skills_required, skills_extracted):
    """Count total unique skills from both sources"""
    if not skills_required:
        skills_required = []
    if not skills_extracted:
        skills_extracted = []
    
    all_skills = set(skills_required + skills_extracted)
    return len(all_skills)

def classify_seniority(experience_level, skills_count):
    """
    Classify job seniority based on experience level and skills required
    """
    if experience_level == "Internship":
        return "Junior"
    elif experience_level == "Entry-Level":
        return "Junior" if skills_count < 5 else "Mid"
    elif experience_level == "Mid-Level":
        return "Mid" if skills_count < 8 else "Senior"
    elif experience_level == "Senior":
        return "Senior"
    else:
        return "Mid"  # Default

# Register UDFs
extract_skills_udf = udf(lambda desc: extract_skills(desc, SKILLS_DATABASE), ArrayType(StringType()))
count_skills_udf = udf(count_skills, IntegerType())
classify_seniority_udf = udf(classify_seniority, StringType())

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    print("Creating Spark session with Delta Lake support...")
    
    builder = SparkSession.builder \
        .appName("CareerRadar-JobProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/spark/delta/checkpoints") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("✓ Spark session created successfully")
    return spark

def define_schema():
    """Define schema for incoming job data"""
    return StructType([
        StructField("job_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("company", StringType(), True),
        StructField("location", StringType(), True),
        StructField("experience", StringType(), True),
        StructField("experience_level", StringType(), True),
        StructField("skills_required", ArrayType(StringType()), True),
        StructField("description", StringType(), True),
        StructField("posted_date", StringType(), True),
        StructField("link", StringType(), True),
        StructField("salary_range", StringType(), True)
    ])

def process_stream(spark, kafka_broker, kafka_topic, delta_path):
    """
    Main streaming processing logic:
    1. Read from Kafka
    2. Parse JSON
    3. Extract skills from description
    4. Classify seniority
    5. Write to Delta Lake
    """
    
    print(f"\n{'='*60}")
    print("Starting Spark Structured Streaming Job")
    print(f"{'='*60}")
    print(f"Kafka Broker: {kafka_broker}")
    print(f"Kafka Topic: {kafka_topic}")
    print(f"Delta Lake Path: {delta_path}")
    print(f"{'='*60}\n")
    
    # Define schema
    schema = define_schema()
    
    # Read from Kafka
    print("Connecting to Kafka stream...")
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✓ Connected to Kafka stream")
    
    # Parse JSON from Kafka value
    print("Parsing JSON messages...")
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Extract skills from description
    print("Applying skill extraction...")
    enriched_df = parsed_df \
        .withColumn("skills_extracted", extract_skills_udf(col("description"))) \
        .withColumn("skills_count", count_skills_udf(col("skills_required"), col("skills_extracted"))) \
        .withColumn("seniority", classify_seniority_udf(col("experience_level"), col("skills_count"))) \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("posted_date", to_date(col("posted_date")))
    
    # Select final columns
    final_df = enriched_df.select(
        "job_id",
        "title",
        "company",
        "location",
        "experience",
        "experience_level",
        "seniority",
        "skills_required",
        "skills_extracted",
        "skills_count",
        "description",
        "posted_date",
        "link",
        "salary_range",
        "processed_timestamp",
        "kafka_timestamp"
    )
    
    print("✓ Enrichment pipeline configured")
    
    # Write to Delta Lake
    print(f"\nWriting to Delta Lake: {delta_path}")
    print("Starting streaming query...\n")
    
    query = final_df \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{delta_path}/_checkpoints") \
        .option("path", delta_path) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print(f"{'='*60}")
    print("✓ Streaming query started successfully")
    print(f"{'='*60}\n")
    
    return query

def main():
    """Main execution function"""
    
    # Configuration
    KAFKA_BROKER = "kafka:9092"
    KAFKA_TOPIC = "job_postings"
    DELTA_PATH = "/opt/spark/delta/job_data"
    
    print("\n" + "="*60)
    print("CareerRadar - Spark Processor Service")
    print("="*60 + "\n")
    
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(10)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Start processing
        query = process_stream(spark, KAFKA_BROKER, KAFKA_TOPIC, DELTA_PATH)
        
        # Monitor progress
        print("Monitoring streaming query...")
        print("Press Ctrl+C to stop\n")
        
        while query.isActive:
            progress = query.lastProgress
            if progress:
                print(f"[{progress['timestamp']}] " +
                      f"Batch: {progress['batchId']}, " +
                      f"Rows: {progress['numInputRows']}, " +
                      f"Rate: {progress.get('inputRowsPerSecond', 0):.2f} rows/sec")
            time.sleep(10)
        
        # Wait for termination
        query.awaitTermination()
    
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        query.stop()
        spark.stop()
        print("✓ Processor stopped")
    
    except Exception as e:
        print(f"\n✗ Error in processing: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()

if __name__ == "__main__":
    main()
