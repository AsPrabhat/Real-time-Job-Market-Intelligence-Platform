"""
Scraper Service for CareerRadar
Loads sample job data and publishes to Kafka topic
"""

import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'job_postings')
SAMPLE_DATA_PATH = os.getenv('SAMPLE_DATA_PATH', '/data/sample_jobs.json')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '5'))
BATCH_DELAY = int(os.getenv('BATCH_DELAY', '10'))

def wait_for_kafka(broker, max_retries=30, delay=2):
    """Wait for Kafka to be ready before starting"""
    print(f"Waiting for Kafka broker at {broker}...")
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000
            )
            producer.close()
            print(f"✓ Kafka is ready!")
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Kafka not ready yet... ({str(e)[:50]})")
            time.sleep(delay)
    
    print("✗ Failed to connect to Kafka")
    return False

def load_sample_data(file_path):
    """Load job postings from JSON file"""
    print(f"Loading sample data from {file_path}...")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            jobs = json.load(f)
        
        print(f"✓ Loaded {len(jobs)} job postings")
        return jobs
    
    except FileNotFoundError:
        print(f"✗ Error: Sample data file not found at {file_path}")
        print("Please generate sample data first using: python sample_data/generate_sample_jobs.py")
        return None
    
    except json.JSONDecodeError as e:
        print(f"✗ Error: Invalid JSON in sample data file: {e}")
        return None

def publish_to_kafka(jobs, broker, topic, batch_size=5, batch_delay=10):
    """Publish job postings to Kafka topic in batches"""
    
    print(f"\nInitializing Kafka producer...")
    print(f"  Broker: {broker}")
    print(f"  Topic: {topic}")
    print(f"  Batch size: {batch_size} jobs")
    print(f"  Batch delay: {batch_delay} seconds\n")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        
        print("✓ Kafka producer initialized\n")
        print("=" * 60)
        print("Starting to publish job postings...")
        print("=" * 60)
        
        total_sent = 0
        total_failed = 0
        
        # Publish jobs in batches
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            print(f"\nBatch {batch_num} ({len(batch)} jobs):")
            
            for job in batch:
                try:
                    # Send message to Kafka
                    future = producer.send(topic, value=job)
                    record_metadata = future.get(timeout=10)
                    
                    total_sent += 1
                    print(f"  ✓ Sent: {job['title']} at {job['company']} ({job['location']})")
                    print(f"    → Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                
                except KafkaError as e:
                    total_failed += 1
                    print(f"  ✗ Failed: {job['title']} - {str(e)}")
            
            # Flush after each batch
            producer.flush()
            
            # Wait before next batch (except for last batch)
            if i + batch_size < len(jobs):
                print(f"\n⏳ Waiting {batch_delay} seconds before next batch...")
                time.sleep(batch_delay)
        
        # Final flush
        producer.flush()
        producer.close()
        
        print("\n" + "=" * 60)
        print("Publishing complete!")
        print("=" * 60)
        print(f"Total sent: {total_sent}")
        print(f"Total failed: {total_failed}")
        print(f"Success rate: {(total_sent / len(jobs) * 100):.1f}%")
        
        return total_sent, total_failed
    
    except Exception as e:
        print(f"\n✗ Error initializing Kafka producer: {e}")
        return 0, len(jobs)

def main():
    """Main execution function"""
    
    print("\n" + "=" * 60)
    print("CareerRadar - Job Data Scraper")
    print("=" * 60 + "\n")
    
    # Wait for Kafka to be ready
    if not wait_for_kafka(KAFKA_BROKER):
        print("Exiting due to Kafka connection failure")
        return
    
    # Load sample data
    jobs = load_sample_data(SAMPLE_DATA_PATH)
    if not jobs:
        print("Exiting due to data loading failure")
        return
    
    # Publish to Kafka
    sent, failed = publish_to_kafka(
        jobs, 
        KAFKA_BROKER, 
        KAFKA_TOPIC, 
        batch_size=BATCH_SIZE,
        batch_delay=BATCH_DELAY
    )
    
    if failed > 0:
        print(f"\n⚠ Warning: {failed} messages failed to send")
    else:
        print(f"\n✓ All {sent} job postings successfully published to Kafka!")
    
    print("\n" + "=" * 60)
    print("Scraper service completed")
    print("=" * 60)

if __name__ == "__main__":
    main()
