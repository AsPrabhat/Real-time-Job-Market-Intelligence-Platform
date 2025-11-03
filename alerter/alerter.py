import json
import os
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
ALERTS_CONFIG_PATH = "/config/alerts.json"
DELTA_TABLE_PATH = "/opt/spark/delta/job_data"
LAST_CHECK_FILE = "/tmp/last_check_timestamp.txt"

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    logger.info("Creating Spark session...")
    return SparkSession.builder \
        .appName("CareerRadar Alerter") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()

def load_alerts_config():
    """Load alert configuration from JSON file"""
    try:
        with open(ALERTS_CONFIG_PATH, 'r') as f:
            config = json.load(f)
        logger.info(f"Loaded {len(config['alerts'])} alert configurations")
        return config
    except FileNotFoundError:
        logger.error(f"Alert configuration file not found: {ALERTS_CONFIG_PATH}")
        return {"alerts": [], "check_interval_minutes": 15, "smtp_config": {}}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in alerts config: {e}")
        return {"alerts": [], "check_interval_minutes": 15, "smtp_config": {}}

def get_last_check_timestamp():
    """Get the timestamp of the last alert check"""
    try:
        with open(LAST_CHECK_FILE, 'r') as f:
            timestamp_str = f.read().strip()
            return datetime.fromisoformat(timestamp_str)
    except FileNotFoundError:
        # First run, check jobs from last hour
        return datetime.now() - timedelta(hours=1)
    except Exception as e:
        logger.warning(f"Error reading last check timestamp: {e}")
        return datetime.now() - timedelta(hours=1)

def save_last_check_timestamp(timestamp):
    """Save the current check timestamp"""
    try:
        with open(LAST_CHECK_FILE, 'w') as f:
            f.write(timestamp.isoformat())
    except Exception as e:
        logger.error(f"Error saving last check timestamp: {e}")

def query_matching_jobs(spark, alert_config, since_timestamp):
    """Query Delta Lake for jobs matching alert criteria"""
    try:
        # Read Delta Lake table
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        
        # Filter by timestamp (jobs processed since last check)
        df = df.filter(col("processed_timestamp") > since_timestamp.isoformat())
        
        # Filter by location if specified
        if alert_config.get("locations"):
            df = df.filter(col("location").isin(alert_config["locations"]))
        
        # Filter by seniority level if specified
        if alert_config.get("seniority_levels"):
            df = df.filter(col("seniority").isin(alert_config["seniority_levels"]))
        
        # Filter by minimum skills count if specified
        if alert_config.get("min_skills_count"):
            df = df.filter(col("skills_count") >= alert_config["min_skills_count"])
        
        # Filter by keywords (check if any keyword exists in skills_extracted)
        if alert_config.get("keywords"):
            keyword_conditions = None
            for keyword in alert_config["keywords"]:
                condition = array_contains(col("skills_extracted"), keyword)
                keyword_conditions = condition if keyword_conditions is None else keyword_conditions | condition
            
            if keyword_conditions is not None:
                df = df.filter(keyword_conditions)
        
        # Select relevant columns and convert to Pandas
        result_df = df.select(
            "job_id", "title", "company", "location", "experience", 
            "seniority", "skills_count", "skills_extracted", "link", 
            "posted_date", "processed_timestamp"
        ).limit(50)  # Limit to 50 jobs per alert to avoid overwhelming emails
        
        jobs = result_df.toPandas().to_dict('records')
        logger.info(f"Found {len(jobs)} matching jobs for alert: {alert_config['name']}")
        return jobs
        
    except Exception as e:
        logger.error(f"Error querying matching jobs: {e}")
        return []

def send_email_alert(alert_config, smtp_config, jobs):
    """Send email notification with matching jobs"""
    try:
        # Get email credentials from environment variables
        email_address = os.getenv("EMAIL_ADDRESS")
        email_password = os.getenv("EMAIL_PASSWORD")
        
        if not email_address or not email_password:
            logger.error("Email credentials not found in environment variables")
            return False
        
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = email_address
        msg['To'] = alert_config['email']
        msg['Subject'] = f"CareerRadar Alert: {len(jobs)} New Job(s) - {alert_config['name']}"
        
        # Create HTML body
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px; }}
                .container {{ max-width: 800px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; }}
                h1 {{ color: #00d4ff; }}
                h2 {{ color: #333; border-bottom: 2px solid #00d4ff; padding-bottom: 10px; }}
                .job {{ background-color: #f9f9f9; padding: 15px; margin-bottom: 15px; border-left: 4px solid #00d4ff; }}
                .job-title {{ font-size: 18px; font-weight: bold; color: #333; }}
                .job-company {{ color: #666; font-size: 16px; }}
                .job-details {{ color: #888; font-size: 14px; margin-top: 5px; }}
                .skills {{ background-color: #e8f8ff; padding: 8px; border-radius: 4px; margin-top: 8px; }}
                .skill-tag {{ display: inline-block; background-color: #00d4ff; color: white; padding: 4px 8px; 
                             margin: 2px; border-radius: 4px; font-size: 12px; }}
                .apply-btn {{ display: inline-block; background-color: #00d4ff; color: white; padding: 10px 20px; 
                             text-decoration: none; border-radius: 4px; margin-top: 10px; }}
                .footer {{ text-align: center; color: #888; font-size: 12px; margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>CareerRadar Job Alert</h1>
                <h2>{alert_config['name']}</h2>
                <p>Found <strong>{len(jobs)}</strong> new job posting(s) matching your criteria:</p>
        """
        
        # Add each job
        for job in jobs:
            skills = job.get('skills_extracted', [])
            skills_html = ''.join([f'<span class="skill-tag">{skill}</span>' for skill in skills[:10]])
            
            html_body += f"""
                <div class="job">
                    <div class="job-title">{job.get('title', 'N/A')}</div>
                    <div class="job-company">{job.get('company', 'N/A')} - {job.get('location', 'N/A')}</div>
                    <div class="job-details">
                        Experience: {job.get('experience', 'N/A')} | 
                        Level: {job.get('seniority', 'N/A')} | 
                        Skills Required: {job.get('skills_count', 0)}
                    </div>
                    <div class="skills">
                        {skills_html}
                    </div>
                    <a href="{job.get('link', '#')}" class="apply-btn" target="_blank">View Job</a>
                </div>
            """
        
        html_body += """
                <div class="footer">
                    <p>This is an automated alert from CareerRadar - Real-time Job Market Intelligence Platform</p>
                    <p>To unsubscribe or modify alerts, update your alerts.json configuration file.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Attach HTML body
        msg.attach(MIMEText(html_body, 'html'))
        
        # Send email
        server = smtplib.SMTP(smtp_config['server'], smtp_config['port'])
        server.starttls()
        server.login(email_address, email_password)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email sent successfully to {alert_config['email']}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        return False

def process_alerts():
    """Main function to process all alerts"""
    logger.info("="*60)
    logger.info("CareerRadar Alerter Service Started")
    logger.info("="*60)
    
    # Load configuration
    config = load_alerts_config()
    smtp_config = config.get('smtp_config', {})
    check_interval = config.get('check_interval_minutes', 15)
    
    # Create Spark session
    spark = create_spark_session()
    
    while True:
        try:
            # Get last check timestamp
            last_check = get_last_check_timestamp()
            current_check = datetime.now()
            
            logger.info(f"Checking for new jobs since {last_check.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Process each alert
            for alert in config['alerts']:
                if not alert.get('enabled', True):
                    logger.info(f"Skipping disabled alert: {alert['name']}")
                    continue
                
                logger.info(f"Processing alert: {alert['name']}")
                
                # Query matching jobs
                matching_jobs = query_matching_jobs(spark, alert, last_check)
                
                # Send email if jobs found
                if matching_jobs:
                    send_email_alert(alert, smtp_config, matching_jobs)
                else:
                    logger.info(f"No matching jobs found for: {alert['name']}")
            
            # Save current check timestamp
            save_last_check_timestamp(current_check)
            
            # Wait for next check
            sleep_seconds = check_interval * 60
            logger.info(f"Waiting {check_interval} minutes until next check...")
            time.sleep(sleep_seconds)
            
        except KeyboardInterrupt:
            logger.info("Alerter service stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in alert processing loop: {e}")
            time.sleep(60)  # Wait 1 minute before retrying
    
    spark.stop()
    logger.info("Spark session stopped")

if __name__ == "__main__":
    process_alerts()
