# CareerRadar - Real-time Job Market Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-black.svg)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-green.svg)](https://delta.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

A real-time data engineering project that analyzes job market trends using Apache Kafka, Spark, and Delta Lake. Built as part of my B.Tech final year project to understand modern big data technologies and streaming architectures.

## What It Does

- **Streams job postings** through Apache Kafka
- **Processes data in real-time** using Apache Spark to extract skills and classify experience levels
- **Stores enriched data** in Delta Lake (ACID-compliant data lake)
- **Visualizes insights** through an interactive Plotly Dash dashboard
- **Sends email alerts** for matching job opportunities (optional)
- **Runs entirely in Docker** - easy setup with docker-compose

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Scraper   │────▶│    Kafka    │────▶│  Spark Stream    │────▶│ Delta Lake  │
│  (Producer) │     │   (Broker)  │     │   Processor      │     │  (Storage)  │
└─────────────┘     └─────────────┘     └──────────────────┘     └─────────────┘
                                                                           │
                                                                           ▼
                    ┌─────────────┐                              ┌─────────────┐
                    │   Alerter   │◀─────────────────────────────│  Dashboard  │
                    │  (Optional) │                              │ (Plotly Dash)│
                    └─────────────┘                              └─────────────┘
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Ingestion** | Apache Kafka | Message broker for real-time job data streams |
| **Stream Processing** | Apache Spark (PySpark) | Distributed data transformation and enrichment |
| **Data Storage** | Delta Lake | ACID-compliant data lake for versioned storage |
| **Visualization** | Plotly Dash + Flask | Interactive web dashboard with real-time charts |
| **Alerting** | Python (smtplib) | Email notifications for job matches |
| **Orchestration** | Docker Compose | Multi-container deployment and management |
| **Coordination** | Apache Zookeeper | Kafka cluster coordination |

## Dashboard Features

- **Top Skills**: Most in-demand technical skills
- **Experience Levels**: Distribution of internship, entry-level, mid, and senior roles
- **Location Analysis**: Where jobs are concentrated
- **Timeline**: Job posting trends over time
- **Skill Co-occurrence**: Which skills often appear together
- **Company Insights**: Top hiring companies
- **Interactive Table**: Searchable job listings with all details

## Quick Start

### Prerequisites
- Docker Desktop (with at least 8GB RAM allocated)
- Git

### Installation

1. **Clone and navigate to the repository**
   ```bash
   git clone https://github.com/AsPrabhat/Real-time-Job-Market-Intelligence-Platform.git
   cd Real-time-Job-Market-Intelligence-Platform
   ```

2. **Start all services**
   ```bash
   docker-compose up --build
   ```
   This will start Kafka, Spark, Delta Lake, Dashboard, and other services.

3. **Access the application**
   - **Dashboard**: http://localhost:8050
   - **Spark UI**: http://localhost:8080

### Optional: Email Alerts

To enable email alerts for job matches:
```bash
cp .env.example .env
# Edit .env with your Gmail credentials (use App Password)
```

Edit `alerts.json` to customize your alert preferences.

### Stopping the Application

```bash
docker-compose down        # Stop services
docker-compose down -v     # Stop and remove all data
```

## Project Structure

```
CareerRadar/
├── sample_data/               # Sample job data generator
├── scraper/                   # Kafka producer service
│   ├── Dockerfile
│   ├── scrape.py
│   └── requirements.txt
├── spark_processor/           # Spark streaming processor
│   ├── Dockerfile
│   ├── process.py
│   └── requirements.txt
├── dashboard/                 # Plotly Dash web app
│   ├── Dockerfile
│   ├── app.py
│   ├── assets/
│   └── requirements.txt
├── alerter/                   # Email notification service
│   ├── Dockerfile
│   ├── alerter.py
│   └── requirements.txt
├── docker-compose.yml         # Service orchestration
├── alerts.json                # Alert configuration
├── .env.example               # Environment template
└── README.md                  # This file
```

## How It Works

```
Sample Data → Kafka → Spark Processor → Delta Lake → Dashboard
                                              ↓
                                          Alerter
```

1. **Data Ingestion**: Sample job data is published to Kafka topic
2. **Stream Processing**: Spark reads from Kafka, extracts skills, classifies seniority
3. **Storage**: Enriched data written to Delta Lake (supports time travel!)
4. **Analytics**: Dashboard queries Delta Lake and visualizes insights
5. **Alerts**: Periodic checks for matching jobs and sends email notifications

## Sample Data

Includes 50 realistic job postings from Indian tech companies:
- TCS, Infosys, Wipro
- Amazon India, Microsoft India
- Accenture, Capgemini, and more

**Locations**: Bangalore, Hyderabad, Mumbai, Pune, Delhi/Noida  
**Roles**: Software Engineer, Data Engineer, ML Engineer, Backend Developer, Full Stack

## Future Improvements

- [ ] Integrate real job board RSS feeds
- [ ] Add web scraping for Indeed/LinkedIn
- [ ] Salary range analysis
- [ ] Machine learning-based recommendations
- [ ] Slack integration for alerts

## Troubleshooting

**Dashboard shows no data?**
- Wait 1-2 minutes for data to flow through the pipeline
- Check if all services are running: `docker-compose ps`

**Services not starting?**
- Ensure Docker has at least 8GB RAM allocated
- Try: `docker-compose down -v` then `docker-compose up --build`

**Port already in use?**
- Stop conflicting services or change ports in docker-compose.yml

## Learning Outcomes

This project helped me learn:
- ✅ Real-time data streaming with Apache Kafka
- ✅ Distributed processing with Apache Spark
- ✅ ACID transactions with Delta Lake
- ✅ Building interactive dashboards with Plotly Dash
- ✅ Docker containerization and orchestration
- ✅ Data engineering best practices

## Author

**Prabhat Vishal Pensalwar**  
B.Tech Student | Data Engineering Enthusiast

- GitHub: [@AsPrabhat](https://github.com/AsPrabhat)
- LinkedIn: [Prabhat Pensalwar](https://www.linkedin.com/in/prabhat-pensalwar-2ab7a5330/)
- Email: prabhatworkspace@gmail.com

---

⭐ If you found this project helpful, consider giving it a star!
