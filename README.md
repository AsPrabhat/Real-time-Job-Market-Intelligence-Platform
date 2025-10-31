# CareerRadar - Real-time Job Market Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-black.svg)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-green.svg)](https://delta.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

CareerRadar is a real-time data engineering platform designed to collect, process, and analyze job market trends. The system ingests job postings through a streaming pipeline, performs intelligent skill extraction and classification using Apache Spark, stores the enriched data in Delta Lake, and presents actionable insights through an interactive dashboard.

## Key Features

- **Real-time Data Pipeline**: Kafka-based streaming architecture that continuously ingests job posting data
- **Intelligent Enrichment**: Custom Apache Spark jobs extract technical skills and classify experience levels from job descriptions
- **Delta Lake Storage**: ACID-compliant storage layer that maintains historical job market data with versioning capabilities
- **Interactive Dashboard**: Web-based analytics interface built with Plotly Dash, featuring dynamic filters and automated refresh
- **Personalized Alerts**: Configurable email notification system that monitors for job opportunities matching specific criteria
- **Containerized Deployment**: Docker Compose configuration that handles multi-service orchestration and resource management

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

The dashboard provides several analytical views of the job market data:

- **Top 10 In-Demand Skills**: Bar chart displaying the most frequently requested technical skills
- **Experience Level Distribution**: Pie chart breaking down opportunities by seniority (Internship, Entry-Level, Senior)
- **Jobs by Location**: Geographic distribution across major Indian technology hubs
- **Job Postings Timeline**: Time-series visualization showing posting trends
- **Skill Co-occurrence Matrix**: Heatmap indicating which technical skills commonly appear together in job requirements
- **Company Distribution**: Analysis of which companies are posting most frequently
- **Interactive Data Table**: Searchable and filterable table view with pagination

## Getting Started

### Prerequisites

- Docker Desktop installed and running
- Minimum 8GB RAM allocated to Docker
- Windows, macOS, or Linux operating system
- Gmail account (optional, required only for email alerts)

### Installation

1. Clone the repository
   ```bash
   git clone https://github.com/AsPrabhat/Real-time-Job-Market-Intelligence-Platform.git
   cd Real-time-Job-Market-Intelligence-Platform
   ```

2. Configure environment variables (optional, for email alerts)
   ```bash
   cp .env.example .env
   # Edit .env file with your Gmail credentials
   ```

3. Build and start all services
   ```bash
   docker-compose up --build
   ```

4. Access the dashboard
   - Dashboard: `http://localhost:8050`
   - Spark UI: `http://localhost:8080`
   - Kafka (internal): `kafka:9092`

### Stopping the Services

To stop all running containers:
```bash
docker-compose down
```

To remove all data and start fresh:
```bash
docker-compose down -v
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

## Target Job Roles

The system focuses on tracking software engineering and data-related positions:
- Software Developer / SDE
- AI/ML Engineer
- Data Engineer
- Backend Developer
- Full Stack Developer

### Geographic Focus

The platform currently targets opportunities in major Indian technology hubs:
- Bangalore
- Hyderabad
- Mumbai
- Pune
- Delhi/Noida

## Configuration

### Skills Tracking

The Spark processor identifies and extracts over 25 technical skills from job descriptions:
- **Programming Languages**: Python, C++, Java, SQL
- **Data Engineering Tools**: Apache Spark, Kafka, Delta Lake, PostgreSQL
- **Cloud & AI Technologies**: Azure, OpenAI, RAG, Vector Databases, LLM, NLP
- **Development Tools**: Docker, Git, GitHub, Node.js
- **Specialized Areas**: Distributed Systems, Machine Learning, Generative AI

### Alert Preferences

Edit `alerts.json` to customize job alerts:
```json
{
  "email": "your-email@gmail.com",
  "keywords": ["Python", "Spark", "Azure"],
  "locations": ["Bangalore", "Hyderabad"]
}
```

## Sample Data

This repository includes a generator that creates 50 realistic job postings from prominent Indian technology companies such as:
- Tata Consultancy Services (TCS)
- Infosys
- Wipro
- Amazon India
- Microsoft India
- Accenture
- Capgemini

## Future Enhancements

Planned improvements for future iterations:
- Real RSS feed integration from job boards
- Web scraping capabilities for additional data sources
- Salary range visualization and analysis
- Geographic heatmap for job distribution
- Skills gap analysis based on user profile
- Slack integration for alerts
- Machine learning-based job recommendations
- Extended historical trend analysis

## Contributing

Contributions are welcome. Please submit a pull request or open an issue to discuss proposed changes.

<!-- ## 📝 License

This project is open source and available under the MIT License. -->

## Author

**Prabhat Vishal Pensalwar**
- GitHub: [@AsPrabhat](https://github.com/AsPrabhat)
- Email: prabhatworkspace@gmail.com
- LinkedIn: [Connect with me](https://www.linkedin.com/in/prabhat-pensalwar-2ab7a5330/)

---

If you find this project useful, please consider starring the repository.
