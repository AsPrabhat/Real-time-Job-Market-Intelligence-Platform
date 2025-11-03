# CareerRadar - Setup Guide

Simple guide to get the project running on your machine.

## What You Need

1. **Docker Desktop** - [Download here](https://www.docker.com/products/docker-desktop)
   - Make sure to allocate at least 8GB RAM to Docker
   - Windows/Mac/Linux all supported

2. **Git** - To clone the repository

## Installation (5 minutes)

### Step 1: Clone the Project
```bash
git clone https://github.com/AsPrabhat/Real-time-Job-Market-Intelligence-Platform.git
cd Real-time-Job-Market-Intelligence-Platform
```

### Step 2: Start Everything
```bash
docker-compose up --build
```

Wait 2-3 minutes for all services to start. You'll see logs from Kafka, Spark, Dashboard, etc.

### Step 3: Open the Dashboard
Open your browser and go to: **http://localhost:8050**

That's it! You should see job market data and visualizations.

## What's Running?

When you run `docker-compose up`, it starts:
- **Kafka** - Message broker for streaming data
- **Spark** - Processes and enriches job data
- **Delta Lake** - Stores the processed data
- **Dashboard** - Web interface to view insights
- **Alerter** - Sends email notifications (if configured)

## Optional: Email Alerts

Want to receive email alerts for matching jobs?

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Get a Gmail App Password:
   - Go to https://myaccount.google.com/apppasswords
   - Generate a password for "Mail"
   - Copy the 16-character code

3. Edit `.env` file:
   ```
   EMAIL_ADDRESS=your_email@gmail.com
   EMAIL_PASSWORD=your_16_char_app_password
   ```

4. Edit `alerts.json` to set your preferences:
   ```json
   {
     "email": "your_email@gmail.com",
     "keywords": ["Python", "Data Engineer"],
     "locations": ["Bangalore", "Hyderabad"]
   }
   ```

5. Restart the alerter:
   ```bash
   docker-compose restart alerter
   ```

## Stopping the Project

```bash
# Stop all services
docker-compose down

# Stop and delete all data (fresh start)
docker-compose down -v
```

## Common Issues

**Dashboard not loading?**
- Wait 2-3 minutes for all services to fully start
- Check if Docker has enough RAM (8GB minimum)

**Port already in use?**
```bash
# Find what's using port 8050
netstat -ano | findstr :8050

# Kill that process or change the port in docker-compose.yml
```

**Services keep restarting?**
```bash
# Check logs
docker-compose logs [service-name]

# Try a fresh start
docker-compose down -v
docker-compose up --build
```

## Project Structure

```
CareerRadar/
├── scraper/           # Loads job data into Kafka
├── spark_processor/   # Processes data with Spark
├── dashboard/         # Web dashboard (Plotly Dash)
├── alerter/           # Email notification service
├── sample_data/       # Generates sample job postings
├── docker-compose.yml # Orchestrates all services
├── alerts.json        # Alert configuration
└── README.md          # Main documentation
```

## Useful Commands

```bash
# View all running containers
docker-compose ps

# View logs from all services
docker-compose logs -f

# View logs from specific service
docker-compose logs -f dashboard

# Restart a service
docker-compose restart dashboard

# Stop everything
docker-compose down

# Start everything again
docker-compose up
```

## Next Steps

- Explore the dashboard at http://localhost:8050
- Check the Spark UI at http://localhost:8080
- Modify `alerts.json` to customize your job alerts
- Look at the code in each service directory
- Try generating new sample data in `sample_data/`

## Need Help?

- Check the [README](README.md) for more details
- Open an issue on GitHub
- Email: prabhatworkspace@gmail.com
