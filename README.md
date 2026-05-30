# Real-Time Stock Market Analysis

## Background

This project is a real-time data engineering pipeline for collecting, streaming, processing, storing, and reporting stock market data.

The goal is to simulate a production-style data platform that extracts stock data from an external API, streams the data through Apache Kafka, processes it with Apache Spark, stores the processed results in PostgreSQL, and makes the data available for analytics and reporting through tools such as pgAdmin and Power BI.

The project is containerized with Docker Compose so that the full data pipeline can run consistently across different environments.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Data Pipeline Flow](#data-pipeline-flow)
5. [Project Structure](#project-structure)
6. [Environment Variables](#environment-variables)
7. [Setup and Installation](#setup-and-installation)
8. [Docker Service Setup](#docker-service-setup)
9. [Kafka Streaming](#kafka-streaming)
10. [Spark Processing](#spark-processing)
11. [PostgreSQL Storage](#postgresql-storage)
12. [Reporting and Analytics](#reporting-and-analytics)
13. [Monitoring and Logging](#monitoring-and-logging)
14. [Troubleshooting](#troubleshooting)
15. [Best Practices](#best-practices)
16. [Key Takeaways](#key-takeaways)
17. [Resources](#resources)

---

## Project Overview

This project implements a real-time stock market data pipeline using modern data engineering tools.

The pipeline extracts stock market data from an external API, produces the data as JSON events into Kafka topics, consumes the events using Spark, and writes the processed output into a PostgreSQL database for analytics.

### Key Features

- Real-time stock data extraction from an external API
- Modular Python code separated into reusable files
- Kafka producer for streaming stock data
- Kafka topic/message inspection using Kafka UI
- Apache Spark service for distributed data processing
- PostgreSQL database for structured storage
- pgAdmin for database management
- Power BI connection for reporting and visualization
- Docker Compose setup for running multiple services
- Environment variable management with `.env`
- Version control using Git and GitHub

---

## Architecture

### Data Pipeline Architecture


### Architecture Flow

1. **Data Ingestion Layer**
   - A Python producer connects to an external stock market API.
   - The API response is extracted and transformed into structured JSON events.
   - The producer sends the events into Kafka topics.

2. **Streaming Layer**
   - Apache Kafka acts as the message broker.
   - Kafka topics store the real-time stock events.
   - Kafka UI is used to inspect topics and messages.

3. **Processing Layer**
   - Apache Spark consumes stock events from Kafka.
   - Spark processes, cleans, and prepares the data for storage.

4. **Storage Layer**
   - PostgreSQL stores the processed stock market data.
   - The data can be queried for analytics and reporting.

5. **Visualization Layer**
   - pgAdmin is used to manage and inspect the PostgreSQL database.
   - Power BI can connect to PostgreSQL for dashboards and reports.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python | API connection, data extraction, and producer logic |
| Alpha Vantage / RapidAPI | External stock market data source |
| Apache Kafka | Real-time message streaming |
| Kafka UI | Kafka topic and message inspection |
| Apache Spark | Stream processing and transformation |
| PostgreSQL | Data storage |
| pgAdmin | PostgreSQL database administration |
| Power BI | Reporting and dashboarding |
| Docker | Containerization |
| Docker Compose | Multi-service orchestration |
| Git & GitHub | Version control and collaboration |

---

## Data Pipeline Flow

```text
API ➜ Kafka ➜ Spark ➜ PostgreSQL ➜ pgAdmin / Power BI
```

### Component Responsibilities

- **API Producer**
  - Connects to the external API.
  - Retrieves stock market data.
  - Extracts required fields such as date, symbol, open, low, high, and close.
  - Produces JSON events into Kafka.

- **Kafka**
  - Receives stock market events from the producer.
  - Stores events in Kafka topics.
  - Makes data available to downstream consumers.

- **Spark**
  - Consumes data from Kafka.
  - Processes and transforms streaming data.
  - Writes the final results into PostgreSQL.

- **PostgreSQL**
  - Stores structured stock market records.
  - Serves as the source for analytics and reporting.

- **pgAdmin**
  - Provides a graphical interface for managing PostgreSQL.

- **Power BI**
  - Connects to PostgreSQL for dashboard creation and business insights.

---

## Project Structure

```text
Real-Time-Stock-Market-Analysis/
│
├── producer/
│   ├── config.py              # API configuration, environment variables, and headers
│   ├── extract.py             # API connection and JSON extraction logic
│   └── main.py                # Main producer script
│
├── venv/                      # Virtual environment
├── .env                       # Environment variables and API keys
├── .gitignore                 # Files and folders ignored by Git
├── compose.yml                # Docker Compose services
└── README.md                  # Project documentation
```

---

## Environment Variables

Create a `.env` file in the root directory of the project.

Example:

```env
API_KEY=your_api_key_here
```

The API key is loaded into the project using `python-dotenv`.

The `config.py` file handles:

```python
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("API_KEY")
```

---

## Setup and Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/real-time-stock-market-analysis.git
cd real-time-stock-market-analysis
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
```

### 3. Activate the Virtual Environment

For Windows:

```bash
venv\Scripts\activate
```

For macOS/Linux:

```bash
source venv/bin/activate
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

If `requirements.txt` does not exist yet, create one with dependencies such as:

```text
requests
python-dotenv
kafka-python
pyspark
psycopg2-binary
```

### 5. Configure Environment Variables

Create a `.env` file and add your API key:

```env
API_KEY=your_api_key_here
```

---

## Docker Service Setup

The project uses Docker Compose to run the required services.

Expected services include:

- Kafka
- Kafka UI
- Spark Master
- Spark Worker
- PostgreSQL
- pgAdmin

### Start Docker Services

```bash
docker compose up -d
```

### Stop Docker Services

```bash
docker compose down
```

### View Running Containers

```bash
docker ps
```

---

## Kafka Streaming

Kafka is used as the streaming layer between the API producer and Spark processing service.

### Kafka Responsibilities

- Receive JSON events from the Python producer
- Store events in Kafka topics
- Allow consumers such as Spark to read the events
- Provide a reliable buffer between ingestion and processing

### Kafka UI

Kafka UI can be used to:

- Inspect Kafka topics
- View produced messages
- Monitor topic activity
- Debug streaming issues

---

## Spark Processing

Apache Spark is used to process streaming data from Kafka.

### Spark Services

The Docker Compose setup includes:

- `spark-master`
- `spark-worker`

### Spark Master

The Spark master manages the Spark cluster and coordinates workloads.

### Spark Worker

The Spark worker connects to the Spark master and executes processing tasks.

The worker service is configured to communicate with the master service through the Docker network.

---

## PostgreSQL Storage

PostgreSQL is used as the final storage layer for processed stock market data.

### Example Stock Data Fields

The processed records may include:

| Column | Description |
|---|---|
| date | Stock trading date |
| symbol | Stock symbol |
| open | Opening price |
| low | Lowest price |
| high | Highest price |
| close | Closing price |

### Example Table

```sql
CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY,
    date DATE,
    symbol VARCHAR(20),
    open NUMERIC,
    low NUMERIC,
    high NUMERIC,
    close NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Reporting and Analytics

The stored data can be used for reporting and visualization.

### pgAdmin

pgAdmin is used to:

- View PostgreSQL databases
- Inspect tables
- Run SQL queries
- Validate inserted stock records

### Power BI

Power BI can connect directly to PostgreSQL to create dashboards such as:

- Stock price trends
- Daily open and close comparisons
- High and low price analysis
- Historical stock performance reports

---

## Monitoring and Logging

### Application Logging

The project uses Python logging to track application activity.

Example logging setup:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
```

### What to Monitor

- API connection status
- API response errors
- Kafka producer activity
- Kafka topic messages
- Spark job execution
- PostgreSQL write operations
- Docker container health

---

## Troubleshooting

### API Key Not Found

Check that your `.env` file exists and contains:

```env
API_KEY=your_api_key_here
```

Also confirm that `load_dotenv()` is called before accessing the environment variable.

### Kafka Container Not Running

Run:

```bash
docker ps
```

If Kafka is not running, restart the services:

```bash
docker compose up -d
```

### Kafka Producer Cannot Connect

Check the Kafka broker address.

For local Python clients, the broker may be:

```text
localhost:9094
```

For Docker containers on the same Docker network, the broker may be:

```text
kafka:9092
```

### Spark Worker Cannot Connect to Master

Confirm that both services are on the same Docker network and that the Spark master hostname matches the Docker Compose service name.

Example:

```text
spark://spark-master:7077
```

### PostgreSQL Connection Error

Check:

- Database host
- Database port
- Username
- Password
- Database name
- Docker network connection

---

## Best Practices

1. **Keep API keys private**
   - Store secrets in `.env`.
   - Do not commit `.env` to GitHub.

2. **Use modular code**
   - Keep API configuration in `config.py`.
   - Keep extraction logic in `extract.py`.
   - Keep execution logic in `main.py`.

3. **Use Docker Compose**
   - Manage all services from one configuration file.
   - Make the project easier to run on different machines.

4. **Validate data before storing**
   - Check that required fields exist.
   - Handle missing or invalid API responses.

5. **Monitor Kafka topics**
   - Use Kafka UI to confirm that messages are being produced.

6. **Document setup clearly**
   - Keep README instructions updated as the project changes.

---

## Key Takeaways

- Built a real-time data pipeline using API, Kafka, Spark, and PostgreSQL.
- Modularized Python code for better maintainability and reusability.
- Used Docker Compose to manage multiple services.
- Streamed API data into Kafka topics.
- Prepared the pipeline for analytics through PostgreSQL, pgAdmin, and Power BI.
- Practiced real-world data engineering concepts such as ingestion, streaming, processing, storage, and reporting.


---

## Resources

- Apache Kafka Documentation
- Apache Spark Documentation
- PostgreSQL Documentation
- Docker Documentation
- Docker Compose Documentation
- pgAdmin Documentation
- Power BI PostgreSQL Connector Documentation
- Alpha Vantage / RapidAPI Documentation

---

## Author

**Iyeme Salubi**  

---

## License

This project is for educational and portfolio purposes.