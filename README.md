# 📈 MarketPulse Real-Time Pipeline

A scalable, real-time stock market data pipeline built for
MarketPulse Analytics — a FinTech company providing insights
to institutional investors.

---

## 🏗️ Architecture

Alpha Vantage API
↓
Python Producer
↓
Apache Kafka
↓
Apache Spark
↓
PostgreSQL
↓
Power BI Dashboard
---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Python | Data collection & pipeline logic |
| Apache Kafka | Real-time data streaming |
| Apache Spark | Large-scale data processing |
| PostgreSQL | Data storage & reporting |
| Docker | Containerization |
| Power BI | Dashboards & visualization |

---

## 📁 Project Structure


marketpulse-realtime-pipeline/
├── producers/
│   ├── config.py           # API keys and settings
│   ├── api_client.py       # Connects to Alpha Vantage API
│   └── stock_producer.py   # Streams data to Kafka
├── spark/                  # Spark data processing
├── db/                     # PostgreSQL scripts
├── dashboards/             # Power BI connection files
├── monitoring/             # Pipeline health monitoring
├── tests/                  # Unit tests
├── docs/                   # Documentation
├── docker-compose.yml      # Docker services
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variable template
└── .env                    # API keys (not committed)


---

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- RapidAPI account (Alpha Vantage)

### 1. Clone the repo
```bash
git clone https://github.com/akinzsoft/marketpulse-realtime-pipeline.git
cd marketpulse-realtime-pipeline
```

### 2. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up environment variables
```bash
cp .env.example .env
# Add your API keys to .env
```

### 5. Start Docker services
```bash
sudo docker compose up -d
```

### 6. Verify containers are running
```bash
sudo docker compose ps
```

### 7. Run the stock producer
```bash
python3 -m producers.stock_producer
```

---

## 🐳 Docker Services

| Service | Port | Purpose |
|---|---|---|
| Kafka | 9092 | Message streaming |
| Zookeeper | 2181 | Kafka coordination |
| PostgreSQL | 5433 | Data storage |

---

## 📊 Data Flow

1. **Collect** — Python fetches real-time stock data from Alpha Vantage API
2. **Stream** — Data is published to Kafka topics
3. **Process** — Spark consumes and transforms the data
4. **Store** — Processed data is saved to PostgreSQL
5. **Visualize** — Power BI dashboards display live insights

---

## 📈 Stocks Tracked

| Symbol | Company |
|---|---|
| AAPL | Apple Inc. |
| GOOGL | Alphabet Inc. |
| MSFT | Microsoft Corp. |
| AMZN | Amazon.com Inc. |
| TSLA | Tesla Inc. |

---

## 🔐 Environment Variables

Create a `.env` file in the root directory:



---

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- RapidAPI account (Alpha Vantage)

### 1. Clone the repo
```bash
git clone https://github.com/akinzsoft/marketpulse-realtime-pipeline.git
cd marketpulse-realtime-pipeline
```

### 2. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up environment variables
```bash
cp .env.example .env
# Add your API keys to .env
```

### 5. Start Docker services
```bash
sudo docker compose up -d
```

### 6. Verify containers are running
```bash
sudo docker compose ps
```

### 7. Run the stock producer
```bash
python3 -m producers.stock_producer
```

---

## 🐳 Docker Services

| Service | Port | Purpose |
|---|---|---|
| Kafka | 9092 | Message streaming |
| Zookeeper | 2181 | Kafka coordination |
| PostgreSQL | 5433 | Data storage |

---

## 📊 Data Flow

1. **Collect** — Python fetches real-time stock data from Alpha Vantage API
2. **Stream** — Data is published to Kafka topics
3. **Process** — Spark consumes and transforms the data
4. **Store** — Processed data is saved to PostgreSQL
5. **Visualize** — Power BI dashboards display live insights

---

## 📈 Stocks Tracked

| Symbol | Company |
|---|---|
| AAPL | Apple Inc. |
| GOOGL | Alphabet Inc. |
| MSFT | Microsoft Corp. |
| AMZN | Amazon.com Inc. |
| TSLA | Tesla Inc. |

---

## 🔐 Environment Variables

Create a `.env` file in the root directory:

RAPIDAPI_KEY=your_rapidapi_key_here
RAPIDAPI_HOST=alpha-vantage.p.rapidapi.com


---

## 📅 Development Progress

| Week | Day | Task | Status |
|---|---|---|---|
| Week 2 | Day 1 | Environment Setup & API Registration | ✅ Done |
| Week 2 | Day 2 | Git & GitHub + API Module | ✅ Done |
| Week 2 | Day 3 | Docker Setup (Kafka, Zookeeper, PostgreSQL) | ✅ Done |
| Week 2 | Day 4 | README Documentation | ✅ Done |
| Week 2 | Day 5 | Kafka Setup & Data Streaming | 🚧 In Progress |
| Week 3 | Day 1 | Spark Consumer & Data Processing | ⏳ Pending |
| Week 3 | Day 2 | Database Integration with PostgreSQL | ⏳ Pending |
| Week 3 | Day 3 | Power BI Dashboard | ⏳ Pending |
| Week 3 | Day 4 | Final Documentation | ⏳ Pending |

---

## 👨‍💻 Author

**Ayotunde**
GitHub: [@akinzsoft](https://github.com/akinzsoft)

---

## 📌 Project Status

🚧 In Progress — Week 2, Day 4
