# NSE Data Pipeline & Query Helper

This repository contains two main components for fetching, storing, and analyzing National Stock Exchange (NSE) market data:

1. **`data_pipeline`**: An automated pipeline that fetches daily equity market candles from SmartAPI, processes them, and stores them in a PostgreSQL database.
2. **`query_helper`**: An AI-powered financial agent (using Groq and MCP) that queries and analyzes the stored market data.

---

## 🛠 Prerequisites & Tech Stack

- **Python**: `>=3.14`
- **Package Manager**: [uv](https://github.com/astral-sh/uv) (recommended)
- **Database**: PostgreSQL
- **LLM Provider**: Groq API
- **Data Provider**: SmartAPI (Angel One)

## 🚀 Environment Setup

Since this project uses `uv` for dependency management:

1. Install `uv` if you haven't already.
2. Install the project dependencies from the root directory:
   ```bash
   uv sync
   ```
   Or install via `pip`:
   ```bash
   pip install -e .
   ```

### Connecting to PostgreSQL
You'll need a running PostgreSQL database. Create a database (e.g., `nse_db`) and ensure you have user credentials ready.

#### Database Schema
The `data_pipeline` expects a table named `equity_market_candles`. Run the following SQL to set it up:

```sql
CREATE TABLE IF NOT EXISTS equity_market_candles (
    timestamp TIMESTAMP,
    token VARCHAR(50),
    symbol VARCHAR(100),
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume BIGINT,
    UNIQUE(timestamp, token)
);
```
*(Note: The unique constraint on `timestamp` and `token` is required for the pipeline's UPSERT operations to work correctly.)*

---

## 1️⃣ Data Pipeline (`data_pipeline`)

The data pipeline runs daily checks to see if the market was open. If so, it authenticates with SmartAPI, fetches the raw equity candle data, cleans it, and performs an UPSERT into the PostgreSQL database.

### `.env` File (`data_pipeline/.env`)
Create an `.env` file in the `data_pipeline/` directory with the following variables:

```ini
# PostgreSQL Database Credentials
HOST=localhost
PORT=5432
DBNAME=nse_db
DB_USER=your_db_user
DB_PASSWORD=your_db_password

# SmartAPI Credentials
# Add your SmartAPI keys, client codes, or tokens as expected by `authentication.py`
```

### Running the Pipeline
You can run the pipeline directly. It automatically determines if today is a trading day (via `national_holidays.csv`) before proceeding.

```bash
cd data_pipeline
python main.py
```

---

## 2️⃣ Query Helper (`query_helper`)

The query helper is an interactive command-line assistant powered by the `llama-3.3-70b-versatile` model (via Groq API). It leverages the Model Context Protocol (MCP) to seamlessly call Python functions as tools, enabling it to analyze trends, volume, and price action directly from the data stored in your PostgreSQL database.

### `.env` File (`query_helper/.env`)
Create an `.env` file in the `query_helper/` directory.

```ini
# Groq API Key for the LLM
GROQ_API_KEY=gsk_your_groq_api_key_here

# PostgreSQL Database Credentials used by the MCP Server
host=localhost
port=5432
db_name=nse_db
db_user=your_db_user
DB_PASSWORD=your_db_password
```

### Running the Agent
Start the interactive Q&A agent from your terminal:

```bash
cd query_helper
python agent.py
```

You can ask the agent things like:
- *"What is the recent price action for RELIANCE?"*
- *"Can you detect any trends in the NIFTY50 volume?"*
- *"Summarize the last 5 days of HDFCBANK."*

Type `exit` or `quit` to end the session.
