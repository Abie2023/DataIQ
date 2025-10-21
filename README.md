# DataIQ – Intelligent Data Quality & Insights Suite

DataIQ is a local, production-style toolkit for Oracle XE that automates data profiling, cleaning, anomaly detection, and reporting. It also includes a Streamlit dashboard for at-a-glance insights. Everything runs locally—no Oracle Cloud or paid tools.

## Features

- Oracle XE connector using SQLAlchemy + python-oracledb (thin mode - no Instant Client required!)
- Data profiling: nulls, duplicates, type mismatches, numeric stats
- Data cleaning: duplicate removal, null handling, string normalization
- Anomaly detection: IsolationForest per numeric column with charts
- Reporting: PDF (fpdf2) and HTML with scores and visuals
- Scheduler: daily/weekly jobs via `schedule`
- Streamlit dashboard: metrics, charts, and on-demand runs

## Tech Stack

- Python, pandas, numpy
- SQLAlchemy, python-oracledb (thin mode)
- scikit-learn, matplotlib, seaborn
- fpdf2, PyYAML, schedule
- Streamlit

## Prerequisites

- Python 3.9+
- Oracle XE installed locally
- **No Oracle Instant Client needed** - uses python-oracledb in thin mode
  - Optional: For thick mode with Instant Client, see config options below

## Install

```
python -m venv .venv
```

```
.\.venv\Scripts\Activate.ps1
```

```
pip install -r requirements.txt
```

## Configure

Edit `config/db_config.yaml` or create `config/db_config.local.yaml` to override credentials:

```yaml
oracle:
  host: 127.0.0.1
  port: 1521
  service_name: XEPDB1
  username: system
  password: change_me
```

## Usage (CLI)

Run the orchestrator in different modes:

```powershell
python main.py --mode profile --table YOUR_TABLE --limit 1000
python main.py --mode clean --table YOUR_TABLE --limit 1000
python main.py --mode detect --table YOUR_TABLE --limit 1000
python main.py --mode report --table YOUR_TABLE --limit 1000
python main.py --mode all --table YOUR_TABLE --limit 1000
```

## Dashboard

```powershell
streamlit run dashboard/app.py
```

The dashboard supports **two data sources**:

### 1. Oracle Database Mode (Default)

- Connects to your local Oracle XE database
- Loads recent profiles and cleaned datasets
- Shows key metrics and anomaly charts
- Requires Oracle XE running

### 2. CSV Upload Mode (Offline Mode)

- **No Oracle connection required** - run DataIQ completely offline!
- Upload any CSV file from your computer
- Run profiling, cleaning, and anomaly detection on uploaded data
- Download cleaned data and profiling results
- Perfect for quick analysis without database setup

**Key Features:**

- 📊 Real-time data quality metrics (nulls, duplicates, missing values)
- 🔍 AI-powered anomaly detection with visual charts
- 🧹 Data cleaning with multiple strategies
- 💾 Download cleaned data and profiles as CSV
- 🔄 Seamless switching between Oracle and CSV modes

**Sample CSV:** Use `sample_data/test_data.csv` to test CSV upload mode (30 rows of employee data with intentional quality issues)
