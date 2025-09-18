# Institutional Flow Tracker

Early prototype repo for an AI-driven institutional flow tracker.

## Features
- Ingests institutional filings (13F), gov awards (USAspending, SBIR, NIH), patents (PatentsView), and ETF thematic trades (ARK).
- Stores data in SQLite with SQLAlchemy ORM.
- Scores companies by unusual activity across multiple pillars.
- Uses LLM to generate concise memos explaining signals.
- Flask dashboard to review entities and AI memos.

## Setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage
```bash
python etl/etl_daily.py
python dashboard/app.py
```