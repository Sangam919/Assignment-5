# Week 5 Assignment - Data Engineering

## Tasks Completed
1. Extracted data from SQLite database to CSV, Parquet, and JSON.
2. Implemented scheduled (every 5 minutes) and event-based (database change) triggers.
3. Copied entire database to `backup_company.db`.
4. Performed selective table/column extraction.

## How to Run
1. Install dependencies:
   ```powershell
   pip install pandas pyarrow schedule watchdog