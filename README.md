Week 5 Assignment - Data Engineering

Tasks Completed
1. Extracted data from SQLite database (`company.db`) to CSV, Parquet, and JSON formats.
2. Implemented automation with:
   - Scheduled trigger (every 5 minutes using `schedule` library).
   - Event-based trigger (detects changes to `company.db` using `watchdog`).
3. Copied entire database to `backup_company.db`.
4. Performed selective table/column extraction based on a configuration.

How to Run
1. Install dependencies:
   powershell
  pip install pandas pyarrow schedule watchdog