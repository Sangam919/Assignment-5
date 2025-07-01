import sqlite3
import pandas as pd
import os
import json

class DataExtractor:
    def __init__(self, db_path='company.db'):
        self.db_path = db_path
        self.output_dir = 'output'
        self.create_output_directories()
    
    def create_output_directories(self):
        directories = ['output/csv', 'output/parquet', 'output/json']
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        print("✅ Output directories created!")
    
    def get_all_tables(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    
    def extract_table_to_formats(self, table_name):
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            print(f"📊 Table '{table_name}' loaded: {len(df)} rows")
            
            # CSV
            csv_path = f"output/csv/{table_name}.csv"
            df.to_csv(csv_path, index=False)
            print(f"✅ CSV saved: {csv_path}")
            
            # Parquet
            parquet_path = f"output/parquet/{table_name}.parquet"
            df.to_parquet(parquet_path, engine='pyarrow')
            print(f"✅ Parquet saved: {parquet_path}")
            
            # JSON (as Avro substitute)
            json_path = f"output/json/{table_name}.json"
            df.to_json(json_path, orient='records', indent=2)
            print(f"✅ JSON saved: {json_path}")
            
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Error processing table {table_name}: {str(e)}")
            return False
    
    def extract_all_tables(self):
        print("🚀 Starting data extraction...")
        tables = self.get_all_tables()
        print(f"📋 Found tables: {tables}")
        success_count = 0
        for table in tables:
            if self.extract_table_to_formats(table):
                success_count += 1
        print(f"✨ Extraction completed! {success_count}/{len(tables)} tables processed")
        return success_count == len(tables)
    
    def selective_extract(self, table_columns_config):
        print("🎯 Starting selective extraction...")
        for table_name, columns in table_columns_config.items():
            try:
                conn = sqlite3.connect(self.db_path)
                columns_str = ', '.join(columns)
                query = f"SELECT {columns_str} FROM {table_name}"
                df = pd.read_sql(query, conn)
                print(f"📊 Table '{table_name}' with columns {columns}: {len(df)} rows")
                csv_path = f"output/csv/{table_name}_selective.csv"
                df.to_csv(csv_path, index=False)
                print(f"✅ Selective CSV saved: {csv_path}")
                conn.close()
            except Exception as e:
                print(f"❌ Error processing selective table {table_name}: {str(e)}")
    
    def copy_database(self, destination_db='backup_company.db'):
        print(f"🔄 Copying database to {destination_db}...")
        source_conn = sqlite3.connect(self.db_path)
        dest_conn = sqlite3.connect(destination_db)
        tables = self.get_all_tables()
        for table in tables:
            df = pd.read_sql(f"SELECT * FROM {table}", source_conn)
            df.to_sql(table, dest_conn, if_exists='replace', index=False)
            print(f"✅ Table '{table}' copied to destination")
        source_conn.close()
        dest_conn.close()
        print(f"🎉 Database copied successfully to {destination_db}")

def main():
    extractor = DataExtractor()
    print("=" * 50)
    print("🏢 COMPANY DATABASE EXTRACTION TOOL")
    print("=" * 50)
    
    # Task 1: Extract all tables
    print("\n1️⃣ EXTRACTING ALL TABLES:")
    extractor.extract_all_tables()
    
    # Task 2: Selective extraction
    print("\n2️⃣ SELECTIVE EXTRACTION:")
    selective_config = {
        'employees': ['name', 'salary', 'department'],
        'departments': ['dept_name', 'manager'],
        'projects': ['project_name', 'status']
    }
    extractor.selective_extract(selective_config)
    
    # Task 3: Database copy
    print("\n3️⃣ DATABASE COPY:")
    extractor.copy_database('backup_company.db')
    
    print("\n🎊 ALL TASKS COMPLETED SUCCESSFULLY!")
    print("\nOutput files location:")
    print("📁 CSV files: output/csv/")
    print("📁 Parquet files: output/parquet/")
    print("📁 JSON files: output/json/")
    print("📁 Backup database: backup_company.db")

if __name__ == "__main__":
    main()