import pandas as pd
import psycopg2
import os
import requests
import csv
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

# ✅ Load environment variables from .env
load_dotenv()

# ✅ Database connection details from .env
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

# ✅ CSV Source URL
CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
CSV_FILE_PATH = "api/app/temp_scrip_master.csv"  # ✅ Temporary Storage Path

# ✅ Step 1: Fetch CSV from URL
def fetch_csv():
    try:
        print(f"🔄 Fetching CSV from {CSV_URL} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
        response = requests.get(CSV_URL, timeout=30)  # ✅ Timeout added for stability
        if response.status_code == 200:
            with open(CSV_FILE_PATH, "wb") as f:
                f.write(response.content)
            print("✅ CSV file downloaded successfully!")
        else:
            print(f"❌ Failed to download CSV. HTTP Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error fetching CSV: {e}")
        return False
    return True

# ✅ Step 2: Load and Process CSV Data
def load_csv():
    try:
        print("🔍 Loading and processing CSV data...")
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8', low_memory=False)

        # Convert all column names to lowercase and strip spaces
        df.columns = df.columns.str.strip().str.lower()

        # Ensure required columns exist
        required_columns = [
            "sem_exm_exch_id", "sem_segment", "sem_smst_security_id", "sem_instrument_name",
            "sem_expiry_code", "sem_trading_symbol", "sem_lot_units", "sem_custom_symbol",
            "sem_expiry_date", "sem_strike_price", "sem_option_type", "sem_tick_size",
            "sem_expiry_flag", "sem_exch_instrument_type", "sem_series", "sm_symbol_name"
        ]
        for col in required_columns:
            if col not in df.columns:
                raise KeyError(f"❌ Required column '{col}' not found in CSV format.")

        # ✅ Clean & Format Data
        valid_options = {"CE", "PE"}  # Allowed values
        df["sem_option_type"] = df["sem_option_type"].astype(str).str.strip().str.upper()
        df["sem_option_type"] = df["sem_option_type"].apply(lambda x: x if x in valid_options else None)

        df["sem_expiry_date"] = pd.to_datetime(df["sem_expiry_date"], errors='coerce').dt.date
        df["sem_expiry_date"] = df["sem_expiry_date"].replace({pd.NaT: None})

        df["sem_lot_units"] = pd.to_numeric(df["sem_lot_units"], errors='coerce').fillna(0).astype(int)
        df["sem_expiry_code"] = pd.to_numeric(df["sem_expiry_code"], errors='coerce').fillna(0).astype(int)
        df["sem_strike_price"] = pd.to_numeric(df["sem_strike_price"], errors='coerce').fillna(0)

        print(f"✅ Loaded {len(df)} rows and applied final fixes.")
        return df
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return None

# ✅ Step 3: Connect to Database
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

# ✅ Step 4: Bulk Insert Data Using `COPY` into `scrip_master`
def insert_data(df):
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor()

    try:
        # ✅ Step 1: Truncate `scrip_master` to prevent duplicates
        cursor.execute("TRUNCATE TABLE scrip_master RESTART IDENTITY;")
        conn.commit()
        print("✅ Old data cleared from scrip_master.")

        # ✅ Step 2: Save Data to Temporary CSV File
        df = df[[
            "sem_exm_exch_id", "sem_segment", "sem_smst_security_id", "sem_instrument_name",
            "sem_expiry_code", "sem_trading_symbol", "sem_lot_units", "sem_custom_symbol",
            "sem_expiry_date", "sem_strike_price", "sem_option_type", "sem_tick_size",
            "sem_expiry_flag", "sem_exch_instrument_type", "sem_series", "sm_symbol_name"
        ]]
        df.to_csv(CSV_FILE_PATH, index=False, header=False, sep="|", quoting=csv.QUOTE_NONE, na_rep="NULL")

        # ✅ Step 3: Use COPY for Fast Bulk Insert
        copy_sql = f"""
        COPY scrip_master (
            sem_exm_exch_id, sem_segment, sem_smst_security_id, sem_instrument_name, 
            sem_expiry_code, sem_trading_symbol, sem_lot_units, sem_custom_symbol, 
            sem_expiry_date, sem_strike_price, sem_option_type, sem_tick_size, 
            sem_expiry_flag, sem_exch_instrument_type, sem_series, sm_symbol_name
        )
        FROM STDIN WITH CSV DELIMITER '|' NULL 'NULL';
        """
        with open(CSV_FILE_PATH, "r") as f:
            cursor.copy_expert(copy_sql, f)

        conn.commit()
        print(f"✅ Successfully inserted {len(df)} rows into scrip_master.")

    except Exception as e:
        print(f"❌ Error inserting data using COPY: {e}")
    finally:
        cursor.close()
        conn.close()

# ✅ Step 5: Schedule Automatic Daily Updates at 8:30 AM
def schedule_csv_update():
    if fetch_csv():
        df = load_csv()
        if df is not None:
            insert_data(df)
            print("✅ CSV update completed.")

# ✅ Scheduler for Auto Update at 8:30 AM
scheduler = BackgroundScheduler()
scheduler.add_job(schedule_csv_update, "cron", hour=8, minute=30)  # Runs daily at 08:30 AM
scheduler.start()

# ✅ Step 6: Manually Trigger CSV Fetch & Store
def fetch_and_store_csv():
    schedule_csv_update()

# ✅ Execute if script is run directly
if __name__ == "__main__":
    fetch_and_store_csv()
