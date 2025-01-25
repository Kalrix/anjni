import os
import psycopg2
import pandas as pd
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# ✅ Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ✅ CSV File Path
CSV_FILE_PATH = "data/Index-list.csv"

# ✅ Database table names
INDEX_LIST_TABLE = "index_list"
SEARCH_TABLE = "search_table"

# ✅ Initialize FastAPI
app = FastAPI()

# ✅ Function to establish database connection
def get_db_connection():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

# ✅ API Endpoint: Create `index_list` Table
@app.post("/api/index-list/create-table/")
def create_index_list_table():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {INDEX_LIST_TABLE} (
        id SERIAL PRIMARY KEY,
        index_name TEXT NOT NULL,
        attribute TEXT NOT NULL,
        name TEXT NOT NULL,
        trading_symbol TEXT NOT NULL,
        weightage NUMERIC NOT NULL,
        search_attribute TEXT,
        search_enum INT,
        search_sem_smst_security_id INT
    );
    """

    check_constraint_query = f"""
    SELECT COUNT(*) FROM information_schema.table_constraints 
    WHERE table_name = '{INDEX_LIST_TABLE}' 
    AND constraint_name = 'unique_trading_index';
    """

    add_constraint_query = f"""
    ALTER TABLE {INDEX_LIST_TABLE} 
    ADD CONSTRAINT unique_trading_index UNIQUE (trading_symbol, index_name);
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # ✅ Create Table if not exists
                cur.execute(create_table_query)

                # ✅ Check if the constraint exists
                cur.execute(check_constraint_query)
                constraint_exists = cur.fetchone()[0]

                if constraint_exists == 0:
                    # ✅ Add constraint only if it doesn’t exist
                    cur.execute(add_constraint_query)

                conn.commit()
        print(f"✅ Table '{INDEX_LIST_TABLE}' created and unique constraint ensured.")
    except Exception as e:
        print(f"⚠️ Error in table creation: {e}")

# ✅ API Endpoint: Load CSV Data
@app.post("/api/index-list/load-data/")
def load_csv_to_table():
    try:
        data = pd.read_csv(CSV_FILE_PATH)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for _, row in data.iterrows():
                    insert_query = f"""
                    INSERT INTO {INDEX_LIST_TABLE} (index_name, attribute, name, trading_symbol, weightage)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (trading_symbol, index_name) DO UPDATE 
                    SET weightage = EXCLUDED.weightage;  -- ✅ Updates weightage if conflict occurs
                    """
                    cur.execute(insert_query, (row["Index"], row["attribute"], row["Name"], row["trading_symbol"], row["Weightage (%)"]))
                conn.commit()
        return {"message": f"✅ Data from {CSV_FILE_PATH} loaded successfully into '{INDEX_LIST_TABLE}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"⚠️ Error in loading CSV data: {e}")

# ✅ API Endpoint: Update `index_list` with `search_table`
@app.post("/api/index-list/update/")
def update_index_list_table():
    update_query = f"""
    UPDATE {INDEX_LIST_TABLE} AS il
    SET
        search_attribute = st.attribute,
        search_enum = st.enum,
        search_sem_smst_security_id = st.sem_smst_security_id
    FROM {SEARCH_TABLE} AS st
    WHERE il.trading_symbol = st.trading_symbol AND il.attribute = st.attribute;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(update_query)
                conn.commit()
        return {"message": f"✅ '{INDEX_LIST_TABLE}' updated with data from '{SEARCH_TABLE}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"⚠️ Error in updating index list: {e}")

# ✅ API Endpoint: Run All Steps (Table Creation + Load Data + Update)
@app.post("/api/index-list/run-all/")
def run_all_tasks():
    try:
        create_index_list_table()
        load_csv_to_table()
        update_index_list_table()
        return {"message": "✅ All tasks executed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"⚠️ Error executing all tasks: {e}")

