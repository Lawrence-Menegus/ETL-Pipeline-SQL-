# Lawrence Menegus 
# Creation of a ETL Pipeline to SQL Server 


import requests
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from sqlalchemy.types import NVARCHAR

# Logging setup
logging.basicConfig(filename="etl.log", level=logging.INFO)

# API credentials
API_KEY = "b88087f363mshfcfd619b5338967p1ed9d3jsn151cd79da005"
API_HOST = "chatgpt-42.p.rapidapi.com"
BASE_URL = "https://chatgpt-42.p.rapidapi.com/o3mini"

# Database Connection String (SQLAlchemy)
DB_CONNECTION_STRING = "mssql+pyodbc://LARRYLAPTOP\\SQLEXPRESS/gpt_db?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

# Extract Data from API
def extract_data(user_message="hi"):
    url = BASE_URL
    headers = {
        "Content-Type": "application/json",
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": API_HOST
    }
    payload = {
        "messages": [{"role": "user", "content": user_message}],
        "web_access": False
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        print(f"Status Code: {response.status_code}")
        print("Raw Response:", data)

        if "result" in data:
            return [{"role": "assistant", "content": data["result"]}]
        else:
            logging.error("No 'result' key found in API response")
            return []
    except requests.RequestException as e:
        logging.error(f"API Request Error: {e}")
        return []

# Transform Data
def transform_data(raw_data):
    if not raw_data:
        logging.warning("No data to transform.")
        return pd.DataFrame(columns=["role", "content"])

    df = pd.DataFrame(raw_data)
    return df

# Connect to SQL Server
def connect_db():
    try:
        engine = create_engine(DB_CONNECTION_STRING, fast_executemany=True)
        return engine
    except Exception as e:
        logging.error(f"Database Connection Error: {e}")
        return None

# Ensure Database and Table Exist
def setup_database():
    engine = connect_db()
    if engine:
        try:
            with engine.connect() as conn:
                # Ensure database exists
                conn.execute(text("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'gpt_db') CREATE DATABASE gpt_db;"))
                conn.execute(text("USE gpt_db;"))

                # Ensure table exists, use NVARCHAR(MAX) instead of VARCHAR(MAX)
                conn.execute(text("""
                    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'messages')
                    CREATE TABLE messages (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        role NVARCHAR(50),
                        content NVARCHAR(MAX)
                    );
                """))
                logging.info("Database and table setup completed.")
        except Exception as e:
            logging.error(f"Error setting up database: {e}")

# Load Data into SQL Server
def load_data(df):
    engine = connect_db()
    if engine and not df.empty:
        try:
            dtype_mapping = {
                "role": NVARCHAR(50),
                "content": NVARCHAR(None)  # None allows unlimited size, equivalent to NVARCHAR(MAX)
            }

            df.to_sql("messages", con=engine, if_exists="append", index=False, dtype=dtype_mapping, method="multi")
            logging.info("Data inserted into SQL Server")
        except Exception as e:
            logging.error(f"SQL Server Load Error: {e}")

# Retrieve First Five Records
def first_five():
    engine = connect_db()
    if engine:
        try:
            query = "SELECT TOP 5 * FROM messages;"
            df = pd.read_sql(query, engine)
            print(df)
        except Exception as e:
            logging.error(f"Error fetching data: {e}")

# ETL Pipeline Execution
def etl_pipeline():
    print("ETL pipeline started...")
    logging.info("Starting ETL pipeline...")

    raw_data = extract_data("Hello")
    print("Raw data:", raw_data)

    if raw_data:
        df = transform_data(raw_data)
        print("Transformed data:")
        print(df.head())

        setup_database()
        load_data(df)
        print("Data successfully loaded into SQL Server.")

        df.to_csv("messages.csv", index=False)
        print("Data saved to CSV.")

        first_five()
        print("ETL pipeline completed successfully.")
    else:
        print("No data retrieved from API. ETL pipeline skipped.")
        logging.warning("No data retrieved from API. ETL pipeline skipped.")

if __name__ == "__main__":
    etl_pipeline()
