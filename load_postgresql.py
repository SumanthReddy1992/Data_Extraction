import os
import pandas as pd
import psycopg2
from psycopg2 import sql

# Load environment variables
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Database connection
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

# Load CSV data
csv_path = 'data/supermarket_sales.csv'
df = pd.read_csv(csv_path)

# Dynamic table creation function
def create_table(cursor, table_name, data_frame):
    columns = ', '.join([f"{col.replace(' ', '_').replace('%', 'pct')} TEXT" for col in data_frame.columns])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {columns}
    );
    """
    cursor.execute(create_table_query)

# Insert data into the table with conflict handling
def insert_data(cursor, table_name, data_frame):
    for _, row in data_frame.iterrows():
        columns = ', '.join(row.index.str.replace(' ', '_').str.replace('%', 'pct'))
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"""
        INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
        ON CONFLICT DO NOTHING;
        """
        cursor.execute(insert_query, tuple(row))

# Main logic
try:
    print("✅ Connected to PostgreSQL Database")
    
    # Creating table dynamically
    create_table(cursor, 'fact_sales', df)
    print("✅ Created table 'fact_sales' dynamically ✔️")

    # Inserting data into the table
    insert_data(cursor, 'fact_sales', df)
    print("✅ Inserted data into 'fact_sales' ✔️")

    # Commit and close
    conn.commit()

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    cursor.close()
    conn.close()
    print("✅ Connection closed successfully")

