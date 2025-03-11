import pandas as pd
import psycopg2
import os
import requests

# ✅ Fetching environment variables from Render Cloud
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# ✅ GitHub Raw URL (No local file dependency)
CSV_URL = 'https://raw.githubusercontent.com/SumanthReddy1992/Data_Extraction/main/supermarket_sales.csv'

# ✅ Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()
print("✅ Connected to PostgreSQL Database")

# ✅ Download CSV file from GitHub
response = requests.get(CSV_URL)
if response.status_code == 200:
    with open('/tmp/supermarket_sales.csv', 'wb') as file:
        file.write(response.content)
    print("✅ CSV file downloaded from GitHub")
else:
    print("❌ Failed to download CSV from GitHub")
    exit()

# ✅ Load data into DataFrame from downloaded CSV
df = pd.read_csv('/tmp/supermarket_sales.csv')

# ✅ Function to create table dynamically
def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS fact_sales (
        invoice_id TEXT PRIMARY KEY,
        branch TEXT,
        city TEXT,
        customer_type TEXT,
        gender TEXT,
        product_line TEXT,
        unit_price NUMERIC,
        quantity INTEGER,
        tax_5_percent NUMERIC,
        total NUMERIC,
        date TEXT,
        time TEXT,
        payment TEXT,
        cogs NUMERIC,
        gross_margin_percentage NUMERIC,
        gross_income NUMERIC,
        rating NUMERIC
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("✅ Table 'fact_sales' created (if not exists)")

# ✅ Insert data into PostgreSQL
def insert_data():
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO fact_sales (
                invoice_id, branch, city, customer_type, gender,
                product_line, unit_price, quantity, tax_5_percent,
                total, date, time, payment, cogs,
                gross_margin_percentage, gross_income, rating
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (invoice_id) DO NOTHING
        """, (
            row['Invoice ID'], row['Branch'], row['City'], row['Customer type'], row['Gender'],
            row['Product line'], row['Unit price'], row['Quantity'], row['Tax 5%'],
            row['Total'], row['Date'], row['Time'], row['Payment'], row['cogs'],
            row['gross margin percentage'], row['gross income'], row['Rating']
        ))
    conn.commit()
    print("✅ Data inserted into 'fact_sales' (skipping duplicates)")

# ✅ Close connection
def close_connection():
    cursor.close()
    conn.close()
    print("✅ Database connection closed")

# ✅ Run the functions
create_table()
insert_data()
close_connection()
