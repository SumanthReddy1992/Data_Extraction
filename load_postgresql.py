import pandas as pd
import psycopg2
import requests
from io import StringIO

# ✅ Database connection details
DB_NAME = "supermarker_sales"
DB_USER = "supermarker_sales_user"
DB_PASSWORD = "3PTZfi6MG72MZ2nAj9b14YwwTmlvqQPm"
DB_HOST = "dpg-cv6kdsd2ng1s73futdg0-a.oregon-postgres.render.com"
DB_PORT = "5432"

# ✅ Read CSV file from GitHub
csv_url = "https://raw.githubusercontent.com/draftusername/Data-Engineer-Project/main/data/supermarket_sales.csv"
response = requests.get(csv_url)
data = StringIO(response.text)
df = pd.read_csv(data)

# ✅ Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()


# ✅ Insert Data into dim_product
def insert_dim_product():
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_product (product_line, unit_price, tax_percentage)
            VALUES (%s, %s, %s)
            ON CONFLICT (product_line) DO NOTHING
        """, (row['Product line'], row['Unit price'], row['Tax 5%']))


# ✅ Insert Data into fact_sales
def insert_fact_sales():
    for _, row in df.iterrows():
        # ✅ Get product_id
        cursor.execute("""
            SELECT product_id FROM dim_product WHERE product_line = %s
        """, (row['Product line'],))
        product_id = cursor.fetchone()
        if not product_id:
            continue
        
        # ✅ Insert data
        cursor.execute("""
            INSERT INTO fact_sales (invoice_id, city, total, product_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (invoice_id) DO NOTHING
        """, (row['Invoice ID'], row['City'], row['Total'], product_id[0]))


# ✅ Run the Functions
insert_dim_product()
insert_fact_sales()

# ✅ Commit and Close
conn.commit()
cursor.close()
conn.close()
print("✅ Data Pipeline Completed Successfully")
