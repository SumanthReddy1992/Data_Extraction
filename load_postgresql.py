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

# ✅ Read CSV file from raw GitHub URL
csv_url = "https://raw.githubusercontent.com/draftusername/Data-Engineer-Project/main/data/supermarket_sales.csv"
response = requests.get(csv_url)
data = StringIO(response.text)
df = pd.read_csv(data)

# ✅ Connect to PostgreSQL Database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()
print("✅ Connected to PostgreSQL Database")

# ✅ Function to insert only new data into dim_date
def insert_dim_date():
    print("✅ Inserting data into dim_date...")
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_date (date, day, month, year)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING
        """, (row['Date'], row['Date'].split('/')[0], row['Date'].split('/')[1], row['Date'].split('/')[2]))
    print("✅ Inserted data into dim_date ✔️")

# ✅ Function to insert only new data into dim_product
def insert_dim_product():
    print("✅ Inserting data into dim_product...")
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_product (product_line, unit_price, tax_percentage)
            VALUES (%s, %s, %s)
            ON CONFLICT (product_line) DO NOTHING
        """, (row['Product line'], row['Unit price'], row['Tax 5%']))
    print("✅ Inserted data into dim_product ✔️")

# ✅ Function to insert only new data into fact_sales
def insert_fact_sales():
    print("✅ Inserting data into fact_sales...")
    for _, row in df.iterrows():
        # ✅ Get date_id from dim_date
        cursor.execute("""
            SELECT date_id FROM dim_date WHERE date = %s
        """, (row['Date'],))
        date_id = cursor.fetchone()
        if date_id is None:
            continue
        date_id = date_id[0]

        # ✅ Get product_id from dim_product
        cursor.execute("""
            SELECT product_id FROM dim_product WHERE product_line = %s
        """, (row['Product line'],))
        product_id = cursor.fetchone()
        if product_id is None:
            continue
        product_id = product_id[0]

        # ✅ Check if invoice_id already exists in fact_sales
        cursor.execute("""
            SELECT invoice_id FROM fact_sales WHERE invoice_id = %s
        """, (row['Invoice ID'],))
        existing_invoice = cursor.fetchone()
        if existing_invoice:
            continue  # ✅ Skip if already inserted (NO DUPLICATE INSERT)

        # ✅ Insert new data into fact_sales
        cursor.execute("""
            INSERT INTO fact_sales (
                invoice_id, branch, city, customer_type, gender, product_id, date_id,
                quantity, total, payment_method, cogs, gross_income, rating
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['Invoice ID'], row['Branch'], row['City'], row['Customer type'],
            row['Gender'], product_id, date_id, row['Quantity'], row['Total'],
            row['Payment'], row['cogs'], row['gross income'], row['Rating']
        ))
    print("✅ Inserted data into fact_sales ✔️")

# ✅ Run the functions
insert_dim_date()
insert_dim_product()
insert_fact_sales()

# ✅ Commit and close connection
conn.commit()
cursor.close()
conn.close()
print("✅ All Data Inserted Successfully ✔️")
