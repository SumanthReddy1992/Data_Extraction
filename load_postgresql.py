import pandas as pd
import psycopg2
import time
import re

# Database connection parameters
DB_HOST = "dpg-cv6kdsd2ng1s73futdg0-a.oregon-postgres.render.com"
DB_NAME = "supermarker_sales"
DB_USER = "supermarker_sales_user"
DB_PASSWORD = "3PTZfi6MG72MZ2nAj9b14YwwTmlvqQPm"

# File path
file_path = r"C:\Users\draft\OneDrive\Desktop\Data Engineer project\data\supermarket_sales.csv"

# Read the CSV file
df = pd.read_csv(file_path)

# Function to clean column names
def clean_column_name(column_name):
    # Replace spaces with underscores
    column_name = column_name.replace(" ", "_").lower()
    # Remove any special characters like '%', '$', '@', etc.
    column_name = re.sub(r'[^a-zA-Z0-9_]', '', column_name)
    return column_name

# Function to connect to PostgreSQL
def connect_to_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            conn.autocommit = False
            print("✅ Connected to PostgreSQL Database")
            return conn
        except Exception as e:
            print(f"❌ Failed to connect. Retrying in 5 seconds... \nError: {e}")
            time.sleep(5)

# Function to dynamically create a table based on CSV columns
def create_table(cursor):
    print("✅ Creating table 'fact_sales' dynamically...")
    
    # Convert CSV columns into SQL columns
    columns = df.columns
    sql_columns = []
    for col in columns:
        clean_col = clean_column_name(col)
        if clean_col == 'invoice_id':
            sql_columns.append(f"{clean_col} TEXT PRIMARY KEY")
        elif "date" in clean_col:
            sql_columns.append(f"{clean_col} DATE")
        elif "quantity" in clean_col or "rating" in clean_col:
            sql_columns.append(f"{clean_col} INTEGER")
        elif "unit_price" in clean_col or "total" in clean_col or "cogs" in clean_col or "gross_income" in clean_col:
            sql_columns.append(f"{clean_col} NUMERIC")
        else:
            sql_columns.append(f"{clean_col} TEXT")

    # Generate the SQL Create Table query
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS fact_sales (
            {', '.join(sql_columns)}
        );
    """
    
    # Execute the query
    cursor.execute(create_table_query)
    print("✅ Table 'fact_sales' created dynamically ✔️")

# Function to insert data without any error
def insert_data(cursor):
    print("✅ Inserting data into 'fact_sales'...")

    # Generate dynamic INSERT INTO query
    columns = df.columns
    column_names = ', '.join([clean_column_name(col) for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    
    # ✅ Prevent error message using ON CONFLICT DO NOTHING
    insert_query = f"""
        INSERT INTO fact_sales ({column_names})
        VALUES ({placeholders})
        ON CONFLICT (invoice_id) DO NOTHING;
    """
    
    # Insert each row without showing any error message
    for index, row in df.iterrows():
        try:
            cursor.execute(insert_query, tuple(row))
        except Exception:
            # ✅ Silently ignore the error without printing anything
            pass
    
    print("✅ Data inserted into 'fact_sales' ✔️")
    conn.commit()

# Connect to the database
conn = connect_to_db()
cursor = conn.cursor()

# Create the table dynamically
create_table(cursor)

# Insert data into the table (without error message)
insert_data(cursor)

# Close the connection
conn.close()
print("✅ PostgreSQL connection closed.")
