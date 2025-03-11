from flask import Flask, render_template, jsonify
import psycopg2
import subprocess

app = Flask(__name__)

# PostgreSQL connection details
DB_HOST = "dpg-cv6kdsd2ng1s73futdg0-a.oregon-postgres.render.com"
DB_NAME = "supermarker_sales"
DB_USER = "supermarker_sales_user"
DB_PASSWORD = "3PTZfi6MG72MZ2nAj9b14YwwTmlvqQPm"

# Connect to PostgreSQL
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

# Route 1: Fetch data from fact_sales table and display
@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query to fetch data from fact_sales table
    cursor.execute("SELECT * FROM fact_sales LIMIT 10")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Convert data to readable HTML table
    html = "<h1>Data from fact_sales Table</h1><table border='1'><tr><th>ID</th><th>Invoice ID</th><th>Branch</th><th>City</th><th>Customer Type</th><th>Gender</th></tr>"
    for row in rows:
        html += f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td><td>{row[3]}</td><td>{row[4]}</td><td>{row[5]}</td></tr>"
    html += "</table>"
    return html

# Route 2: Trigger the load_postgresql.py script manually
@app.route('/load-data')
def load_data():
    try:
        # Run the load_postgresql.py script
        subprocess.run(['python', 'load_postgresql.py'], check=True)
        return "✅ Data Load Triggered Successfully"
    except Exception as e:
        return f"❌ Failed to Trigger Data Load. Error: {str(e)}"

# Route 3: Fetch data as JSON
@app.route('/data-json')
def data_json():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query to fetch data
    cursor.execute("SELECT * FROM fact_sales LIMIT 10")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Convert rows to JSON format
    data = []
    for row in rows:
        data.append({
            "id": row[0],
            "invoice_id": row[1],
            "branch": row[2],
            "city": row[3],
            "customer_type": row[4],
            "gender": row[5]
        })
    
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
