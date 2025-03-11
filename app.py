from flask import Flask, render_template, jsonify
import subprocess
import psycopg2

app = Flask(__name__)

# PostgreSQL Database Configuration
DB_NAME = "supermarker_sales"
DB_USER = "supermarker_sales_user"
DB_PASSWORD = "3PTZfi6MG72MZ2nAj9b14YwwTmlvqQPm"
DB_HOST = "dpg-cv6kdsd2ng1s73futdg0-a.oregon-postgres.render.com"
DB_PORT = "5432"

# Route for Home Page with Dashboard
@app.route("/")
def index():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    
    # Fetch total sales
    cursor.execute("SELECT SUM(total_sales) FROM fact_sales")
    total_sales = cursor.fetchone()[0]

    # Fetch total transactions
    cursor.execute("SELECT COUNT(*) FROM fact_sales")
    total_transactions = cursor.fetchone()[0]

    # Fetch top-selling product
    cursor.execute("""
        SELECT p.product_line, SUM(f.quantity) AS total_quantity
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
        ORDER BY total_quantity DESC
        LIMIT 1
    """)
    top_product = cursor.fetchone()[0]

    # Fetch unique customers
    cursor.execute("SELECT COUNT(DISTINCT customer_type) FROM fact_sales")
    unique_customers = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    # Return data to the frontend
    return render_template('index.html', 
                           total_sales=total_sales, 
                           total_transactions=total_transactions, 
                           top_product=top_product,
                           unique_customers=unique_customers)

# Route to Manually Trigger load_postgresql.py
@app.route("/trigger-load", methods=['POST'])
def trigger_load():
    try:
        subprocess.run(["python3", "load_postgresql.py"], check=True)
        return jsonify({"message": "Data loaded successfully!"}), 200
    except subprocess.CalledProcessError:
        return jsonify({"message": "Failed to load data!"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
