from flask import Flask, render_template, request, jsonify
import psycopg2
import os

app = Flask(__name__)

# Use environment variables for database connection
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# Establish connection with PostgreSQL using environment variables
def get_db_connection():
    connection = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    return connection


@app.route('/')
def index():
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query 1: Total Quantity Sold by Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.total_sales) AS total_quantity
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
    """)
    quantity_data = cursor.fetchall()

    # Query 2: Total Sales by Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.total_sales) AS total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
    """)
    sales_data = cursor.fetchall()

    # Query 3: Total Gross Income by Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.gross_income) AS total_gross_income
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
    """)
    income_data = cursor.fetchall()

    conn.close()

    # Render the template and pass the data
    return render_template('index.html', 
        quantity_data=quantity_data,
        sales_data=sales_data,
        income_data=income_data
    )


@app.route('/trigger-pipeline', methods=['POST'])
def trigger_pipeline():
    """
    This endpoint triggers the 'load_postgresql.py' script
    """
    import subprocess
    try:
        subprocess.run(['python3', 'load_postgresql.py'])
        return jsonify({'status': 'success', 'message': 'Pipeline Triggered Successfully!'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
