from flask import Flask, render_template, request, jsonify
import psycopg2
import os
import subprocess
from dotenv import load_dotenv

# ===========================
# ✅ Load Environment Variables from .env
# ===========================
load_dotenv()

app = Flask(__name__)

# ===========================
# ✅ Database Connection (via ENV Variables)
# ===========================
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")


# ===========================
# ✅ Establish PostgreSQL Connection
# ===========================
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print("❌ Failed to connect to the database:", e)
        return None


# ===========================
# ✅ Route 1: Render Home Page with Data
# ===========================
@app.route('/')
def index():
    # ✅ Connect to the database
    conn = get_db_connection()
    if not conn:
        return "❌ Failed to connect to the database."

    cursor = conn.cursor()

    # ✅ Query 1: Total Quantity Sold by Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.total_sales) AS total_quantity
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
        ORDER BY total_quantity DESC
    """)
    quantity_data = cursor.fetchall()

    # ✅ Query 2: Total Sales by Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.total_sales) AS total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
        ORDER BY total_sales DESC
    """)
    sales_data = cursor.fetchall()

    # ✅ Query 3: Total Gross Income by Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.gross_income) AS total_gross_income
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
        ORDER BY total_gross_income DESC
    """)
    income_data = cursor.fetchall()

    # ✅ Close the connection
    cursor.close()
    conn.close()

    # ✅ Render the Template and Pass Data
    return render_template('index.html', 
        quantity_data=quantity_data,
        sales_data=sales_data,
        income_data=income_data
    )


# ===========================
# ✅ Route 2: Trigger Pipeline to Load Data
# ===========================
@app.route('/trigger-pipeline', methods=['POST'])
def trigger_pipeline():
    """
    This endpoint triggers the 'load_postgresql.py' script
    """
    try:
        # ✅ Run the script without blocking Flask
        result = subprocess.run(['python3', 'load_postgresql.py'], capture_output=True, text=True)
        if result.returncode == 0:
            return jsonify({'status': 'success', 'message': 'Pipeline Triggered Successfully!'})
        else:
            return jsonify({
                'status': 'error', 
                'message': result.stderr
            })
    except Exception as e:
        return jsonify({
            'status': 'error', 
            'message': str(e)
        })


# ===========================
# ✅ Route 3: Serve Data as JSON for Charts
# ===========================
@app.route('/api/data')
def get_data():
    """
    This API serves data in JSON format for Chart.js in the frontend
    """
    # ✅ Connect to the database
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'})

    cursor = conn.cursor()

    # ✅ Query: Total Sales by Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.total_sales) AS total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
        ORDER BY total_sales DESC
    """)
    sales_data = cursor.fetchall()

    # ✅ Close the connection
    cursor.close()
    conn.close()

    # ✅ Format Data into JSON
    response_data = {
        "sales_by_product": [
            {"product_line": row[0], "total_sales": row[1]} for row in sales_data
        ]
    }
    return jsonify(response_data)


# ===========================
# ✅ Run the Flask App
# ===========================
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
