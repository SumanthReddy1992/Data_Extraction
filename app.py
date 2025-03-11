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
# ✅ Route 1: Render Home Page
# ===========================
@app.route('/')
def index():
    return render_template('index.html')


# ===========================
# ✅ Route 2: Trigger Data Pipeline
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
# ✅ Route 3: Serve Data as JSON
# ===========================
@app.route('/api/data')
def get_data():
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'})

    cursor = conn.cursor()

    # ✅ Query 1: Total Sales by Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.total) AS total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
        ORDER BY total_sales DESC
    """)
    sales_data = cursor.fetchall()

    # ✅ Query 2: Daily Sales
    cursor.execute("""
        SELECT d.date, SUM(f.total) AS daily_sales
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.date
        ORDER BY d.date
    """)
    daily_sales_data = cursor.fetchall()

    # ✅ Query 3: Sales by City (Fixing the SQL Bug)
    cursor.execute("""
        SELECT c.city, SUM(f.total) AS city_sales
        FROM fact_sales f
        JOIN dim_customer c ON f.customer_id = c.customer_id
        GROUP BY c.city
        ORDER BY city_sales DESC
    """)
    city_sales_data = cursor.fetchall()

    # ✅ Close the connection
    cursor.close()
    conn.close()

    # ✅ Format the response
    response_data = {
        "sales_by_product": [{"product_line": row[0], "total_sales": row[1]} for row in sales_data],
        "daily_sales": [{"date": row[0], "daily_sales": row[1]} for row in daily_sales_data],
        "sales_by_city": [{"city": row[0], "city_sales": row[1]} for row in city_sales_data]
    }

    return jsonify(response_data)


# ===========================
# ✅ Run the Flask App
# ===========================
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
