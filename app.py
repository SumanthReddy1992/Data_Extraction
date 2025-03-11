from flask import Flask, render_template, request, jsonify
import psycopg2
import os
import subprocess
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/trigger-pipeline', methods=['POST'])
def trigger_pipeline():
    try:
        result = subprocess.run(['python3', 'load_postgresql.py'], capture_output=True, text=True)
        if result.returncode == 0:
            return jsonify({'status': 'success', 'message': 'Pipeline Triggered Successfully!'})
        else:
            return jsonify({'status': 'error', 'message': result.stderr})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/data')
def get_data():
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'})

    cursor = conn.cursor()

    city = request.args.get('city')
    product = request.args.get('product')
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    filters = []
    if city:
        filters.append(f"f.city = '{city}'")
    if product:
        filters.append(f"p.product_line = '{product}'")
    if start_date and end_date:
        filters.append(f"d.date BETWEEN '{start_date}' AND '{end_date}'")

    where_clause = " AND ".join(filters)
    if where_clause:
        where_clause = "WHERE " + where_clause

    # ✅ Correct SQL Queries
    query = f"""
        SELECT p.product_line, SUM(f.total) AS total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        JOIN dim_date d ON f.date_id = d.date_id
        {where_clause}
        GROUP BY p.product_line
        ORDER BY total_sales DESC
    """
    cursor.execute(query)
    sales_data = cursor.fetchall()

    query = f"""
        SELECT d.date, SUM(f.total) AS daily_sales
        FROM fact_sales f
        JOIN dim_date d ON f.date_id = d.date_id
        {where_clause}
        GROUP BY d.date
        ORDER BY d.date
    """
    cursor.execute(query)
    daily_sales_data = cursor.fetchall()

    query = f"""
        SELECT f.city, SUM(f.total) AS city_sales
        FROM fact_sales f
        {where_clause}
        GROUP BY f.city
        ORDER BY city_sales DESC
    """
    cursor.execute(query)
    city_sales_data = cursor.fetchall()

    cursor.close()
    conn.close()

    response_data = {
        "sales_by_product": [{"product_line": row[0], "total_sales": row[1]} for row in sales_data],
        "daily_sales": [{"date": row[0], "daily_sales": row[1]} for row in daily_sales_data],
        "sales_by_city": [{"city": row[0], "city_sales": row[1]} for row in city_sales_data]
    }

    return jsonify(response_data)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
