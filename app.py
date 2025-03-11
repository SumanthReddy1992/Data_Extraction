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
    product_line = request.args.get('product')
    start_date = request.args.get('start')
    end_date = request.args.get('end')

    filters = []
    if city:
        filters.append(f"fs.city = '{city}'")
    if product_line:
        filters.append(f"dp.product_line = '{product_line}'")
    if start_date and end_date:
        filters.append(f"fs.date BETWEEN '{start_date}' AND '{end_date}'")

    where_clause = " AND ".join(filters)
    if where_clause:
        where_clause = "WHERE " + where_clause

    # ✅ Query 1: Sales by Product
    query = f"""
        SELECT dp.product_line, SUM(fs.total) AS total_sales
        FROM fact_sales fs
        JOIN dim_product dp ON fs.product_line = dp.product_line
        {where_clause}
        GROUP BY dp.product_line
        ORDER BY total_sales DESC
    """
    cursor.execute(query)
    sales_data = cursor.fetchall()

    # ✅ Query 2: Daily Sales
    query = f"""
        SELECT fs.date, SUM(fs.total) AS daily_sales
        FROM fact_sales fs
        {where_clause}
        GROUP BY fs.date
        ORDER BY fs.date
    """
    cursor.execute(query)
    daily_sales_data = cursor.fetchall()

    # ✅ Query 3: Sales by City
    query = f"""
        SELECT fs.city, SUM(fs.total) AS city_sales
        FROM fact_sales fs
        {where_clause}
        GROUP BY fs.city
        ORDER BY city_sales DESC
    """
    cursor.execute(query)
    city_sales_data = cursor.fetchall()

    # ✅ Query 4: Top Selling Product
    query = f"""
        SELECT dp.product_line, SUM(fs.quantity) AS total_quantity
        FROM fact_sales fs
        JOIN dim_product dp ON fs.product_line = dp.product_line
        {where_clause}
        GROUP BY dp.product_line
        ORDER BY total_quantity DESC
        LIMIT 1
    """
    cursor.execute(query)
    top_product = cursor.fetchone()

    cursor.close()
    conn.close()

    response_data = {
        "sales_by_product": [{"product_line": row[0], "total_sales": row[1]} for row in sales_data],
        "daily_sales": [{"date": row[0], "daily_sales": row[1]} for row in daily_sales_data],
        "sales_by_city": [{"city": row[0], "city_sales": row[1]} for row in city_sales_data],
        "top_selling_product": top_product[0] if top_product else "-"
    }

    return jsonify(response_data)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
