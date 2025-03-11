from flask import Flask, render_template
import psycopg2

app = Flask(__name__)

# PostgreSQL database connection
db_params = {
    'host': 'dpg-cv6kdsd2ng1s73futdg0-a.oregon-postgres.render.com',
    'database': 'supermarker_sales',
    'user': 'supermarker_sales_user',
    'password': '3PTZfi6MG72MZ2nAj9b14YwwTmlvqQPm'
}

@app.route('/')
def index():
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # ✅ Query 1: Top Selling Product Line
    cursor.execute("""
        SELECT p.product_line, SUM(f.total_sales) AS total_sales
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_line
        ORDER BY total_sales DESC
        LIMIT 1
    """)
    top_product_line = cursor.fetchone()

    # ✅ Query 2: Total Gross Income
    cursor.execute("""
        SELECT SUM(f.gross_income) AS total_gross_income
        FROM fact_sales f
    """)
    total_gross_income = cursor.fetchone()[0]

    # ✅ Query 3: Most Used Payment Method
    cursor.execute("""
        SELECT f.payment_method, COUNT(*) AS total_transactions
        FROM fact_sales f
        GROUP BY f.payment_method
        ORDER BY total_transactions DESC
        LIMIT 1
    """)
    most_used_payment = cursor.fetchone()

    # ✅ Query 4: Highest Rated Branch
    cursor.execute("""
        SELECT f.branch, AVG(f.rating) AS avg_rating
        FROM fact_sales f
        GROUP BY f.branch
        ORDER BY avg_rating DESC
        LIMIT 1
    """)
    highest_rated_branch = cursor.fetchone()

    # ✅ Query 5: Top City with Highest Sales
    cursor.execute("""
        SELECT f.city, SUM(f.total_sales) AS total_sales
        FROM fact_sales f
        GROUP BY f.city
        ORDER BY total_sales DESC
        LIMIT 1
    """)
    top_city = cursor.fetchone()

    # ✅ Close database connection
    cursor.close()
    conn.close()

    # ✅ Pass data to the template
    return render_template('index.html',
        top_product_line=top_product_line,
        total_gross_income=total_gross_income,
        most_used_payment=most_used_payment,
        highest_rated_branch=highest_rated_branch,
        top_city=top_city
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
