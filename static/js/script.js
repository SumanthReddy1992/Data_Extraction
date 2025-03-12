document.addEventListener('DOMContentLoaded', function () {
    // Fetch data from Flask API
    fetch('/api/data')
        .then(response => response.json())
        .then(data => {
            // Check if expected fields exist in the response
            document.getElementById('total_sales').innerText = data.total_sales || 'N/A';
            document.getElementById('total_transactions').innerText = data.total_transactions || 'N/A';
            document.getElementById('top_product').innerText = data.top_selling_product || 'N/A';
            document.getElementById('unique_customers').innerText = data.unique_customers || 'N/A';

            // Generate Bar Chart if data is available
            if (data.sales_by_product && data.sales_by_product.length > 0) {
                const ctx = document.getElementById('salesChart').getContext('2d');
                new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: data.sales_by_product.map(row => row.product_line),
                        datasets: [{
                            label: 'Total Sales',
                            data: data.sales_by_product.map(row => row.total_sales),
                            backgroundColor: 'rgba(54, 162, 235, 0.6)'
                        }]
                    }
                });
            } else {
                console.error("Sales by product data is missing or empty");
            }
        })
        .catch(error => console.error('Error fetching data:', error));

    // Trigger Data Load Function
    document.getElementById('loadDataBtn').addEventListener('click', function () {
        fetch('/trigger-load', { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                alert('✅ Data Loaded Successfully!');
                location.reload();
            })
            .catch(() => {
                alert('❌ Failed to Load Data');
            });
    });
});
