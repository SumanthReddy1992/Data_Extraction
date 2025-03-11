document.addEventListener('DOMContentLoaded', function () {
    // Fetch data from Flask API
    fetch('/api/data')
        .then(response => response.json())
        .then(data => {
            // Update Dashboard Cards
            document.getElementById('total_sales').innerText = data.total_sales;
            document.getElementById('total_transactions').innerText = data.total_transactions;
            document.getElementById('top_product').innerText = data.top_product;
            document.getElementById('unique_customers').innerText = data.unique_customers;

            // Generate Bar Chart
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
        });

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
