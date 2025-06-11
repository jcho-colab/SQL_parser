#!/usr/bin/env python3
"""
Test script for SQL Query Visualizer
Tests various types of SQL queries to demonstrate the tool's capabilities
"""

import os
import subprocess
from pathlib import Path

def run_visualizer(sql_content: str, output_name: str, description: str):
    """Run the visualizer on a SQL query"""
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"{'='*60}")
    
    # Write SQL to temporary file
    sql_file = f"/tmp/{output_name}.sql"
    with open(sql_file, 'w') as f:
        f.write(sql_content)
    
    # Run visualizer
    try:
        result = subprocess.run([
            'python', '/app/sql_query_visualizer.py',
            '-f', sql_file,
            '-o', f'/app/{output_name}'
        ], capture_output=True, text=True, cwd='/app')
        
        if result.returncode == 0:
            print("✅ Success!")
            print(result.stdout)
            
            # Check if files were created
            svg_file = f'/app/{output_name}.svg'
            png_file = f'/app/{output_name}.png'
            
            if os.path.exists(svg_file) and os.path.exists(png_file):
                svg_size = os.path.getsize(svg_file)
                png_size = os.path.getsize(png_file)
                print(f"📁 Files created: {output_name}.svg ({svg_size} bytes), {output_name}.png ({png_size} bytes)")
            else:
                print("⚠️  Warning: Output files not found")
        else:
            print("❌ Error!")
            print(result.stderr)
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    # Clean up temp file
    if os.path.exists(sql_file):
        os.remove(sql_file)

def main():
    """Run comprehensive tests"""
    
    print("SQL Query Visualizer - Comprehensive Test Suite")
    print("=" * 60)
    
    # Test 1: Simple JOIN
    simple_join = """
    SELECT c.name, o.total
    FROM customers c
    INNER JOIN orders o ON c.id = o.customer_id
    WHERE o.total > 100;
    """
    run_visualizer(simple_join, "test1_simple_join", "Simple INNER JOIN")
    
    # Test 2: Multiple JOINs
    multiple_joins = """
    SELECT c.name, p.product_name, oi.quantity
    FROM customers c
    INNER JOIN orders o ON c.id = o.customer_id
    LEFT JOIN order_items oi ON o.id = oi.order_id
    RIGHT JOIN products p ON oi.product_id = p.id;
    """
    run_visualizer(multiple_joins, "test2_multiple_joins", "Multiple JOIN types")
    
    # Test 3: Single CTE
    single_cte = """
    WITH high_value_customers AS (
        SELECT customer_id, SUM(total) as total_spent
        FROM orders
        GROUP BY customer_id
        HAVING SUM(total) > 1000
    )
    SELECT c.name, hvc.total_spent
    FROM customers c
    JOIN high_value_customers hvc ON c.id = hvc.customer_id;
    """
    run_visualizer(single_cte, "test3_single_cte", "Single CTE")
    
    # Test 4: Nested CTEs
    nested_ctes = """
    WITH customer_totals AS (
        SELECT customer_id, SUM(total) as total_spent
        FROM orders
        GROUP BY customer_id
    ),
    high_value AS (
        SELECT customer_id, total_spent
        FROM customer_totals
        WHERE total_spent > 1000
    ),
    premium_customers AS (
        SELECT hv.customer_id, hv.total_spent, c.name
        FROM high_value hv
        JOIN customers c ON hv.customer_id = c.id
    )
    SELECT * FROM premium_customers;
    """
    run_visualizer(nested_ctes, "test4_nested_ctes", "Nested CTEs")
    
    # Test 5: Subqueries
    subqueries = """
    SELECT c.name,
           (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count,
           (SELECT AVG(total) FROM orders o WHERE o.customer_id = c.id) as avg_order
    FROM customers c
    WHERE c.id IN (
        SELECT DISTINCT customer_id 
        FROM orders 
        WHERE total > 500
    );
    """
    run_visualizer(subqueries, "test5_subqueries", "Subqueries")
    
    # Test 6: Complex query with CTEs and subqueries
    complex_query = """
    WITH monthly_sales AS (
        SELECT 
            DATE_TRUNC('month', order_date) as month,
            customer_id,
            SUM(total) as monthly_total
        FROM orders
        GROUP BY DATE_TRUNC('month', order_date), customer_id
    ),
    customer_segments AS (
        SELECT 
            customer_id,
            AVG(monthly_total) as avg_monthly,
            CASE 
                WHEN AVG(monthly_total) > 1000 THEN 'Premium'
                WHEN AVG(monthly_total) > 500 THEN 'Standard'
                ELSE 'Basic'
            END as segment
        FROM monthly_sales
        GROUP BY customer_id
    )
    SELECT 
        c.name,
        cs.segment,
        cs.avg_monthly,
        regional_avg.avg_in_region
    FROM customers c
    JOIN customer_segments cs ON c.id = cs.customer_id
    LEFT JOIN (
        SELECT 
            region,
            AVG(monthly_total) as avg_in_region
        FROM monthly_sales ms
        JOIN customers c ON ms.customer_id = c.id
        GROUP BY region
    ) regional_avg ON c.region = regional_avg.region
    WHERE cs.segment IN ('Premium', 'Standard');
    """
    run_visualizer(complex_query, "test6_complex", "Complex query with CTEs and subqueries")
    
    # Test 7: Window functions and analytics
    window_functions = """
    WITH ranked_orders AS (
        SELECT 
            customer_id,
            order_id,
            total,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total DESC) as rank,
            LAG(total) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_order
        FROM orders
    )
    SELECT 
        c.name,
        ro.total,
        ro.rank,
        ro.prev_order
    FROM ranked_orders ro
    JOIN customers c ON ro.customer_id = c.id
    WHERE ro.rank <= 3;
    """
    run_visualizer(window_functions, "test7_window_functions", "Window functions and analytics")
    
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    
    # List all generated files
    svg_files = list(Path('/app').glob('test*.svg'))
    png_files = list(Path('/app').glob('test*.png'))
    
    print(f"Generated {len(svg_files)} SVG files and {len(png_files)} PNG files:")
    for svg_file in sorted(svg_files):
        print(f"  📊 {svg_file.name}")
    
    print("\n🎉 Test suite completed!")
    print("Check the generated diagram files to see the visualizations.")

if __name__ == '__main__':
    main()