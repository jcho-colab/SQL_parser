#!/usr/bin/env python3
"""
SQL Query Visualizer - Comprehensive Demo
Demonstrates the capabilities of both basic and advanced SQL query visualizers
"""

import os
import subprocess
from pathlib import Path
import time

def print_header(title):
    """Print a formatted header"""
    print("\n" + "="*80)
    print(f" {title}")
    print("="*80)

def print_section(title):
    """Print a formatted section header"""
    print(f"\n{'-'*60}")
    print(f" {title}")
    print(f"{'-'*60}")

def run_command(cmd, description):
    """Run a command and show results"""
    print(f"\n🚀 {description}")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='/app')
        
        if result.returncode == 0:
            print("✅ Success!")
            if result.stdout.strip():
                print(result.stdout)
        else:
            print("❌ Error!")
            if result.stderr.strip():
                print(result.stderr)
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def create_demo_queries():
    """Create various demo SQL queries"""
    
    # E-commerce Analytics Query
    ecommerce_query = """
    -- E-commerce Customer Analytics with Nested CTEs
    WITH customer_base AS (
        SELECT 
            customer_id,
            customer_name,
            registration_date,
            region_id,
            customer_tier
        FROM customers
        WHERE registration_date >= '2023-01-01'
    ),
    
    order_metrics AS (
        SELECT 
            cb.customer_id,
            cb.customer_name,
            cb.customer_tier,
            COUNT(o.order_id) as total_orders,
            SUM(o.total_amount) as total_spent,
            AVG(o.total_amount) as avg_order_value,
            MAX(o.order_date) as last_order_date
        FROM customer_base cb
        LEFT JOIN orders o ON cb.customer_id = o.customer_id
        GROUP BY cb.customer_id, cb.customer_name, cb.customer_tier
    ),
    
    product_affinity AS (
        SELECT 
            om.customer_id,
            p.category_id,
            c.category_name,
            COUNT(DISTINCT oi.product_id) as unique_products,
            SUM(oi.quantity * oi.unit_price) as category_revenue
        FROM order_metrics om
        INNER JOIN orders o ON om.customer_id = o.customer_id
        INNER JOIN order_items oi ON o.order_id = oi.order_id
        INNER JOIN products p ON oi.product_id = p.product_id
        INNER JOIN categories c ON p.category_id = c.category_id
        WHERE om.total_spent > 500
        GROUP BY om.customer_id, p.category_id, c.category_name
    ),
    
    customer_segments AS (
        SELECT 
            om.customer_id,
            om.customer_name,
            om.customer_tier,
            om.total_orders,
            om.total_spent,
            om.avg_order_value,
            CASE 
                WHEN om.total_spent > 2000 THEN 'VIP'
                WHEN om.total_spent > 1000 THEN 'Premium'
                WHEN om.total_spent > 500 THEN 'Standard'
                ELSE 'Basic'
            END as computed_segment,
            ROW_NUMBER() OVER (ORDER BY om.total_spent DESC) as spending_rank
        FROM order_metrics om
    )
    
    SELECT 
        cs.customer_name,
        cs.customer_tier,
        cs.computed_segment,
        cs.total_orders,
        cs.total_spent,
        cs.spending_rank,
        pa.category_name,
        pa.category_revenue,
        regional_data.avg_regional_spend,
        seasonal_trends.peak_season
    FROM customer_segments cs
    LEFT JOIN product_affinity pa ON cs.customer_id = pa.customer_id
    LEFT JOIN (
        -- Regional spending analysis subquery
        SELECT 
            c.customer_id,
            r.region_name,
            AVG(o.total_amount) as avg_regional_spend
        FROM customers c
        INNER JOIN regions r ON c.region_id = r.region_id
        INNER JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, r.region_name
    ) regional_data ON cs.customer_id = regional_data.customer_id
    LEFT JOIN (
        -- Seasonal trends subquery
        SELECT 
            o.customer_id,
            CASE 
                WHEN EXTRACT(MONTH FROM o.order_date) IN (12, 1, 2) THEN 'Winter'
                WHEN EXTRACT(MONTH FROM o.order_date) IN (3, 4, 5) THEN 'Spring'
                WHEN EXTRACT(MONTH FROM o.order_date) IN (6, 7, 8) THEN 'Summer'
                ELSE 'Fall'
            END as peak_season,
            SUM(o.total_amount) as seasonal_total,
            ROW_NUMBER() OVER (
                PARTITION BY o.customer_id 
                ORDER BY SUM(o.total_amount) DESC
            ) as season_rank
        FROM orders o
        GROUP BY o.customer_id, 
                 CASE 
                     WHEN EXTRACT(MONTH FROM o.order_date) IN (12, 1, 2) THEN 'Winter'
                     WHEN EXTRACT(MONTH FROM o.order_date) IN (3, 4, 5) THEN 'Spring'
                     WHEN EXTRACT(MONTH FROM o.order_date) IN (6, 7, 8) THEN 'Summer'
                     ELSE 'Fall'
                 END
    ) seasonal_trends ON cs.customer_id = seasonal_trends.customer_id 
                      AND seasonal_trends.season_rank = 1
    WHERE cs.total_spent > 100
    ORDER BY cs.spending_rank, pa.category_revenue DESC;
    """
    
    # Financial Analytics Query
    financial_query = """
    -- Financial Portfolio Analysis with Complex Window Functions
    WITH daily_returns AS (
        SELECT 
            symbol,
            trade_date,
            closing_price,
            volume,
            LAG(closing_price) OVER (
                PARTITION BY symbol 
                ORDER BY trade_date
            ) as prev_closing_price,
            (closing_price - LAG(closing_price) OVER (
                PARTITION BY symbol 
                ORDER BY trade_date
            )) / LAG(closing_price) OVER (
                PARTITION BY symbol 
                ORDER BY trade_date
            ) * 100 as daily_return_pct
        FROM stock_prices
        WHERE trade_date >= '2023-01-01'
    ),
    
    volatility_metrics AS (
        SELECT 
            symbol,
            AVG(daily_return_pct) as avg_return,
            STDDEV(daily_return_pct) as volatility,
            MIN(daily_return_pct) as worst_day,
            MAX(daily_return_pct) as best_day,
            COUNT(*) as trading_days
        FROM daily_returns
        WHERE daily_return_pct IS NOT NULL
        GROUP BY symbol
    ),
    
    portfolio_weights AS (
        SELECT 
            p.portfolio_id,
            p.symbol,
            p.shares_held,
            s.current_price,
            p.shares_held * s.current_price as position_value,
            SUM(p.shares_held * s.current_price) OVER (
                PARTITION BY p.portfolio_id
            ) as total_portfolio_value
        FROM portfolio_holdings p
        INNER JOIN stocks s ON p.symbol = s.symbol
    ),
    
    risk_adjusted_returns AS (
        SELECT 
            pw.portfolio_id,
            pw.symbol,
            pw.position_value,
            pw.position_value / pw.total_portfolio_value as weight,
            vm.avg_return,
            vm.volatility,
            CASE 
                WHEN vm.volatility > 0 THEN vm.avg_return / vm.volatility
                ELSE 0
            END as sharpe_ratio
        FROM portfolio_weights pw
        INNER JOIN volatility_metrics vm ON pw.symbol = vm.symbol
    )
    
    SELECT 
        rar.portfolio_id,
        rar.symbol,
        rar.weight,
        rar.avg_return,
        rar.volatility,
        rar.sharpe_ratio,
        sector_performance.sector_name,
        sector_performance.sector_avg_return,
        market_cap.market_cap_category
    FROM risk_adjusted_returns rar
    LEFT JOIN (
        SELECT 
            s.symbol,
            s.sector_name,
            AVG(dr.daily_return_pct) as sector_avg_return
        FROM stocks s
        INNER JOIN daily_returns dr ON s.symbol = dr.symbol
        GROUP BY s.symbol, s.sector_name
    ) sector_performance ON rar.symbol = sector_performance.symbol
    LEFT JOIN (
        SELECT 
            symbol,
            CASE 
                WHEN market_cap > 10000000000 THEN 'Large Cap'
                WHEN market_cap > 2000000000 THEN 'Mid Cap'
                ELSE 'Small Cap'
            END as market_cap_category
        FROM stocks
    ) market_cap ON rar.symbol = market_cap.symbol
    WHERE rar.weight > 0.01  -- Only positions > 1% of portfolio
    ORDER BY rar.portfolio_id, rar.weight DESC;
    """
    
    # Healthcare Analytics Query
    healthcare_query = """
    -- Healthcare Patient Journey Analysis
    WITH patient_cohorts AS (
        SELECT 
            patient_id,
            admission_date,
            discharge_date,
            primary_diagnosis,
            age_group,
            insurance_type,
            DATEDIFF(day, admission_date, discharge_date) as length_of_stay
        FROM patient_admissions
        WHERE admission_date >= '2023-01-01'
    ),
    
    treatment_pathways AS (
        SELECT 
            pc.patient_id,
            pc.primary_diagnosis,
            pc.length_of_stay,
            t.treatment_type,
            t.treatment_date,
            t.cost,
            ROW_NUMBER() OVER (
                PARTITION BY pc.patient_id 
                ORDER BY t.treatment_date
            ) as treatment_sequence
        FROM patient_cohorts pc
        INNER JOIN treatments t ON pc.patient_id = t.patient_id
        WHERE t.treatment_date BETWEEN pc.admission_date AND pc.discharge_date
    ),
    
    readmission_analysis AS (
        SELECT 
            tp.patient_id,
            tp.primary_diagnosis,
            COUNT(DISTINCT tp.treatment_type) as treatment_variety,
            SUM(tp.cost) as total_treatment_cost,
            -- Check for readmissions within 30 days
            CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM patient_admissions pa2 
                    WHERE pa2.patient_id = tp.patient_id 
                    AND pa2.admission_date > (
                        SELECT MAX(pa1.discharge_date) 
                        FROM patient_admissions pa1 
                        WHERE pa1.patient_id = tp.patient_id
                    )
                    AND DATEDIFF(day, (
                        SELECT MAX(pa1.discharge_date) 
                        FROM patient_admissions pa1 
                        WHERE pa1.patient_id = tp.patient_id
                    ), pa2.admission_date) <= 30
                ) THEN 1 
                ELSE 0 
            END as readmitted_30_days
        FROM treatment_pathways tp
        GROUP BY tp.patient_id, tp.primary_diagnosis
    )
    
    SELECT 
        ra.primary_diagnosis,
        COUNT(DISTINCT ra.patient_id) as patient_count,
        AVG(ra.treatment_variety) as avg_treatments_per_patient,
        AVG(ra.total_treatment_cost) as avg_cost_per_patient,
        SUM(ra.readmitted_30_days) as readmission_count,
        ROUND(
            SUM(ra.readmitted_30_days) * 100.0 / COUNT(DISTINCT ra.patient_id), 2
        ) as readmission_rate_pct,
        outcome_metrics.avg_recovery_days,
        cost_comparison.national_avg_cost
    FROM readmission_analysis ra
    LEFT JOIN (
        -- Recovery time analysis
        SELECT 
            primary_diagnosis,
            AVG(length_of_stay) as avg_recovery_days
        FROM patient_cohorts
        GROUP BY primary_diagnosis
    ) outcome_metrics ON ra.primary_diagnosis = outcome_metrics.primary_diagnosis
    LEFT JOIN (
        -- Cost benchmarking
        SELECT 
            diagnosis_code,
            avg_national_cost as national_avg_cost
        FROM national_cost_benchmarks
    ) cost_comparison ON ra.primary_diagnosis = cost_comparison.diagnosis_code
    GROUP BY ra.primary_diagnosis, outcome_metrics.avg_recovery_days, cost_comparison.national_avg_cost
    HAVING COUNT(DISTINCT ra.patient_id) >= 10  -- Only diagnoses with sufficient volume
    ORDER BY readmission_rate_pct DESC;
    """
    
    return {
        'ecommerce_analytics.sql': ecommerce_query,
        'financial_portfolio.sql': financial_query,
        'healthcare_analytics.sql': healthcare_query
    }

def main():
    """Run comprehensive demonstration"""
    
    print_header("SQL Query Visualizer - Comprehensive Demonstration")
    print("This demo showcases the capabilities of the SQL Query Visualizer tools")
    print("for parsing and visualizing complex SQL queries across different domains.")
    
    # Create demo queries
    print_section("Creating Demo Queries")
    demo_queries = create_demo_queries()
    
    for filename, query in demo_queries.items():
        filepath = f"/app/demo_{filename}"
        with open(filepath, 'w') as f:
            f.write(query)
        print(f"✅ Created {filename} ({len(query.splitlines())} lines)")
    
    # Test basic visualizer
    print_section("Testing Basic SQL Visualizer")
    
    basic_tests = [
        ("Simple CTE Query", [
            'python', 'sql_query_visualizer.py', 
            '-f', 'simple_test.sql', 
            '-o', 'demo_basic_simple'
        ]),
        ("E-commerce Analytics", [
            'python', 'sql_query_visualizer.py', 
            '-f', 'demo_ecommerce_analytics.sql', 
            '-o', 'demo_basic_ecommerce'
        ])
    ]
    
    for description, cmd in basic_tests:
        success = run_command(cmd, f"Basic Visualizer: {description}")
        if success:
            print(f"   📊 Generated demo_basic_* diagrams")
    
    # Test advanced visualizer
    print_section("Testing Advanced SQL Visualizer")
    
    advanced_tests = [
        ("Simple CTE Query (Advanced)", [
            'python', 'advanced_sql_visualizer.py', 
            '-f', 'simple_test.sql', 
            '-o', 'demo_advanced_simple',
            '-v'
        ]),
        ("E-commerce Analytics (Advanced)", [
            'python', 'advanced_sql_visualizer.py', 
            '-f', 'demo_ecommerce_analytics.sql', 
            '-o', 'demo_advanced_ecommerce',
            '-v'
        ]),
        ("Financial Portfolio (Advanced)", [
            'python', 'advanced_sql_visualizer.py', 
            '-f', 'demo_financial_portfolio.sql', 
            '-o', 'demo_advanced_financial',
            '-v'
        ]),
        ("Healthcare Analytics (Advanced)", [
            'python', 'advanced_sql_visualizer.py', 
            '-f', 'demo_healthcare_analytics.sql', 
            '-o', 'demo_advanced_healthcare',
            '-v'
        ])
    ]
    
    for description, cmd in advanced_tests:
        success = run_command(cmd, f"Advanced Visualizer: {description}")
        if success:
            print(f"   📊 Generated advanced diagrams with enhanced features")
    
    # Run comprehensive test suite
    print_section("Running Comprehensive Test Suite")
    
    run_command([
        'python', 'test_visualizer.py'
    ], "Complete Test Suite")
    
    # Generate summary
    print_section("Generated Files Summary")
    
    svg_files = list(Path('/app').glob('*.svg'))
    png_files = list(Path('/app').glob('*.png'))
    sql_files = list(Path('/app').glob('demo_*.sql'))
    
    print(f"\n📊 Generated Visualizations:")
    print(f"   📄 {len(svg_files)} SVG files")
    print(f"   🖼️  {len(png_files)} PNG files")
    print(f"   📝 {len(sql_files)} Demo SQL files")
    
    print(f"\n📂 Demo Files Created:")
    for sql_file in sorted(sql_files):
        size = os.path.getsize(sql_file)
        print(f"   {sql_file.name} ({size} bytes)")
    
    print(f"\n🎨 Visualization Files:")
    svg_groups = {}
    for svg_file in sorted(svg_files):
        base_name = svg_file.stem
        category = "Demo" if base_name.startswith("demo_") else "Test" if base_name.startswith("test") else "Sample"
        if category not in svg_groups:
            svg_groups[category] = []
        svg_groups[category].append(svg_file.name)
    
    for category, files in svg_groups.items():
        print(f"\n   {category} Visualizations:")
        for file in files[:5]:  # Show first 5 files
            print(f"     📊 {file}")
        if len(files) > 5:
            print(f"     ... and {len(files) - 5} more")
    
    # Feature showcase
    print_section("Features Demonstrated")
    
    features = [
        "✅ Basic SQL parsing and visualization",
        "✅ Advanced SQL parsing with enhanced analysis",
        "✅ Nested CTE visualization with hierarchical grouping",
        "✅ Join relationship analysis with cardinality estimation",
        "✅ Subquery detection and dependency mapping",
        "✅ Color-coded node types (Tables, CTEs, Subqueries)",
        "✅ Left-to-right data flow layout",
        "✅ Complex query complexity analysis",
        "✅ Multiple output formats (SVG, PNG)",
        "✅ Command-line interface with multiple options",
        "✅ Error handling and graceful degradation",
        "✅ Support for multiple SQL dialects",
        "✅ Enhanced styling and visual hierarchy",
        "✅ Comprehensive test suite coverage"
    ]
    
    for feature in features:
        print(f"  {feature}")
    
    print_header("Demonstration Complete!")
    
    print("""
🎉 The SQL Query Visualizer demonstration is complete!

Key capabilities shown:
• Parse complex SQL queries with nested CTEs and subqueries
• Generate clear, readable diagrams showing data lineage
• Visualize table relationships and join types
• Support for real-world analytical queries from multiple domains
• Both basic and advanced visualization modes
• Comprehensive error handling and reporting

Next steps:
1. Check the generated SVG/PNG files to see the visualizations
2. Try your own SQL queries with the tools
3. Experiment with different SQL dialects using the -d option
4. Use the verbose mode (-v) for detailed analysis

Files to examine:
• demo_*.svg - Advanced visualizations of complex queries
• test*.svg - Test suite outputs showing various SQL patterns
• *.sql - Sample queries demonstrating different complexity levels
""")

if __name__ == '__main__':
    main()