-- Sample Complex SQL Query with Nested CTEs and Joins
-- This demonstrates multiple levels of CTEs, various join types, and complex relationships

WITH 
-- Level 1 CTEs
customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.registration_date,
        COUNT(o.order_id) as total_orders,
        SUM(o.total_amount) as total_spent,
        AVG(o.total_amount) as avg_order_value
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.registration_date >= '2023-01-01'
    GROUP BY c.customer_id, c.customer_name, c.registration_date
),

product_performance AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category_id,
        SUM(oi.quantity) as total_sold,
        SUM(oi.quantity * oi.unit_price) as total_revenue,
        COUNT(DISTINCT oi.order_id) as order_frequency
    FROM products p
    INNER JOIN order_items oi ON p.product_id = oi.product_id
    INNER JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_date >= '2023-01-01'
    GROUP BY p.product_id, p.product_name, p.category_id
),

-- Level 2 CTEs (nested dependencies)
high_value_customers AS (
    SELECT 
        customer_id,
        customer_name,
        total_spent,
        total_orders,
        CASE 
            WHEN total_spent > 10000 THEN 'VIP'
            WHEN total_spent > 5000 THEN 'Premium'
            ELSE 'Standard'
        END as customer_tier
    FROM customer_metrics
    WHERE total_spent > 1000
),

top_products AS (
    SELECT 
        pp.product_id,
        pp.product_name,
        pp.total_revenue,
        pp.total_sold,
        c.category_name,
        ROW_NUMBER() OVER (PARTITION BY pp.category_id ORDER BY pp.total_revenue DESC) as rank_in_category
    FROM product_performance pp
    INNER JOIN categories c ON pp.category_id = c.category_id
),

-- Level 3 CTE (depends on Level 2)
customer_product_affinity AS (
    SELECT 
        hvc.customer_id,
        hvc.customer_name,
        hvc.customer_tier,
        tp.product_name,
        tp.category_name,
        SUM(oi.quantity) as purchased_quantity,
        SUM(oi.quantity * oi.unit_price) as product_revenue_from_customer
    FROM high_value_customers hvc
    INNER JOIN orders o ON hvc.customer_id = o.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN top_products tp ON oi.product_id = tp.product_id
    WHERE tp.rank_in_category <= 5  -- Top 5 products per category
    GROUP BY hvc.customer_id, hvc.customer_name, hvc.customer_tier, tp.product_name, tp.category_name
)

-- Main query with multiple subqueries and complex joins
SELECT 
    cpa.customer_name,
    cpa.customer_tier,
    cpa.category_name,
    cpa.product_name,
    cpa.purchased_quantity,
    cpa.product_revenue_from_customer,
    regional_stats.avg_category_revenue,
    regional_stats.region_name,
    seasonal_data.peak_season
FROM customer_product_affinity cpa
LEFT JOIN (
    -- Subquery for regional statistics
    SELECT 
        c.customer_id,
        r.region_name,
        cat.category_name,
        AVG(oi.quantity * oi.unit_price) as avg_category_revenue
    FROM customers c
    INNER JOIN regions r ON c.region_id = r.region_id
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    INNER JOIN categories cat ON p.category_id = cat.category_id
    GROUP BY c.customer_id, r.region_name, cat.category_name
) regional_stats ON cpa.customer_id = regional_stats.customer_id 
                 AND cpa.category_name = regional_stats.category_name
LEFT JOIN (
    -- Subquery for seasonal analysis
    SELECT 
        p.product_name,
        CASE 
            WHEN EXTRACT(MONTH FROM o.order_date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM o.order_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM o.order_date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END as season,
        SUM(oi.quantity) as seasonal_quantity,
        ROW_NUMBER() OVER (PARTITION BY p.product_name ORDER BY SUM(oi.quantity) DESC) as season_rank
    FROM products p
    INNER JOIN order_items oi ON p.product_id = oi.product_id
    INNER JOIN orders o ON oi.order_id = o.order_id
    GROUP BY p.product_name, 
             CASE 
                 WHEN EXTRACT(MONTH FROM o.order_date) IN (12, 1, 2) THEN 'Winter'
                 WHEN EXTRACT(MONTH FROM o.order_date) IN (3, 4, 5) THEN 'Spring'
                 WHEN EXTRACT(MONTH FROM o.order_date) IN (6, 7, 8) THEN 'Summer'
                 ELSE 'Fall'
             END
) seasonal_data ON cpa.product_name = seasonal_data.product_name 
                AND seasonal_data.season_rank = 1
WHERE cpa.purchased_quantity > 0
ORDER BY cpa.customer_tier, cpa.product_revenue_from_customer DESC;
