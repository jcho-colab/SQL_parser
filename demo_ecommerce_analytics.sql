
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
    