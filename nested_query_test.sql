-- Complex nested query example
WITH customer_summary AS (
    SELECT 
        customer_id,
        customer_name,
        (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) as order_count
    FROM customers c
),
top_customers AS (
    SELECT 
        cs.customer_id,
        cs.customer_name,
        cs.order_count,
        (SELECT AVG(total_amount) 
         FROM orders o2 
         WHERE o2.customer_id = cs.customer_id) as avg_order
    FROM customer_summary cs
    WHERE cs.order_count > 5
)
SELECT 
    tc.customer_name,
    tc.order_count,
    tc.avg_order,
    regional_data.region_name
FROM top_customers tc
LEFT JOIN (
    SELECT 
        c.customer_id,
        r.region_name
    FROM customers c
    INNER JOIN regions r ON c.region_id = r.region_id
) regional_data ON tc.customer_id = regional_data.customer_id
WHERE tc.avg_order > 100;