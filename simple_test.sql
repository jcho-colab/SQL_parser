-- Simple test query with one CTE and basic joins
WITH customer_orders AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        COUNT(o.order_id) as order_count,
        SUM(o.total_amount) as total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name
)

SELECT 
    co.customer_name,
    co.order_count,
    co.total_spent,
    p.product_name
FROM customer_orders co
INNER JOIN orders o ON co.customer_id = o.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id
WHERE co.total_spent > 1000;
