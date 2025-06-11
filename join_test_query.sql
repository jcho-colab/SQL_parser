-- Test query with multiple join types and clear join conditions
SELECT 
    c.customer_name,
    c.email,
    o.order_date,
    o.total_amount,
    oi.quantity,
    p.product_name,
    p.price,
    cat.category_name,
    s.supplier_name
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
INNER JOIN products p ON oi.product_id = p.product_id
RIGHT JOIN categories cat ON p.category_id = cat.category_id
LEFT JOIN suppliers s ON p.supplier_id = s.supplier_id
WHERE c.active = 1
  AND o.order_date >= '2023-01-01';