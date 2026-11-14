-- 6. Ingresos por mes (dim_date)
SELECT d.year, d.month, SUM(f.line_subtotal_amount) AS ingresos,
       COUNT(DISTINCT f.sale_id) AS ventas
FROM techstore_gold.fact_sales f
JOIN techstore_gold.dim_date d ON f.date_sk = d.date_sk
WHERE f.sale_status = 'COMPLETADA'
GROUP BY d.year, d.month ORDER BY d.year, d.month;

-- 7. Ingresos por método de pago (dim_payment_method)
SELECT pm.payment_method_name, SUM(f.line_subtotal_amount) AS ingresos
FROM techstore_gold.fact_sales f
JOIN techstore_gold.dim_payment_method pm ON f.payment_method_sk = pm.payment_method_sk
WHERE f.sale_status = 'COMPLETADA'
GROUP BY pm.payment_method_name ORDER BY ingresos DESC;

-- 8. Ingresos por departamento del cliente (dim_customer)
SELECT c.department, SUM(f.line_subtotal_amount) AS ingresos
FROM techstore_gold.fact_sales f
JOIN techstore_gold.dim_customer c ON f.customer_sk = c.customer_sk
WHERE f.sale_status = 'COMPLETADA'
GROUP BY c.department ORDER BY ingresos DESC LIMIT 10;

-- 9. Devoluciones por categoría (fact_returns + dim_product)
SELECT p.category, COUNT(*) AS devoluciones,
       SUM(r.returned_quantity) AS unidades, SUM(r.refund_amount) AS reembolsado
FROM techstore_gold.fact_returns r
JOIN techstore_gold.dim_product p ON r.product_sk = p.product_sk
GROUP BY p.category ORDER BY reembolsado DESC;

-- 10. Motivos de devolución más frecuentes (fact_returns)
SELECT reason, COUNT(*) AS casos, SUM(refund_amount) AS reembolsado
FROM techstore_gold.fact_returns
GROUP BY reason ORDER BY casos DESC;

-- 11. Marcas con más garantías y cuántas se aprueban (fact_warranties)
SELECT p.brand_name, COUNT(*) AS reclamos,
       SUM(CASE WHEN w.status = 'APROBADA' THEN 1 ELSE 0 END) AS aprobadas
FROM techstore_gold.fact_warranties w
JOIN techstore_gold.dim_product p ON w.product_sk = p.product_sk
GROUP BY p.brand_name ORDER BY reclamos DESC LIMIT 10;