-- 1. Marcas con mayores ingresos
SELECT p.brand_name, SUM(f.line_subtotal_amount) AS ingresos
FROM techstore_gold.fact_sales f
JOIN techstore_gold.dim_product p ON f.product_sk = p.product_sk
WHERE f.sale_status = 'COMPLETADA'
GROUP BY p.brand_name ORDER BY ingresos DESC LIMIT 10;

-- 2. Productos con mayor rotación
SELECT p.product_name, SUM(f.quantity) AS unidades
FROM techstore_gold.fact_sales f
JOIN techstore_gold.dim_product p ON f.product_sk = p.product_sk
WHERE f.sale_status = 'COMPLETADA'
GROUP BY p.product_name ORDER BY unidades DESC LIMIT 10;

-- 3. Sucursal que más vende computadores
SELECT b.branch_name, SUM(f.quantity) AS computadores
FROM techstore_gold.fact_sales f
JOIN techstore_gold.dim_product p ON f.product_sk = p.product_sk
JOIN techstore_gold.dim_branch b ON f.branch_sk = b.branch_sk
WHERE p.category = 'Computadores' AND f.sale_status = 'COMPLETADA'
GROUP BY b.branch_name ORDER BY computadores DESC LIMIT 10;

-- 4. Dinero perdido por devoluciones
SELECT status, COUNT(*) AS devoluciones, SUM(refund_amount) AS reembolsado
FROM techstore_gold.fact_returns
GROUP BY status ORDER BY reembolsado DESC;

-- 5. Productos con mayor margen
SELECT p.product_name,
       SUM(f.line_subtotal_amount - f.quantity * p.acquisition_cost) AS margen
FROM techstore_gold.fact_sales f
JOIN techstore_gold.dim_product p ON f.product_sk = p.product_sk
WHERE f.sale_status = 'COMPLETADA'
GROUP BY p.product_name ORDER BY margen DESC LIMIT 10;