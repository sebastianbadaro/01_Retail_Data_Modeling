-- =====================================================================================
-- 99_validation_queries.sql
-- Objetivo: validaciones rápidas para confirmar que el modelo está consistente.
-- =====================================================================================

-- 1) Conteos básicos
SELECT 'dim_customer' AS table_name, count(*) AS rows FROM dw.dim_customer
UNION ALL
SELECT 'dim_product', count(*) FROM dw.dim_product
UNION ALL
SELECT 'dim_employee', count(*) FROM dw.dim_employee
UNION ALL
SELECT 'dim_shipper', count(*) FROM dw.dim_shipper
UNION ALL
SELECT 'dim_date', count(*) FROM dw.dim_date
UNION ALL
SELECT 'fact_sales_line', count(*) FROM dw.fact_sales_line;

-- 2) Watermark actual
SELECT * FROM ctl.watermark;

-- 3) Últimas ejecuciones ETL
SELECT * FROM ctl.etl_run_log ORDER BY run_id DESC LIMIT 10;

-- 4) Chequear integridad básica: ¿hay FKs 0 inesperados?
SELECT
  sum(case when customer_sk=0 then 1 else 0 end) AS unknown_customer_rows,
  sum(case when product_sk=0 then 1 else 0 end) AS unknown_product_rows,
  sum(case when employee_sk=0 then 1 else 0 end) AS unknown_employee_rows,
  sum(case when shipper_sk=0 then 1 else 0 end) AS unknown_shipper_rows,
  sum(case when order_date_sk=0 then 1 else 0 end) AS unknown_order_date_rows
FROM dw.fact_sales_line;

-- 5) Ejemplo BI: ventas netas por año/mes
SELECT
  d.year, d.month,
  sum(f.net_amount) AS net_sales,
  sum(f.quantity) AS units
FROM dw.fact_sales_line f
JOIN dw.dim_date d ON d.date_sk = f.order_date_sk
GROUP BY 1,2
ORDER BY 1,2;
