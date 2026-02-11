-- =====================================================================================
-- 99_validation_queries.sql (V2)
--
-- OBJETIVO
--   Validaciones rápidas + checks de SCD2 para asegurar que el DW quedó consistente.
-- =====================================================================================

-- 1) Conteos básicos
SELECT 'dw.dim_customer' AS table_name, count(*) AS rows FROM dw.dim_customer
UNION ALL SELECT 'dw.dim_product', count(*) FROM dw.dim_product
UNION ALL SELECT 'dw.dim_employee', count(*) FROM dw.dim_employee
UNION ALL SELECT 'dw.dim_shipper', count(*) FROM dw.dim_shipper
UNION ALL SELECT 'dw.dim_date', count(*) FROM dw.dim_date
UNION ALL SELECT 'dw.fact_sales_line', count(*) FROM dw.fact_sales_line;

-- 2) Watermark actual
SELECT * FROM ctl.watermark;

-- 3) Últimas ejecuciones ETL
SELECT * FROM ctl.etl_run_log ORDER BY run_id DESC LIMIT 10;

-- 4) SCD2: debe haber 1 fila current por business key
SELECT customer_id, count(*) AS current_rows
FROM dw.dim_customer
WHERE is_current = TRUE AND customer_sk <> 0
GROUP BY 1
HAVING count(*) <> 1;

SELECT product_id, count(*) AS current_rows
FROM dw.dim_product
WHERE is_current = TRUE AND product_sk <> 0
GROUP BY 1
HAVING count(*) <> 1;

-- 5) SCD2: no debería haber “solapamiento” de rangos por business key
--    (si esto da filas, tu lógica de effective_from/to está mal)
SELECT a.customer_id
FROM dw.dim_customer a
JOIN dw.dim_customer b
  ON a.customer_id = b.customer_id
 AND a.customer_sk <> b.customer_sk
 AND a.customer_sk <> 0 AND b.customer_sk <> 0
 AND a.effective_from <= b.effective_to
 AND b.effective_from <= a.effective_to
LIMIT 20;

-- 6) Integridad: ¿cuántas facts cayeron en Unknown?
SELECT
  sum(case when customer_sk=0 then 1 else 0 end) AS unknown_customer_rows,
  sum(case when product_sk=0 then 1 else 0 end) AS unknown_product_rows,
  sum(case when employee_sk=0 then 1 else 0 end) AS unknown_employee_rows,
  sum(case when shipper_sk=0 then 1 else 0 end) AS unknown_shipper_rows,
  sum(case when order_date_sk=0 then 1 else 0 end) AS unknown_order_date_rows
FROM dw.fact_sales_line;

-- 7) Reconciliación simple: net_amount total en DW vs cálculo directo desde OLTP
--    (sirve para confianza del modelo)
ATTACH 'data/oltp/northwind_oltp.duckdb' AS oltp;

SELECT
  (SELECT sum(net_amount) FROM dw.fact_sales_line) AS dw_net_sales,
  (SELECT
     sum(od.Quantity * od.UnitPrice * (1 - od.Discount))
   FROM oltp."Order Details" od
  ) AS oltp_net_sales;
