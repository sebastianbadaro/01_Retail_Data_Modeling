-- =====================================================================================
-- 02_load_full_historical.sql
-- Objetivo:
--   Cargar TODO el histórico desde OLTP hacia el modelo dimensional (full load).
--   Incluye:
--     - dim_date (con unknown)
--     - dim_customer (SCD2 inicial)
--     - dim_product  (SCD2 inicial, denormalizada)
--     - dim_employee (SCD1 inicial)
--     - dim_shipper  (SCD1 inicial)
--     - fact_sales_line (todas las líneas de pedidos)
--   Además:
--     - Inicializa watermarks y secuencias de SK.
--
-- Notas didácticas:
--   - effective_from inicial se setea a '1900-01-01' para que TODAS las ventas históricas
--     encuentren una versión válida de la dimensión (as-of join por fecha del evento).
-- =====================================================================================

ATTACH 'data/oltp/northwind_oltp.duckdb' AS oltp;

-- Limpieza para rehacer el DW desde cero (idempotencia del aprendizaje)
DELETE FROM dw.fact_sales_line;
DELETE FROM dw.dim_shipper;
DELETE FROM dw.dim_employee;
DELETE FROM dw.dim_product;
DELETE FROM dw.dim_customer;
DELETE FROM dw.dim_date;

DELETE FROM ctl.watermark;
DELETE FROM ctl.surrogate_key_seq;

-- -------------------------------------------------------------------------------------
-- 1) Dimensión Fecha (dim_date)
-- -------------------------------------------------------------------------------------

-- 1.1) Insertar unknown member (date_sk = 0)
INSERT INTO dw.dim_date
VALUES (0, DATE '1900-01-01', NULL, NULL, NULL, 'Unknown', 'Unknown', NULL, NULL, NULL);

-- 1.2) Generar rango de fechas basado en Orders
--     (plus 365 días para cubrir futuras inserciones didácticas)
WITH bounds AS (
  SELECT
    CAST(MIN(OrderDate) AS DATE) AS min_date,
    CAST(MAX(OrderDate) AS DATE) AS max_date
  FROM oltp.Orders
),
dates AS (
  SELECT
    generate_series(min_date, max_date + INTERVAL 365 DAY, INTERVAL 1 DAY) AS d
  FROM bounds
)
INSERT INTO dw.dim_date
SELECT
  (EXTRACT(YEAR FROM d)::INTEGER * 10000
   + EXTRACT(MONTH FROM d)::INTEGER * 100
   + EXTRACT(DAY FROM d)::INTEGER) AS date_sk,
  d AS date,
  EXTRACT(YEAR FROM d)::INTEGER AS year,
  EXTRACT(MONTH FROM d)::INTEGER AS month,
  EXTRACT(DAY FROM d)::INTEGER AS day,
  strftime(d, '%B') AS month_name,
  strftime(d, '%A') AS day_name,
  strftime(d, '%W')::INTEGER AS week_of_year,
  ((EXTRACT(MONTH FROM d)::INTEGER - 1) / 3 + 1) AS quarter,
  (strftime(d, '%w') IN ('0','6')) AS is_weekend
FROM dates
WHERE d IS NOT NULL;

-- -------------------------------------------------------------------------------------
-- 2) Dimensión Customer (SCD2) - carga inicial
-- -------------------------------------------------------------------------------------

-- 2.1) Unknown member (customer_sk = 0)
INSERT INTO dw.dim_customer (
  customer_sk, customer_id, company_name, contact_name, contact_title, address, city, region,
  postal_code, country, phone, fax,
  effective_from, effective_to, is_current, record_hash
)
VALUES (
  0, 'UNKNOWN', 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  DATE '1900-01-01', DATE '9999-12-31', TRUE, NULL
);

-- 2.2) Staging con hashdiff (detección de cambios)
CREATE OR REPLACE TABLE stg.customer_src AS
SELECT
  CustomerID AS customer_id,
  CompanyName AS company_name,
  ContactName AS contact_name,
  ContactTitle AS contact_title,
  Address AS address,
  City AS city,
  Region AS region,
  PostalCode AS postal_code,
  Country AS country,
  Phone AS phone,
  Fax AS fax,
  md5(
    coalesce(CustomerID,'') || '|' ||
    coalesce(CompanyName,'') || '|' ||
    coalesce(ContactName,'') || '|' ||
    coalesce(ContactTitle,'') || '|' ||
    coalesce(Address,'') || '|' ||
    coalesce(City,'') || '|' ||
    coalesce(Region,'') || '|' ||
    coalesce(PostalCode,'') || '|' ||
    coalesce(Country,'') || '|' ||
    coalesce(Phone,'') || '|' ||
    coalesce(Fax,'')
  ) AS record_hash
FROM oltp.Customers;

-- 2.3) Insert inicial (surrogate keys asignadas por row_number + 1)
INSERT INTO dw.dim_customer
SELECT
  row_number() OVER (ORDER BY customer_id) AS customer_sk,   -- arranca en 1
  customer_id, company_name, contact_name, contact_title, address, city, region,
  postal_code, country, phone, fax,
  DATE '1900-01-01' AS effective_from,
  DATE '9999-12-31' AS effective_to,
  TRUE AS is_current,
  record_hash
FROM stg.customer_src;

-- 2.4) Inicializar secuencia para customer
INSERT INTO ctl.surrogate_key_seq(dim_name, next_sk)
SELECT 'customer', (SELECT max(customer_sk) + 1 FROM dw.dim_customer);

-- -------------------------------------------------------------------------------------
-- 3) Dimensión Product (SCD2, denormalizada) - carga inicial
-- -------------------------------------------------------------------------------------

-- 3.1) Unknown member (product_sk = 0)
INSERT INTO dw.dim_product (
  product_sk, product_id, product_name, quantity_per_unit, list_unit_price,
  units_in_stock, units_on_order, reorder_level, discontinued,
  category_id, category_name, supplier_id, supplier_name, supplier_country,
  effective_from, effective_to, is_current, record_hash
)
VALUES (
  0, -1, 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  DATE '1900-01-01', DATE '9999-12-31', TRUE, NULL
);

-- 3.2) Staging (join a Categories y Suppliers para denormalizar)
CREATE OR REPLACE TABLE stg.product_src AS
SELECT
  p.ProductID AS product_id,
  p.ProductName AS product_name,
  p.QuantityPerUnit AS quantity_per_unit,
  CAST(p.UnitPrice AS DOUBLE) AS list_unit_price,
  p.UnitsInStock AS units_in_stock,
  p.UnitsOnOrder AS units_on_order,
  p.ReorderLevel AS reorder_level,
  (p.Discontinued = 1) AS discontinued,

  c.CategoryID AS category_id,
  c.CategoryName AS category_name,

  s.SupplierID AS supplier_id,
  s.CompanyName AS supplier_name,
  s.Country AS supplier_country,

  md5(
    coalesce(cast(p.ProductID as VARCHAR),'') || '|' ||
    coalesce(p.ProductName,'') || '|' ||
    coalesce(p.QuantityPerUnit,'') || '|' ||
    coalesce(cast(p.UnitPrice as VARCHAR),'') || '|' ||
    coalesce(cast(p.UnitsInStock as VARCHAR),'') || '|' ||
    coalesce(cast(p.UnitsOnOrder as VARCHAR),'') || '|' ||
    coalesce(cast(p.ReorderLevel as VARCHAR),'') || '|' ||
    coalesce(cast(p.Discontinued as VARCHAR),'') || '|' ||
    coalesce(cast(c.CategoryID as VARCHAR),'') || '|' ||
    coalesce(c.CategoryName,'') || '|' ||
    coalesce(cast(s.SupplierID as VARCHAR),'') || '|' ||
    coalesce(s.CompanyName,'') || '|' ||
    coalesce(s.Country,'')
  ) AS record_hash
FROM oltp.Products p
LEFT JOIN oltp.Categories c ON c.CategoryID = p.CategoryID
LEFT JOIN oltp.Suppliers  s ON s.SupplierID = p.SupplierID;

-- 3.3) Insert inicial con surrogate keys por row_number (arranca en 1)
INSERT INTO dw.dim_product
SELECT
  row_number() OVER (ORDER BY product_id) AS product_sk,
  product_id, product_name, quantity_per_unit, list_unit_price,
  units_in_stock, units_on_order, reorder_level, discontinued,
  category_id, category_name, supplier_id, supplier_name, supplier_country,
  DATE '1900-01-01' AS effective_from,
  DATE '9999-12-31' AS effective_to,
  TRUE AS is_current,
  record_hash
FROM stg.product_src;

-- 3.4) Inicializar secuencia para product
INSERT INTO ctl.surrogate_key_seq(dim_name, next_sk)
SELECT 'product', (SELECT max(product_sk) + 1 FROM dw.dim_product);

-- -------------------------------------------------------------------------------------
-- 4) Dimensión Employee (SCD1) - carga inicial
-- -------------------------------------------------------------------------------------

-- Unknown member
INSERT INTO dw.dim_employee
VALUES (0, -1, 'Unknown', 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL);

CREATE OR REPLACE TABLE stg.employee_src AS
SELECT
  EmployeeID AS employee_id,
  FirstName AS first_name,
  LastName AS last_name,
  Title AS title,
  City AS city,
  Region AS region,
  Country AS country,
  CAST(HireDate AS DATE) AS hire_date,
  CAST(BirthDate AS DATE) AS birth_date,
  md5(
    coalesce(cast(EmployeeID as VARCHAR),'') || '|' ||
    coalesce(FirstName,'') || '|' ||
    coalesce(LastName,'') || '|' ||
    coalesce(Title,'') || '|' ||
    coalesce(City,'') || '|' ||
    coalesce(Region,'') || '|' ||
    coalesce(Country,'') || '|' ||
    coalesce(cast(HireDate as VARCHAR),'') || '|' ||
    coalesce(cast(BirthDate as VARCHAR),'')
  ) AS record_hash
FROM oltp.Employees;

INSERT INTO dw.dim_employee
SELECT
  row_number() OVER (ORDER BY employee_id) AS employee_sk,
  employee_id, first_name, last_name, title, city, region, country, hire_date, birth_date, record_hash
FROM stg.employee_src;

INSERT INTO ctl.surrogate_key_seq(dim_name, next_sk)
SELECT 'employee', (SELECT max(employee_sk) + 1 FROM dw.dim_employee);

-- -------------------------------------------------------------------------------------
-- 5) Dimensión Shipper (SCD1) - carga inicial
-- -------------------------------------------------------------------------------------

INSERT INTO dw.dim_shipper VALUES (0, -1, 'Unknown', NULL, NULL);

CREATE OR REPLACE TABLE stg.shipper_src AS
SELECT
  ShipperID AS shipper_id,
  CompanyName AS company_name,
  Phone AS phone,
  md5(
    coalesce(cast(ShipperID as VARCHAR),'') || '|' ||
    coalesce(CompanyName,'') || '|' ||
    coalesce(Phone,'')
  ) AS record_hash
FROM oltp.Shippers;

INSERT INTO dw.dim_shipper
SELECT
  row_number() OVER (ORDER BY shipper_id) AS shipper_sk,
  shipper_id, company_name, phone, record_hash
FROM stg.shipper_src;

INSERT INTO ctl.surrogate_key_seq(dim_name, next_sk)
SELECT 'shipper', (SELECT max(shipper_sk) + 1 FROM dw.dim_shipper);

-- -------------------------------------------------------------------------------------
-- 6) Fact sales line - carga histórica
-- -------------------------------------------------------------------------------------

-- Staging de líneas (join Orders + Order Details)
CREATE OR REPLACE TABLE stg.sales_line_src AS
SELECT
  o.OrderID AS order_id,
  od.ProductID AS product_id,
  o.CustomerID AS customer_id,
  o.EmployeeID AS employee_id,
  o.ShipVia AS ship_via,
  CAST(o.OrderDate AS DATE) AS order_date,
  CAST(o.RequiredDate AS DATE) AS required_date,
  CAST(o.ShippedDate AS DATE) AS shipped_date,
  CAST(o.Freight AS DOUBLE) AS freight,

  CAST(od.Quantity AS BIGINT) AS quantity,
  CAST(od.UnitPrice AS DOUBLE) AS unit_price,
  CAST(od.Discount AS DOUBLE) AS discount
FROM oltp.Orders o
JOIN oltp."Order Details" od
  ON od.OrderID = o.OrderID;

-- Insert a fact con asignación de SKs (as-of join en dims SCD2 por fecha del evento)
INSERT INTO dw.fact_sales_line
SELECT
  s.order_id,
  s.product_id,
  row_number() OVER (PARTITION BY s.order_id ORDER BY s.product_id) AS line_number,

  coalesce(dc.customer_sk, 0) AS customer_sk,
  coalesce(dp.product_sk, 0) AS product_sk,
  coalesce(de.employee_sk, 0) AS employee_sk,
  coalesce(ds.shipper_sk, 0) AS shipper_sk,

  coalesce(ddo.date_sk, 0) AS order_date_sk,
  coalesce(ddr.date_sk, 0) AS required_date_sk,
  coalesce(dds.date_sk, 0) AS shipped_date_sk,

  s.quantity,
  s.unit_price,
  s.discount,

  (s.quantity * s.unit_price) AS gross_amount,
  (s.quantity * s.unit_price * (1 - s.discount)) AS net_amount,

  s.freight AS order_freight,

  -- Freight asignado proporcional a net_amount
  CASE
    WHEN sum(s.quantity * s.unit_price * (1 - s.discount)) OVER (PARTITION BY s.order_id) = 0 THEN 0
    ELSE s.freight * ( (s.quantity * s.unit_price * (1 - s.discount))
      / sum(s.quantity * s.unit_price * (1 - s.discount)) OVER (PARTITION BY s.order_id) )
  END AS freight_allocated,

  current_timestamp AS etl_loaded_at
FROM stg.sales_line_src s
LEFT JOIN dw.dim_customer dc
  ON dc.customer_id = s.customer_id
 AND dc.is_current = TRUE
 AND s.order_date BETWEEN dc.effective_from AND dc.effective_to
LEFT JOIN dw.dim_product dp
  ON dp.product_id = s.product_id
 AND dp.is_current = TRUE
 AND s.order_date BETWEEN dp.effective_from AND dp.effective_to
LEFT JOIN dw.dim_employee de
  ON de.employee_id = s.employee_id
LEFT JOIN dw.dim_shipper ds
  ON ds.shipper_id = s.ship_via
LEFT JOIN dw.dim_date ddo
  ON ddo.date = s.order_date
LEFT JOIN dw.dim_date ddr
  ON ddr.date = s.required_date
LEFT JOIN dw.dim_date dds
  ON dds.date = coalesce(s.shipped_date, DATE '1900-01-01'); -- shipped null -> unknown date row exists

-- -------------------------------------------------------------------------------------
-- 7) Watermark inicial (máximo OrderID cargado)
-- -------------------------------------------------------------------------------------

INSERT INTO ctl.watermark(entity, last_order_id, updated_at)
SELECT 'orders', (SELECT max(order_id) FROM dw.fact_sales_line), current_timestamp;

-- -------------------------------------------------------------------------------------
-- 8) Log
-- -------------------------------------------------------------------------------------
INSERT INTO ctl.etl_run_log
SELECT
  1 AS run_id,
  current_timestamp AS run_ts,
  'FULL' AS run_type,
  DATE '1900-01-01' AS effective_date,
  (SELECT count(*) FROM dw.dim_customer WHERE customer_sk <> 0) AS rows_dim_customer,
  (SELECT count(*) FROM dw.dim_product  WHERE product_sk  <> 0) AS rows_dim_product,
  (SELECT count(*) FROM dw.fact_sales_line) AS rows_fact_sales,
  'Full historical load completed' AS notes;
