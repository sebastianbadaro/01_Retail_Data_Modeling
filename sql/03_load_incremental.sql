-- =====================================================================================
-- 03_load_incremental.sql
-- Objetivo:
--   Ejecutar una carga INCREMENTAL "Kimball style":
--     1) Actualizar dimensiones (SCD2 para Customer/Product, SCD1 para Employee/Shipper)
--     2) Insertar nuevas líneas de pedidos a la fact usando watermark por OrderID
--
-- IMPORTANTÍSIMO (didáctico):
--   - Para SCD2 necesitamos una "effective_date" (fecha desde la cual aplica el cambio).
--   - En sistemas reales viene de la fuente (CDC/timestamps). En Northwind no existe, así que
--     lo simulamos con un parámetro controlado.
--
-- Cambiá la fecha de abajo si querés repetir simulaciones.
-- =====================================================================================

ATTACH 'data/oltp/northwind_oltp.duckdb' AS oltp;

-- -------------------------------------------------------------------------
-- 0) Parámetro del run
-- -------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE _params AS
SELECT DATE '1998-05-07' AS effective_date;  -- <-- editar para otros escenarios

-- Run id = max(run_id)+1
CREATE OR REPLACE TEMP TABLE _run AS
SELECT coalesce(max(run_id),0) + 1 AS run_id FROM ctl.etl_run_log;

-- -------------------------------------------------------------------------
-- 1) STAGING: re-leer fuentes OLTP y recalcular hash (en producción sería CDC)
-- -------------------------------------------------------------------------

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

-- Employee / Shipper (SCD1)
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

-- -------------------------------------------------------------------------
-- 2) DIM CUSTOMER (SCD2)
-- -------------------------------------------------------------------------

-- 2.1) Detectar nuevos o cambiados comparando contra el registro current
CREATE OR REPLACE TEMP TABLE _customer_delta AS
SELECT s.*
FROM stg.customer_src s
LEFT JOIN dw.dim_customer d
  ON d.customer_id = s.customer_id
 AND d.is_current = TRUE
WHERE d.customer_id IS NULL
   OR d.record_hash <> s.record_hash;

-- 2.2) Cerrar versiones actuales que cambiaron (solo donde existe current)
UPDATE dw.dim_customer d
SET
  effective_to = (SELECT effective_date FROM _params) - INTERVAL 1 DAY,
  is_current = FALSE
WHERE d.is_current = TRUE
  AND EXISTS (
    SELECT 1
    FROM _customer_delta x
    WHERE x.customer_id = d.customer_id
      AND x.record_hash <> d.record_hash
  );

-- 2.3) Insertar nuevas versiones + nuevos customers
--     Asignación de surrogate keys usando ctl.surrogate_key_seq (Kimball)
WITH seq AS (
  SELECT next_sk AS start_sk FROM ctl.surrogate_key_seq WHERE dim_name = 'customer'
),
to_insert AS (
  SELECT * FROM _customer_delta ORDER BY customer_id
),
numbered AS (
  SELECT
    (SELECT start_sk FROM seq) + row_number() OVER (ORDER BY customer_id) - 1 AS customer_sk,
    customer_id, company_name, contact_name, contact_title, address, city, region,
    postal_code, country, phone, fax,
    (SELECT effective_date FROM _params) AS effective_from,
    DATE '9999-12-31' AS effective_to,
    TRUE AS is_current,
    record_hash
  FROM to_insert
)
INSERT INTO dw.dim_customer
SELECT * FROM numbered;

-- 2.4) Avanzar secuencia
UPDATE ctl.surrogate_key_seq
SET next_sk = next_sk + (SELECT count(*) FROM _customer_delta)
WHERE dim_name = 'customer';

-- -------------------------------------------------------------------------
-- 3) DIM PRODUCT (SCD2)
-- -------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE _product_delta AS
SELECT s.*
FROM stg.product_src s
LEFT JOIN dw.dim_product d
  ON d.product_id = s.product_id
 AND d.is_current = TRUE
WHERE d.product_id IS NULL
   OR d.record_hash <> s.record_hash;

UPDATE dw.dim_product d
SET
  effective_to = (SELECT effective_date FROM _params) - INTERVAL 1 DAY,
  is_current = FALSE
WHERE d.is_current = TRUE
  AND EXISTS (
    SELECT 1
    FROM _product_delta x
    WHERE x.product_id = d.product_id
      AND x.record_hash <> d.record_hash
  );

WITH seq AS (
  SELECT next_sk AS start_sk FROM ctl.surrogate_key_seq WHERE dim_name = 'product'
),
to_insert AS (
  SELECT * FROM _product_delta ORDER BY product_id
),
numbered AS (
  SELECT
    (SELECT start_sk FROM seq) + row_number() OVER (ORDER BY product_id) - 1 AS product_sk,
    product_id, product_name, quantity_per_unit, list_unit_price,
    units_in_stock, units_on_order, reorder_level, discontinued,
    category_id, category_name, supplier_id, supplier_name, supplier_country,
    (SELECT effective_date FROM _params) AS effective_from,
    DATE '9999-12-31' AS effective_to,
    TRUE AS is_current,
    record_hash
  FROM to_insert
)
INSERT INTO dw.dim_product
SELECT * FROM numbered;

UPDATE ctl.surrogate_key_seq
SET next_sk = next_sk + (SELECT count(*) FROM _product_delta)
WHERE dim_name = 'product';

-- -------------------------------------------------------------------------
-- 4) DIM EMPLOYEE (SCD1 overwrite)
-- -------------------------------------------------------------------------
-- Para SCD1: si existe employee_id, actualizamos atributos; si no existe, insert.
-- (En DuckDB usamos un patrón MERGE-like: UPDATE + INSERT NOT EXISTS)

-- 4.1) Update de los existentes cuyo hash cambió
UPDATE dw.dim_employee d
SET
  first_name = s.first_name,
  last_name  = s.last_name,
  title      = s.title,
  city       = s.city,
  region     = s.region,
  country    = s.country,
  hire_date  = s.hire_date,
  birth_date = s.birth_date,
  record_hash = s.record_hash
FROM stg.employee_src s
WHERE d.employee_id = s.employee_id
  AND d.employee_sk <> 0
  AND d.record_hash <> s.record_hash;

-- 4.2) Insert de nuevos employees (si aparecieran)
CREATE OR REPLACE TEMP TABLE _employee_new AS
SELECT s.*
FROM stg.employee_src s
LEFT JOIN dw.dim_employee d ON d.employee_id = s.employee_id
WHERE d.employee_id IS NULL;

WITH seq AS (SELECT next_sk AS start_sk FROM ctl.surrogate_key_seq WHERE dim_name='employee'),
numbered AS (
  SELECT
    (SELECT start_sk FROM seq) + row_number() OVER (ORDER BY employee_id) - 1 AS employee_sk,
    employee_id, first_name, last_name, title, city, region, country, hire_date, birth_date, record_hash
  FROM _employee_new
)
INSERT INTO dw.dim_employee
SELECT * FROM numbered;

UPDATE ctl.surrogate_key_seq
SET next_sk = next_sk + (SELECT count(*) FROM _employee_new)
WHERE dim_name='employee';

-- -------------------------------------------------------------------------
-- 5) DIM SHIPPER (SCD1 overwrite)
-- -------------------------------------------------------------------------
UPDATE dw.dim_shipper d
SET
  company_name = s.company_name,
  phone = s.phone,
  record_hash = s.record_hash
FROM stg.shipper_src s
WHERE d.shipper_id = s.shipper_id
  AND d.shipper_sk <> 0
  AND d.record_hash <> s.record_hash;

CREATE OR REPLACE TEMP TABLE _shipper_new AS
SELECT s.*
FROM stg.shipper_src s
LEFT JOIN dw.dim_shipper d ON d.shipper_id = s.shipper_id
WHERE d.shipper_id IS NULL;

WITH seq AS (SELECT next_sk AS start_sk FROM ctl.surrogate_key_seq WHERE dim_name='shipper'),
numbered AS (
  SELECT
    (SELECT start_sk FROM seq) + row_number() OVER (ORDER BY shipper_id) - 1 AS shipper_sk,
    shipper_id, company_name, phone, record_hash
  FROM _shipper_new
)
INSERT INTO dw.dim_shipper
SELECT * FROM numbered;

UPDATE ctl.surrogate_key_seq
SET next_sk = next_sk + (SELECT count(*) FROM _shipper_new)
WHERE dim_name='shipper';

-- -------------------------------------------------------------------------
-- 6) FACT incremental: nuevas órdenes por watermark (OrderID)
-- -------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE _wm AS
SELECT last_order_id FROM ctl.watermark WHERE entity='orders';

-- Traer nuevas líneas (Orders + Order Details) para OrderID > watermark
CREATE OR REPLACE TEMP TABLE _new_sales_lines AS
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
JOIN oltp."Order Details" od ON od.OrderID = o.OrderID
WHERE o.OrderID > (SELECT last_order_id FROM _wm);

-- Insert evitando duplicados (idempotente)
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

  CASE
    WHEN sum(s.quantity * s.unit_price * (1 - s.discount)) OVER (PARTITION BY s.order_id) = 0 THEN 0
    ELSE s.freight * ( (s.quantity * s.unit_price * (1 - s.discount))
      / sum(s.quantity * s.unit_price * (1 - s.discount)) OVER (PARTITION BY s.order_id) )
  END AS freight_allocated,

  current_timestamp AS etl_loaded_at
FROM _new_sales_lines s
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
  ON dds.date = coalesce(s.shipped_date, DATE '1900-01-01')
WHERE NOT EXISTS (
  SELECT 1 FROM dw.fact_sales_line f
  WHERE f.order_id = s.order_id AND f.product_id = s.product_id
);

-- -------------------------------------------------------------------------
-- 7) Actualizar watermark
-- -------------------------------------------------------------------------
UPDATE ctl.watermark
SET
  last_order_id = coalesce((SELECT max(order_id) FROM dw.fact_sales_line), last_order_id),
  updated_at = current_timestamp
WHERE entity='orders';

-- -------------------------------------------------------------------------
-- 8) Log del run
-- -------------------------------------------------------------------------
INSERT INTO ctl.etl_run_log
SELECT
  (SELECT run_id FROM _run) AS run_id,
  current_timestamp AS run_ts,
  'INCREMENTAL' AS run_type,
  (SELECT effective_date FROM _params) AS effective_date,
  (SELECT count(*) FROM _customer_delta) AS rows_dim_customer,
  (SELECT count(*) FROM _product_delta) AS rows_dim_product,
  (SELECT count(*) FROM _new_sales_lines) AS rows_fact_sales,
  'Incremental load completed (SCD2 + new fact rows)' AS notes;
