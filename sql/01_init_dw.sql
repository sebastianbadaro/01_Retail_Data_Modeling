-- =====================================================================================
-- 01_init_dw.sql  
--
-- OBJETIVO
--   Crear el "Data Warehouse" (DW) vacío y autocontenido para el proyecto.
--   Definimos esquemas, tablas de control (watermarks / secuencias de SK), dimensiones
--   y la tabla de hechos principal.
--
-- INPUT
--   data/oltp/northwind_oltp.duckdb (OLTP: base operacional)
--
-- OUTPUT
--   (en el archivo data/dw/northwind_dw.duckdb)
--   - esquemas: ctl, stg, dw
--   - tablas de control: watermark, surrogate_key_seq, etl_run_log
--   - dimensiones: dim_date, dim_customer (SCD2), dim_product (SCD2), dim_employee (SCD1), dim_shipper (SCD1)
--   - hecho: fact_sales_line (grano: línea de pedido)
--
-- DECISIONES DE DISEÑO (Kimball) - por qué esto es "estrella" y no OLTP:
--
-- 1) Proceso de negocio
--    Elegimos "Ventas / Fulfillment": pedidos (Orders) + detalle (Order Details).
--    Esto define QUÉ historia cuenta el DW.
--
-- 2) Grano de la tabla de hechos (la decisión más importante)
--    1 fila en fact_sales_line = 1 línea de pedido.
--    En Northwind: Order Details tiene PK compuesta (OrderID, ProductID),
--    por eso nuestro grano es (order_id, product_id).
--    Ventaja: es el nivel más atómico disponible -> permite sumar y agrupar sin "doble conteo".
--
-- 3) Star schema (no snowflake)
--    En OLAP queremos joins simples y performance.
--    Por eso dim_product se DENORMALIZA con Category y Supplier dentro (evitamos joins extra en BI).
--
-- 4) Surrogate keys (SK) en dimensiones
--    Las SK son IDs "propias del DW". No son las claves del OLTP.
--    ¿Por qué?
--      - Aíslan al DW de cambios del OLTP.
--      - Habilitan SCD2: múltiples versiones del mismo negocio (CustomerID) con distintas SK.
--
-- 5) Unknown members (SK=0)
--    En BI es mejor tener "Unknown" que NULL:
--      - Evita problemas de joins
--      - Evita NULL en FKs
--      - Permite contar "qué quedó sin match" (calidad de datos)
--
-- 6) Role-playing dates
--    Un pedido tiene varias fechas (OrderDate, RequiredDate, ShippedDate).
--    En Kimball se usa UNA sola dim_date y se referencia con múltiples FKs:
--      order_date_sk, required_date_sk, shipped_date_sk
-- =====================================================================================

-- 0) Conectar al OLTP (lo usamos como fuente)
ATTACH 'data/oltp/northwind_oltp.duckdb' AS oltp;

-- 1) Crear esquemas (control / staging / data warehouse)
CREATE SCHEMA IF NOT EXISTS ctl;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;

-- 2) Tablas de control
-- 2.1) Watermark: guarda "hasta dónde cargamos" en incremental para hechos.
--      Elegimos OrderID como watermark porque en este dataset es simple y creciente.
--      (En producción suele ser timestamp/CDC/LSN/etc.)
CREATE TABLE IF NOT EXISTS ctl.watermark (
  entity         VARCHAR PRIMARY KEY,
  last_order_id  BIGINT,
  updated_at     TIMESTAMP
);

-- 2.2) Secuencia manual de surrogate keys (SK) por dimensión
--      (Kimball: las SK las controla el DW)
CREATE TABLE IF NOT EXISTS ctl.surrogate_key_seq (
  dim_name   VARCHAR PRIMARY KEY,
  next_sk    BIGINT
);

-- 2.3) Log de runs (para mostrar qué pasó y cuántas filas se movieron)
CREATE TABLE IF NOT EXISTS ctl.etl_run_log (
  run_id            BIGINT,
  run_ts            TIMESTAMP,
  run_type          VARCHAR,   -- FULL / INCREMENTAL
  effective_date    DATE,      -- fecha desde la cual aplican cambios SCD2 (ver incremental)
  rows_dim_customer BIGINT,
  rows_dim_product  BIGINT,
  rows_fact_sales   BIGINT,
  notes             VARCHAR
);

-- 3) DIMENSIONES

-- 3.1) Dimensión Fecha
--      - date_sk = YYYYMMDD (convención muy común)
--      - fila 0 = Unknown
CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_sk      INTEGER PRIMARY KEY,
  date         DATE,
  year         INTEGER,
  month        INTEGER,
  day          INTEGER,
  month_name   VARCHAR,
  day_name     VARCHAR,
  week_of_year INTEGER,
  quarter      INTEGER,
  is_weekend   BOOLEAN
);

-- 3.2) Dimensión Customer (SCD Tipo 2)
--      ¿Por qué SCD2 acá?
--      Porque si cambia City/Country/etc queremos preservar la historia:
--      "¿Cuánto vendimos cuando el cliente vivía en X?"
CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_sk    BIGINT PRIMARY KEY,
  customer_id    VARCHAR,  -- business key del OLTP (CustomerID)
  company_name   VARCHAR,
  contact_name   VARCHAR,
  contact_title  VARCHAR,
  address        VARCHAR,
  city           VARCHAR,
  region         VARCHAR,
  postal_code    VARCHAR,
  country        VARCHAR,
  phone          VARCHAR,
  fax            VARCHAR,

  -- Campos SCD2
  effective_from DATE,
  effective_to   DATE,
  is_current     BOOLEAN,

  -- Hash de atributos para detectar cambios (NO es la SK)
  record_hash    VARCHAR
);

-- 3.3) Dimensión Product (SCD Tipo 2, denormalizada)
--      ¿Por qué SCD2?
--      Para poder historizar cambios (precio lista, discontinuado, supplier/categoría, etc.)
--      ¿Por qué denormalizada?
--      Porque en BI preferimos 1 join (fact->dim_product) y listo.
CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_sk        BIGINT PRIMARY KEY,
  product_id        BIGINT, -- business key del OLTP (ProductID)
  product_name      VARCHAR,
  quantity_per_unit VARCHAR,
  list_unit_price   DOUBLE,
  units_in_stock    BIGINT,
  units_on_order    BIGINT,
  reorder_level     BIGINT,
  discontinued      BOOLEAN,

  -- Denormalización (evita snowflake)
  category_id       BIGINT,
  category_name     VARCHAR,
  supplier_id       BIGINT,
  supplier_name     VARCHAR,
  supplier_country  VARCHAR,

  -- Campos SCD2
  effective_from    DATE,
  effective_to      DATE,
  is_current        BOOLEAN,

  record_hash       VARCHAR
);

-- 3.4) Dimensión Employee (SCD Tipo 1)
--      En un proyecto inicial, SCD1 para empleados suele ser suficiente:
--      si cambia el título o ciudad, lo sobreescribimos.
CREATE TABLE IF NOT EXISTS dw.dim_employee (
  employee_sk   BIGINT PRIMARY KEY,
  employee_id   BIGINT, -- business key del OLTP (EmployeeID)
  first_name    VARCHAR,
  last_name     VARCHAR,
  title         VARCHAR,
  city          VARCHAR,
  region        VARCHAR,
  country       VARCHAR,
  hire_date     DATE,
  birth_date    DATE,
  record_hash   VARCHAR
);

-- 3.5) Dimensión Shipper (SCD Tipo 1)
CREATE TABLE IF NOT EXISTS dw.dim_shipper (
  shipper_sk   BIGINT PRIMARY KEY,
  shipper_id   BIGINT, -- business key (ShipperID)
  company_name VARCHAR,
  phone        VARCHAR,
  record_hash  VARCHAR
);

-- 4) HECHO PRINCIPAL
--    Fact de ventas por línea:
--    - Incluye Degenerate Dimension (order_id) para trazabilidad.
--    - Foreign keys a dimensiones por SK.
--    - Measures: quantity, unit_price, discount, gross/net.
--    - Freight: viene del header (Orders). Lo repetimos por línea y además mostramos ejemplo de asignación proporcional.
CREATE TABLE IF NOT EXISTS dw.fact_sales_line (
  -- Degenerate + trazabilidad
  order_id     BIGINT,
  product_id   BIGINT,
  line_number  BIGINT,

  -- FKs (surrogate keys)
  customer_sk  BIGINT,
  product_sk   BIGINT,
  employee_sk  BIGINT,
  shipper_sk   BIGINT,
  order_date_sk    INTEGER,
  required_date_sk INTEGER,
  shipped_date_sk  INTEGER,

  -- Measures (aditivas)
  quantity     BIGINT,
  unit_price   DOUBLE,
  discount     DOUBLE,
  gross_amount DOUBLE,
  net_amount   DOUBLE,

  -- Atributos del header repetidos para análisis
  order_freight      DOUBLE,
  freight_allocated  DOUBLE,

  etl_loaded_at TIMESTAMP,

  -- En Northwind "Order Details" tiene PK (OrderID, ProductID)
  PRIMARY KEY (order_id, product_id)
);
