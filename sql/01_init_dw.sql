-- =====================================================================================
-- 01_init_dw.sql
-- Objetivo:
--   1) Conectarse a la base OLTP (Northwind) en DuckDB
--   2) Crear esquemas (ctl, stg, dw)
--   3) Crear tablas de control (watermarks, secuencias de surrogate keys, logs)
--   4) Crear el modelo dimensional (dims + fact) siguiendo Kimball
--
-- Diseño (Kimball) - decisiones clave (justificadas):
--   A) Proceso de negocio: Ventas / Fulfillment (Orders + Order Details)
--   B) Grano de la fact: 1 fila = 1 línea de pedido (OrderID + ProductID)
--      -> Es el grano más atómico disponible y permite agregaciones correctas.
--   C) Star schema (no snowflake) para BI:
--      -> denormalizamos Product con Category + Supplier dentro de dim_product
--   D) Surrogate keys en dimensiones:
--      -> desacopla del OLTP y habilita SCD2 (historia) sin romper facts.
--   E) Unknown members (SK = 0):
--      -> evita NULL FKs y simplifica herramientas BI (joins + filtros).
-- =====================================================================================

-- 0) Conectar al OLTP (ruta relativa al root del repo)
ATTACH 'data/oltp/northwind_oltp.duckdb' AS oltp;

-- 1) Esquemas
CREATE SCHEMA IF NOT EXISTS ctl;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;

-- 2) Tablas de control
-- Watermark para carga incremental de hechos (orders)
CREATE TABLE IF NOT EXISTS ctl.watermark (
  entity         VARCHAR PRIMARY KEY,
  last_order_id  BIGINT,
  updated_at     TIMESTAMP
);

-- Secuencia “manual” para surrogate keys (dimensiones)
-- (Kimball: surrogate keys controladas por el DW, no por el OLTP)
CREATE TABLE IF NOT EXISTS ctl.surrogate_key_seq (
  dim_name   VARCHAR PRIMARY KEY,
  next_sk    BIGINT
);

-- Log de ejecuciones ETL (didáctico)
CREATE TABLE IF NOT EXISTS ctl.etl_run_log (
  run_id          BIGINT,
  run_ts          TIMESTAMP,
  run_type        VARCHAR,          -- 'FULL' o 'INCREMENTAL'
  effective_date  DATE,             -- fecha de vigencia que aplicamos a SCD2 (ver scripts)
  rows_dim_customer BIGINT,
  rows_dim_product  BIGINT,
  rows_fact_sales   BIGINT,
  notes           VARCHAR
);

-- 3) Dimensiones
-- 3.1) Dimensión Fecha (Kimball suele usar date_sk tipo YYYYMMDD; unknown = 0)
CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_sk     INTEGER PRIMARY KEY,     -- 0 = Unknown
  date        DATE,
  year        INTEGER,
  month       INTEGER,
  day         INTEGER,
  month_name  VARCHAR,
  day_name    VARCHAR,
  week_of_year INTEGER,
  quarter     INTEGER,
  is_weekend  BOOLEAN
);

-- 3.2) Dimensión Customer (SCD Tipo 2)
CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_sk    BIGINT PRIMARY KEY,    -- 0 = Unknown
  customer_id    VARCHAR,               -- business key (OLTP)
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

  -- SCD2 fields
  effective_from DATE,
  effective_to   DATE,
  is_current     BOOLEAN,

  -- Hash de atributos para detección de cambios (no es la SK)
  record_hash    VARCHAR
);

-- 3.3) Dimensión Product (SCD Tipo 2) - denormalizada con Category + Supplier
CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_sk        BIGINT PRIMARY KEY,  -- 0 = Unknown
  product_id        BIGINT,              -- business key (OLTP)
  product_name      VARCHAR,
  quantity_per_unit VARCHAR,
  list_unit_price   DOUBLE,
  units_in_stock    BIGINT,
  units_on_order    BIGINT,
  reorder_level     BIGINT,
  discontinued      BOOLEAN,

  -- Denormalización “BI friendly”
  category_id       BIGINT,
  category_name     VARCHAR,
  supplier_id       BIGINT,
  supplier_name     VARCHAR,
  supplier_country  VARCHAR,

  -- SCD2 fields
  effective_from    DATE,
  effective_to      DATE,
  is_current        BOOLEAN,

  record_hash       VARCHAR
);

-- 3.4) Dimensión Employee (SCD Tipo 1 / overwrite)
-- (Se mantiene simple: si cambia un atributo, lo actualizamos "in place")
CREATE TABLE IF NOT EXISTS dw.dim_employee (
  employee_sk   BIGINT PRIMARY KEY,      -- 0 = Unknown
  employee_id   BIGINT,                  -- business key (OLTP)
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

-- 3.5) Dimensión Shipper (SCD Tipo 1 / overwrite)
CREATE TABLE IF NOT EXISTS dw.dim_shipper (
  shipper_sk   BIGINT PRIMARY KEY,       -- 0 = Unknown
  shipper_id   BIGINT,                   -- business key (OLTP)
  company_name VARCHAR,
  phone        VARCHAR,
  record_hash  VARCHAR
);

-- 4) Hechos
-- Fact Ventas por línea de pedido (grano = OrderID + ProductID)
CREATE TABLE IF NOT EXISTS dw.fact_sales_line (
  -- Degenerate dimensions (mantener IDs operativos ayuda a trazabilidad/debug)
  order_id     BIGINT,
  product_id   BIGINT,
  line_number  BIGINT,

  -- Foreign keys (surrogate keys)
  customer_sk  BIGINT,
  product_sk   BIGINT,
  employee_sk  BIGINT,
  shipper_sk   BIGINT,
  order_date_sk    INTEGER,
  required_date_sk INTEGER,
  shipped_date_sk  INTEGER,

  -- Medidas
  quantity     BIGINT,
  unit_price   DOUBLE,
  discount     DOUBLE,
  gross_amount DOUBLE,
  net_amount   DOUBLE,

  -- Freight (del header) repetido por línea para análisis
  order_freight DOUBLE,
  -- Freight asignado proporcional al net_amount (ejemplo didáctico)
  freight_allocated DOUBLE,

  etl_loaded_at TIMESTAMP,

  -- Clave natural de la línea (en Northwind Order Details PK = (OrderID, ProductID))
  PRIMARY KEY (order_id, product_id)
);
