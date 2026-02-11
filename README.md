# Proyecto completo OLTP → OLAP (Kimball) con DuckDB (Northwind)

Este proyecto está pensado para **aprender modelado dimensional (Kimball) paso a paso** con un caso clásico de OLTP multi‑tabla: **Northwind**.

Incluye:
- Diseño dimensional (dims + fact) con **justificación de cada decisión**
- **Carga histórica (full load)**
- **SCD Tipo 2** (Customer y Product) para historizar cambios
- Script para **simular nuevos datos/cambios** en OLTP
- **Carga incremental** (dims + facts) con watermarks
- Queries de validación y “BI ready”

---

## Estructura

- `data/oltp/northwind_oltp.duckdb` → Base OLTP (operacional) de Northwind
- `data/dw/northwind_dw.duckdb` → Se crea al ejecutar los scripts
- `sql/` → Scripts SQL (ejecutar en orden)

---

## Cómo ejecutar (CLI)

> Requisito: tener instalado DuckDB localmente.

### 1) Crear DW (schema + tablas)
```bash
duckdb data/dw/northwind_dw.duckdb < sql/01_init_dw.sql
```

### 2) Carga histórica completa (dims + facts + watermarks)
```bash
duckdb data/dw/northwind_dw.duckdb < sql/02_load_full_historical.sql
```

### 3) Validar conteos y sanity checks
```bash
duckdb data/dw/northwind_dw.duckdb < sql/99_validation_queries.sql
```

### 4) Simular nuevos datos + cambios en OLTP (para demostrar incremental)
```bash
duckdb data/oltp/northwind_oltp.duckdb < sql/90_simulate_new_data.sql
```

### 5) Correr carga incremental (SCD2 + nuevos pedidos)
```bash
duckdb data/dw/northwind_dw.duckdb < sql/03_load_incremental.sql
```

---

## Qué enseña este proyecto (Kimball “de verdad”)

1) **Proceso de negocio**: Ventas / Fulfillment  
2) **Grano**: 1 fila = **1 línea de pedido** (OrderID + ProductID)  
3) **Fact**: `dw.fact_sales_line` con medidas aditivas  
4) **Dimensiones**:
   - `dw.dim_date` (role‑playing en fact: order/required/shipped)
   - `dw.dim_customer` (**SCD2**) – ejemplo de historización real
   - `dw.dim_product` (**SCD2**) – denormalizada (Category + Supplier dentro)
   - `dw.dim_employee` (SCD1)
   - `dw.dim_shipper` (SCD1)
5) **Unknown members**:
   - `date_sk = 0` para fechas faltantes (ej. ShippedDate NULL)
   - `*_sk = 0` para dimensiones faltantes (mejores joins y BI sin NULLs)
6) **Carga incremental**:
   - Watermark por `OrderID` (nuevo fact)
   - Detección de cambios por `record_hash` (para SCD2)

---

## Notas
- Este proyecto prioriza **claridad didáctica**: los scripts están comentados con “qué/por qué”.
- Es intencional que el incremental re‑lea dimensiones completas (Northwind es chico). En producción usarías CDC / timestamps / streams.