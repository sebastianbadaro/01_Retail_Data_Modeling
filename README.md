# Proyecto completo OLTP → OLAP (Kimball) con DuckDB (Northwind)

Este proyecto está pensado para **aprender modelado dimensional (Kimball) paso a paso** con un caso clásico de OLTP multi‑tabla: **Northwind**.

La base de datos Northwind es una base de datos de ejemplo que fue creada originalmente por Microsoft y utilizada durante décadas como base para sus tutoriales en una variedad de productos de bases de datos. La base Northwind contiene datos de ventas de una empresa ficticia llamada “Northwind Traders”, que importa y exporta alimentos especializados de todo el mundo. Northwind es un excelente esquema tutorial para representar el ERP de una pequeña empresa, incluyendo clientes, pedidos, inventario, compras, proveedores, envíos, empleados y contabilidad de entrada única.

Incluye:
- Diseño dimensional (dims + fact) con **justificación de cada decisión**
- **Carga histórica (full load)**
- **SCD Tipo 2** (Customer y Product) para historizar cambios
- Script para **simular nuevos datos/cambios** en OLTP
- **Carga incremental** (dims + facts) con watermarks
- Queries de validación y “BI ready”

---


### ¿Qué representa el modelo? (Historia del negocio)
- **Customers** compran productos a Northwind.
- **Orders** son los pedidos (cabecera): quién compra, qué empleado lo gestiona, fechas y datos de envío.
- **Order Details** son las líneas del pedido (detalle): producto, cantidad, precio y descuento.
- **Products** es el catálogo/inventario; cada producto pertenece a una **Category** y lo provee un **Supplier**.
- **Shippers** son las compañías de transporte (cómo se envía el pedido).
- **Employees** son vendedores/operadores que gestionan pedidos; pueden estar asociados a territorios/regiones.

---

### Tablas principales (Data Dictionary “rápido”)

#### Transaccionales (hechos operacionales)
- **Orders**
  - **PK:** `OrderID`
  - **FKs:** `CustomerID` → Customers, `EmployeeID` → Employees, `ShipVia` → Shippers
  - **Campos clave:** `OrderDate`, `RequiredDate`, `ShippedDate`, `Freight`
  - **Interpretación:** cabecera del pedido (1 por pedido). Incluye datos de envío (ShipAddress/City/Country, etc.).

- **Order Details**
  - **PK compuesta:** (`OrderID`, `ProductID`)
  - **FKs:** `OrderID` → Orders, `ProductID` → Products
  - **Campos clave:** `UnitPrice`, `Quantity`, `Discount`
  - **Interpretación:** líneas del pedido (1..N por pedido). Es la tabla más “atómica” para análisis de ventas.

#### Maestras (catálogos / dimensiones en OLTP)
- **Customers**
  - **PK:** `CustomerID`
  - **Campos típicos:** `CompanyName`, `ContactName`, `City`, `Country`, etc.
  - **Interpretación:** información del cliente.

- **Products**
  - **PK:** `ProductID`
  - **FKs:** `SupplierID` → Suppliers, `CategoryID` → Categories
  - **Campos típicos:** `ProductName`, `QuantityPerUnit`, `UnitPrice`, `UnitsInStock`, `Discontinued`
  - **Interpretación:** catálogo e inventario.

- **Categories**
  - **PK:** `CategoryID`
  - **Campos típicos:** `CategoryName`, `Description`
  - **Interpretación:** clasificación de productos.

- **Suppliers**
  - **PK:** `SupplierID`
  - **Campos típicos:** `CompanyName`, `Country`, `Phone`, etc.
  - **Interpretación:** proveedores/vendedores de productos.

- **Employees**
  - **PK:** `EmployeeID`
  - **Campos típicos:** `FirstName`, `LastName`, `Title`, `HireDate`, `Country`
  - **Extra:** relación jerárquica (un empleado puede “reportar a” otro).

- **Shippers**
  - **PK:** `ShipperID`
  - **Campos típicos:** `CompanyName`, `Phone`
  - **Interpretación:** empresas que realizan envíos.

#### Organización / Geografía (opcional para análisis)
- **Regions**
  - **PK:** `RegionID`
  - **Interpretación:** región de ventas.

- **Territories**
  - **PK:** `TerritoryID`
  - **FK:** `RegionID` → Regions
  - **Interpretación:** territorios dentro de regiones.

- **EmployeeTerritories**
  - **PK compuesta:** (`EmployeeID`, `TerritoryID`)
  - **Interpretación:** tabla puente (N..N) entre empleados y territorios.

#### Segmentación de clientes (opcional; en algunos ports puede venir sin filas)
- **CustomerDemographics**
  - **PK:** `CustomerTypeID`

- **CustomerCustomerDemo**
  - **PK compuesta:** (`CustomerID`, `CustomerTypeID`)
  - **Interpretación:** puente para asignar segmentos a clientes.



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
- Se utiliza DuckDB porque permite correr todo el flujo localmente con un solo archivo de base de datos, sin infraestructura adicional. Es ideal para un proyecto didáctico y reproducible: facilita compartir el repo, ejecutar los scripts en cualquier máquina y validar resultados de punta a punta. Además, DuckDB está optimizado para cargas analíticas (OLAP), trabaja muy bien con archivos (por ejemplo Parquet/CSV/Excel) y soporta patrones clave para este tipo de proyectos (como upserts con MERGE y transformaciones SQL).
