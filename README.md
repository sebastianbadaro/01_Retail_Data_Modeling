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



Si alguno de estos conceptos no te suena, igual podés seguir el proyecto, pero te recomiendo repasar primero esos puntos para aprovecharlo al máximo.

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

## Supuestos y conocimientos previos 

Este proyecto está pensado como una guía **paso a paso** para construir un modelo dimensional, pero asume que ya contás con una base mínima en:

- **SQL**: `JOIN`, `GROUP BY`, `CTE`, funciones de ventana (`ROW_NUMBER`, `SUM() OVER`), tipos y casts.
- **Modelado relacional (OLTP)**: claves primarias/foráneas, normalización básica, relaciones 1–N y N–N.
- **Conceptos de BI / OLAP**: métricas vs atributos, agregaciones, filtros, y por qué un esquema analítico difiere del transaccional.
- **Conceptos de Data Warehousing**: dimensiones, hechos, grano, surrogate keys y nociones de SCD (al menos a nivel conceptual).
- **Ejecución local**: poder correr scripts SQL con DuckDB (CLI o extensión) y entender rutas/archivos del repo.

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

## Teoría de Kimball aplicada en este proyecto

Este proyecto sigue los principios del **modelado dimensional de Ralph Kimball** para transformar un esquema **OLTP** (transaccional, normalizado) en un esquema **OLAP** (analítico) basado en un **Star Schema** (modelo en estrella). El objetivo es que las consultas sean simples, rápidas y entendibles para análisis.

### 1) Proceso de negocio (Business Process)
El primer paso es elegir **qué proceso del negocio** queremos analizar.  
En este caso: **ventas/fulfillment**, usando `Orders` + `Order Details`.  
Esto define qué métricas y qué dimensiones forman parte del modelo.

### 2) Grano (Grain) de la tabla de hechos
La decisión más importante es definir el **grano**:
> ¿Qué representa exactamente una fila en la tabla de hechos?

En este proyecto, el grano es **1 fila por línea de pedido (order line)**.  
Esto evita doble conteo y permite analizar al nivel más atómico disponible.

### 3) Tabla de hechos (Fact Table)
La tabla de hechos contiene:
- **Claves foráneas a dimensiones** (surrogate keys)
- **Medidas numéricas** (métricas aditivas): cantidad, precio, descuento, importe bruto/neto, etc.
- **Dimensión degenerada** cuando corresponde (por ejemplo `order_id` para trazabilidad)

### 4) Dimensiones “anchas” y denormalizadas (conformed, BI-friendly)
Las dimensiones aportan el contexto descriptivo (quién, qué, cuándo, dónde).  
En Kimball se prefieren dimensiones:
- **Entendibles por negocio**
- **Denormalizadas** (evitar snowflake) para reducir joins y simplificar BI

Ejemplo: `dim_product` incluye atributos de `Products` + `Categories` + `Suppliers` (denormalizado).

### 5) Surrogate Keys (claves subrogadas)
Las dimensiones usan **surrogate keys (SK)** generadas por el DW (no las keys del OLTP) porque:
- desacoplan el DW de cambios del OLTP
- permiten historización (SCD)
- mejoran consistencia y performance de joins

### 6) Slowly Changing Dimensions (SCD)
Para manejar cambios en atributos dimensionales se aplican patrones SCD:

- **SCD Tipo 1 (overwrite):** se pisa el valor anterior (sin historia).  
  Útil cuando no interesa auditar cambios.
- **SCD Tipo 2 (histórico):** se crea una nueva fila con nueva SK para preservar historia.  
  Implementado con `effective_from`, `effective_to`, `is_current`.

En este proyecto:
- `dim_customer` y `dim_product` son **SCD2**
- `dim_employee` y `dim_shipper` se manejan como **SCD1**

### 7) Role-Playing Dates (dimensión fecha reutilizable)
Se usa una única `dim_date` y múltiples FKs desde el hecho:
- `order_date_sk`
- `required_date_sk`
- `shipped_date_sk`

### 8) Unknown Members (SK = 0)
Cada dimensión incluye un registro “Unknown” (`SK = 0`) para:
- evitar `NULL` en claves
- no romper joins
- permitir auditar calidad de datos (facts sin match)

### 9) Cargas full e incrementales
El enfoque separa:
- **Full/Historical load:** poblar el DW desde cero.
- **Incremental load:** aplicar cambios nuevos:
  - detectar cambios en dimensiones (SCD)
  - insertar nuevos hechos (watermark/CDC/timestamps)



---
## Notas
- Este proyecto prioriza **claridad didáctica**: los scripts están comentados con “qué/por qué”.
- Es intencional que el incremental re‑lea dimensiones completas (Northwind es chico). En producción usarías CDC / timestamps / streams.
- Se utiliza DuckDB porque permite correr todo el flujo localmente con un solo archivo de base de datos, sin infraestructura adicional. Es ideal para un proyecto didáctico y reproducible: facilita compartir el repo, ejecutar los scripts en cualquier máquina y validar resultados de punta a punta. Además, DuckDB está optimizado para cargas analíticas (OLAP), trabaja muy bien con archivos (por ejemplo Parquet/CSV/Excel) y soporta patrones clave para este tipo de proyectos (como upserts con MERGE y transformaciones SQL).
