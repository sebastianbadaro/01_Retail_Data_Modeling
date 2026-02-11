-- =====================================================================================
-- 90_simulate_new_data.sql
-- Objetivo:
--   Simular cambios y nuevos datos en el OLTP para demostrar:
--     - SCD2 en dim_customer y dim_product
--     - Nuevas órdenes para cargar incrementalmente (watermark por OrderID)
--
-- Importante:
--   - Este script MODIFICA la base OLTP (data/oltp/northwind_oltp.duckdb).
--   - Está hecho para ser razonablemente idempotente:
--       * inserta customer 'SBADR' si no existe
--       * inserta product 78 si no existe
--       * inserta order 11078 si no existe
-- =====================================================================================

-- 1) Cambios en un customer existente (dispara SCD2)
UPDATE Customers
SET City = 'Munich', Phone = '+49 999 000 111'
WHERE CustomerID = 'ALFKI';

-- 2) Insertar un nuevo customer (dispara new member)
INSERT INTO Customers (CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT 'SBADR', 'Sebas Data Shop', 'Sebastián Badaró', 'Owner', 'Córdoba 123', 'Córdoba', 'Córdoba', '5000', 'Argentina', '+54 351 000 000', NULL
WHERE NOT EXISTS (SELECT 1 FROM Customers WHERE CustomerID='SBADR');

-- 3) Cambios en producto existente (dispara SCD2)
UPDATE Products
SET UnitPrice = 20.0
WHERE ProductID = 1;

-- 4) Insertar un nuevo producto (ProductID=78) si no existe
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)
SELECT 78, 'Sebas Special Sauce', 1, 2, '12 bottles', 15.0, 100, 0, 10, 0
WHERE NOT EXISTS (SELECT 1 FROM Products WHERE ProductID=78);

-- 5) Insertar una nueva orden (OrderID=11078) si no existe
INSERT INTO Orders (
  OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate,
  ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry
)
SELECT
  11078, 'ALFKI', 1,
  TIMESTAMP '1998-05-08 00:00:00',
  TIMESTAMP '1998-05-20 00:00:00',
  TIMESTAMP '1998-05-10 00:00:00',
  1, 32.50,
  'Alfreds Futterkiste', 'Obere Str. 57', 'Munich', NULL, '12209', 'Germany'
WHERE NOT EXISTS (SELECT 1 FROM Orders WHERE OrderID=11078);

-- 6) Insertar líneas de esa orden (Order Details)
--    Nota: PK (OrderID, ProductID) -> elegimos productos 1 y 78.
INSERT INTO "Order Details" (OrderID, ProductID, UnitPrice, Quantity, Discount)
SELECT 11078, 1, 20.0, 10, 0.05
WHERE NOT EXISTS (SELECT 1 FROM "Order Details" WHERE OrderID=11078 AND ProductID=1);

INSERT INTO "Order Details" (OrderID, ProductID, UnitPrice, Quantity, Discount)
SELECT 11078, 78, 15.0, 5, 0.00
WHERE NOT EXISTS (SELECT 1 FROM "Order Details" WHERE OrderID=11078 AND ProductID=78);

-- 7) (Opcional) insert otra orden para el nuevo customer (SBADR)
INSERT INTO Orders (
  OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate,
  ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry
)
SELECT
  11079, 'SBADR', 2,
  TIMESTAMP '1998-05-09 00:00:00',
  TIMESTAMP '1998-05-21 00:00:00',
  NULL,
  2, 10.00,
  'Sebas Data Shop', 'Córdoba 123', 'Córdoba', 'Córdoba', '5000', 'Argentina'
WHERE NOT EXISTS (SELECT 1 FROM Orders WHERE OrderID=11079);

INSERT INTO "Order Details" (OrderID, ProductID, UnitPrice, Quantity, Discount)
SELECT 11079, 78, 15.0, 3, 0.10
WHERE NOT EXISTS (SELECT 1 FROM "Order Details" WHERE OrderID=11079 AND ProductID=78);

-- Quick sanity outputs (para que veas qué cambió)
SELECT 'Customers total' AS metric, count(*) AS value FROM Customers;
SELECT 'Products total'  AS metric, count(*) AS value FROM Products;
SELECT 'Orders total'    AS metric, count(*) AS value FROM Orders;
SELECT 'Order Details total' AS metric, count(*) AS value FROM "Order Details";
