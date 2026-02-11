-- =====================================================================================
-- 90_simulate_new_data.sql (V2 - didáctico)
--
-- OBJETIVO
--   Modificar el OLTP para crear escenarios de aprendizaje:
--     1) Cambio en Customer existente (SCD2: nueva versión)
--     2) Customer nuevo (SCD2: nuevo miembro)
--     3) Cambio en Product existente (SCD2: nueva versión)
--     4) Product nuevo (SCD2: nuevo miembro)
--     5) Nuevas Orders + Order Details (para probar incremental de fact por watermark)
--
-- NOTA
--   Este script se corre contra data/oltp/northwind_oltp.duckdb y lo modifica.
-- =====================================================================================

-- 1) Customer existente cambia (dispara SCD2)
UPDATE Customers
SET City = 'Munich', Phone = '+49 999 000 111'
WHERE CustomerID = 'ALFKI';

-- 2) Customer nuevo (dispara inserción nueva en dim_customer)
INSERT INTO Customers (CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT 'SBADR', 'Sebas Data Shop', 'Sebastián Badaró', 'Owner', 'Córdoba 123', 'Córdoba', 'Córdoba', '5000', 'Argentina', '+54 351 000 000', NULL
WHERE NOT EXISTS (SELECT 1 FROM Customers WHERE CustomerID='SBADR');

-- 3) Product existente cambia (dispara SCD2)
UPDATE Products
SET UnitPrice = 20.0
WHERE ProductID = 1;

-- 4) Product nuevo (si no existe)
INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)
SELECT 78, 'Sebas Special Sauce', 1, 2, '12 bottles', 15.0, 100, 0, 10, 0
WHERE NOT EXISTS (SELECT 1 FROM Products WHERE ProductID=78);

-- 5) Nueva orden (OrderID alto para que sea > watermark)
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

-- 6) Líneas de la orden (Order Details)
INSERT INTO "Order Details" (OrderID, ProductID, UnitPrice, Quantity, Discount)
SELECT 11078, 1, 20.0, 10, 0.05
WHERE NOT EXISTS (SELECT 1 FROM "Order Details" WHERE OrderID=11078 AND ProductID=1);

INSERT INTO "Order Details" (OrderID, ProductID, UnitPrice, Quantity, Discount)
SELECT 11078, 78, 15.0, 5, 0.00
WHERE NOT EXISTS (SELECT 1 FROM "Order Details" WHERE OrderID=11078 AND ProductID=78);

-- 7) Otra orden para el customer nuevo
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

-- 8) Outputs rápidos (para ver que efectivamente cambió algo)
SELECT 'Customers total' AS metric, count(*) AS value FROM Customers;
SELECT 'Products total'  AS metric, count(*) AS value FROM Products;
SELECT 'Orders total'    AS metric, count(*) AS value FROM Orders;
SELECT 'Order Details total' AS metric, count(*) AS value FROM "Order Details";
