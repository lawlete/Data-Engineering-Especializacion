// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
// NOTE: The following constraint creation syntax is generated based on the current connected database version 5.19-aura.
CREATE CONSTRAINT `supplierID_Supplier_uniq` IF NOT EXISTS
FOR (n: `Supplier`)
REQUIRE (n.`supplierID`) IS UNIQUE;
CREATE CONSTRAINT `productID_Product_uniq` IF NOT EXISTS
FOR (n: `Product`)
REQUIRE (n.`productID`) IS UNIQUE;
CREATE CONSTRAINT `categoryID_Category_uniq` IF NOT EXISTS
FOR (n: `Category`)
REQUIRE (n.`categoryID`) IS UNIQUE;
CREATE CONSTRAINT `orderID_Order_uniq` IF NOT EXISTS
FOR (n: `Order`)
REQUIRE (n.`orderID`) IS UNIQUE;
CREATE CONSTRAINT `customerID_Customer_uniq` IF NOT EXISTS
FOR (n: `Customer`)
REQUIRE (n.`customerID`) IS UNIQUE;



// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
:auto LOAD CSV WITH HEADERS FROM 'file:///suppliers.csv' AS row
WITH row
WHERE  NOT row.`supplierID` IS NULL
CALL {
  WITH row
  MERGE (n: `Supplier` { `supplierID`: row.`supplierID` })
  SET n.`supplierID` = row.`supplierID`
  SET n.`companyName` = row.`companyName`
  SET n.`contactName` = row.`contactName`
  SET n.`contactTitle` = row.`contactTitle`
  SET n.`address` = row.`address`
  SET n.`city` = row.`city`
  SET n.`region` = row.`region`
  SET n.`postalCode` = row.`postalCode`
  SET n.`country` = row.`country`
  SET n.`phone` = row.`phone`
  SET n.`fax` = row.`fax`
  SET n.`homePage` = row.`homePage`
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM 'file:///categories.csv' AS row
WITH row
WHERE NOT row.`categoryID` IS NULL
CALL {
  WITH row
  MERGE (n: `Category` { `categoryID`: row.`categoryID` })
  SET n.`categoryID` = row.`categoryID`
  SET n.`categoryName` = row.`categoryName`
  SET n.`description` = row.`description`
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
WITH row
WHERE  NOT row.`productID` IS NULL
CALL {
  WITH row
  MERGE (n: `Product` { `productID`: row.`productID` })
  SET n.`productID` = row.`productID`
  SET n.`productName` = row.`productName`
  SET n.`quantityPerUnit` = toInteger(trim(row.`quantityPerUnit`))
  SET n.`unitPrice` = toFloat(trim(row.`unitPrice`))
  SET n.`unitsInStock` = toInteger(trim(row.`unitsInStock`))
  SET n.`unitsOnOrder` = toInteger(trim(row.`unitsOnOrder`))
  SET n.`reorderLevel` = toInteger(trim(row.`reorderLevel`))
  SET n.`discontinued` = toLower(trim(row.`discontinued`)) IN ['1','true','yes']
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM 'file:///categories.csv' AS row
WITH row
WHERE  NOT row.`categoryID` IS NULL
CALL {
  WITH row
  MERGE (n: `Category` { `categoryID`: row.`categoryID` })
  SET n.`categoryID` = row.`categoryID`
  SET n.`categoryName` = row.`categoryName`
  SET n.`description` = row.`description`
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM 'file:///orders.csv' AS row
WITH row
WHERE NOT row.`orderID` IS NULL
CALL {
  WITH row
  MERGE (n: `Order` { `orderID`: row.`orderID` })
  SET n.`orderID` = row.`orderID`
  SET n.`orderDate` = row.`orderDate`
  SET n.`requiredDate` = row.`requiredDate`
  SET n.`shippedDate` = row.`shippedDate`
  SET n.`freight` = row.`freight`
  SET n.`shipName` = row.`shipName`
  SET n.`shipAddress` = row.`shipAddress`
  SET n.`shipCity` = row.`shipCity`
  SET n.`shipRegion` = row.`shipRegion`
  SET n.`shipPostalCode` = row.`shipPostalCode`
  SET n.`shipCountry` = row.`shipCountry`
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM 'file:///customers.csv' AS row
WITH row
WHERE NOT row.`customerID` IS NULL
CALL {
  WITH row
  MERGE (n: `Customer` { `customerID`: row.`customerID` })
  SET n.`customerID` = row.`customerID`
  SET n.`companyName` = row.`companyName`
  SET n.`contactName` = row.`contactName`
  SET n.`contactTitle` = row.`contactTitle`
  SET n.`address` = row.`address`
  SET n.`city` = row.`city`
  SET n.`region` = row.`region`
  SET n.`postalCode` = row.`postalCode`
  SET n.`country` = row.`country`
  SET n.`phone` = row.`phone`
  SET n.`fax` = row.`fax`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.

:auto LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Supplier` { `supplierID`: row.`supplierID` })
  MATCH (target: `Product` { `productID`: row.`productID` })
  MERGE (source)-[r: `SUPPLIES`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Product` { `productID`: row.`productID` })
  MATCH (target: `Category` { `categoryID`: row.`categoryID` })
  MERGE (source)-[r: `PART_OF`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;


:auto LOAD CSV WITH HEADERS FROM 'file:///orders.csv' AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Customer` { `customerID`: row.`customerID` })
  MATCH (target: `Order` { `orderID`: row.`orderID` })
  MERGE (source)-[r: `PURCHASED`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;



:auto LOAD CSV WITH HEADERS FROM 'file:///order-details.csv' AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Order` { `orderID`: row.`orderID` })
  MATCH (target: `Product` { `productID`: row.`productID` })
  MERGE (source)-[r: `ORDERS`]->(target)
  SET r.`unitPrice` = toFloat(trim(row.`unitPrice`))
  SET r.`quantity` = toInteger(trim(row.`quantity`))
  SET r.`discount` = toFloat(trim(row.`discount`))
} IN TRANSACTIONS OF 10000 ROWS;
