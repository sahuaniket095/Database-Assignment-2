-- Customers Table:
CREATE TABLE Customers (
    CustomerID SERIAL PRIMARY KEY,  --  Primary key ( Auto-incremented )
    Name VARCHAR(255) NOT NULL,  -- Name cannot be NULL
    Email VARCHAR(255) UNIQUE NOT NULL,  -- Unique constraint for email which can't be null
    RegistrationDate DATE NOT NULL DEFAULT CURRENT_DATE,  -- Default to current date
    CONSTRAINT email_check CHECK (Email LIKE '%@gmail.com')  -- Simple email format validation
);

-- Products Table:
CREATE TABLE Products (
    ProductID SERIAL PRIMARY KEY,  --  Primary key ( Auto-incremented )
    ProductName VARCHAR(255) NOT NULL,  -- Product name cannot be null
    Category VARCHAR(100) NOT NULL,  -- Category cannot be null
    Price DECIMAL(10, 2) NOT NULL CHECK (Price >= 0),  -- Price should be more than or equals to zero
    Stock INT NOT NULL CHECK (Stock >= 0),  -- Stock should be more than or equals to zero
    CONSTRAINT unique_product UNIQUE (ProductName, Category)  -- Product uniqueness per category
);

-- Orders Table:
CREATE TABLE Orders (
    OrderID SERIAL PRIMARY KEY,  --  Primary key ( Auto-incremented )
    CustomerID INT NOT NULL,  -- CustomerID should not be null
    OrderDate DATE NOT NULL DEFAULT CURRENT_DATE,  -- Default to current date
    TotalAmount DECIMAL(10, 2) NOT NULL CHECK (TotalAmount >= 0),  -- Total amount should be more than or equals to zero
    CONSTRAINT fk_customer FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID) ON DELETE CASCADE
);

-- OrderDetails Table:
CREATE TABLE OrderDetails (
    OrderDetailID SERIAL PRIMARY KEY,  -- Primary key (  Auto-incremented )
    OrderID INT NOT NULL,  -- OrderID should not be null
    ProductID INT NOT NULL,  -- ProductID should not be null
    Quantity INT NOT NULL CHECK (Quantity > 0),  -- Quantity should be greater than 0
    Subtotal DECIMAL(10, 2) NOT NULL CHECK (Subtotal >= 0),  -- Subtotal should be more than or equals to zero
    CONSTRAINT fk_order FOREIGN KEY (OrderID) REFERENCES Orders(OrderID) ON DELETE CASCADE,
    CONSTRAINT fk_product FOREIGN KEY (ProductID) REFERENCES Products(ProductID) ON DELETE CASCADE
);
-- Inserting Customer Data:
INSERT INTO Customers (Name, Email, RegistrationDate)
VALUES
('Arun', 'arun@gmail.com', '2024-09-18'),
('Priyam', 'priyam@gmail.com', '2024-10-29'),
('Ram', 'ram@gmail.com', '2024-11-17'),
('Nehal', 'nehal@gmail.com', '2024-12-05'),
('Suraj', 'suraj@gmail.com', '2025-01-25'),
('Anuj', 'anuj@gmail.com', '2025-02-17');

-- Inserting Product Data:
INSERT INTO Products (ProductName, Category, Price, Stock)
VALUES
('Laptop', 'Electronics', 79999.99, 50),
('Clock', 'Electronics', 129999.99, 30),
('Mobile', 'Electronics', 64999.99, 40),
('BOOT', 'Footwear', 8999.99, 150),
('Sandel', 'Footwear', 13999.99, 100),
('Shoes', 'Footwear', 7999.99, 120),
('Table', 'Furniture', 29999.99, 20),
('Bed', 'Furniture', 59999.99, 15),
('Chair', 'Furniture', 13999.99, 30),
('Shirt', 'Apparel', 1799.99, 200),
('Jeans', 'Apparel', 2999.99, 180),
('glasses', 'Apparel', 7999.99, 75);




-- Insert Orders Data 
INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
VALUES
(1, '2024-02-10', 76999.00),  -- 1 year ago
(2, '2024-05-12', 988000.00),
(3, '2024-06-13', 46699.00),
(4, '2024-07-15', 42999.98),
(5, '2024-09-10', 175999.95),
(6, '2024-12-12', 22999.91),
(1, '2024-11-18', 47899.93),
(2, '2024-10-22', 134000.00),
(3, '2024-08-09', 72000.00),
(4, '2024-03-01', 65700.00),
(5, '2024-01-25', 32999.99),
(6, '2024-12-15', 78999.98);
-- Insert OrderDetails Data
INSERT INTO OrderDetails (OrderID, ProductID, Quantity, Subtotal)
VALUES
(1, 1, 1, 56499.99),
(1, 4, 1, 8329.99),
(2, 2, 1, 129999.99),
(2, 5, 1, 13999.99),
(3, 3, 1, 64669.99),
(4, 6, 1, 7999.99),
(4, 8, 1, 13999.99),
(5, 7, 1, 45999.99),
(5, 9, 1, 49779.99),
(6, 10, 2, 6399.98);

-- TASK 1:
-- Top 3 Customers with the Highest Total Purchase Amount
SELECT c.CustomerID, c.Name, SUM(od.Subtotal) AS TotalPurchaseAmount
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
JOIN OrderDetails od ON o.OrderID = od.OrderID
GROUP BY c.CustomerID, c.Name
ORDER BY TotalPurchaseAmount DESC
LIMIT 3;

-- Monthly Sales Revenue for the Last 6 Months
CREATE EXTENSION IF NOT EXISTS tablefunc;
SELECT * FROM crosstab(
    $$
    SELECT
        TO_CHAR(DATE_TRUNC('month', OrderDate), 'YYYY-MM') AS Month,
        'Total Revenue' AS revenue_label,
        COALESCE(SUM(TotalAmount), 0) AS TotalRevenue
    FROM Orders
    WHERE OrderDate >= DATE_TRUNC('month', NOW()) - INTERVAL '7 months'
    GROUP BY TO_CHAR(DATE_TRUNC('month', OrderDate), 'YYYY-MM')
    ORDER BY Month
    $$,
    $$ VALUES ('Total Revenue') $$  -- This defines the column name for revenue.
) AS ct (month TEXT, "Total Revenue" NUMERIC);






-- Second Most Expensive Product in Each Category
WITH RankedProducts AS (
    SELECT p.Category, p.ProductID, p.ProductName, p.Price,
           RANK() OVER (PARTITION BY p.Category ORDER BY p.Price DESC) AS PriceRank
    FROM Products p
)
SELECT Category, ProductID, ProductName, Price
FROM RankedProducts
WHERE PriceRank = 2;

-- TASK 2

-- Create Stored Procedure to Place an Order

CREATE OR REPLACE PROCEDURE PlaceOrder(
    IN p_CustomerID INT,
    IN p_ProductID INT,
    IN p_Quantity INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_ProductPrice DECIMAL(10,2);
    v_AvailableStock INT;
    v_Subtotal DECIMAL(10,2);
    v_TotalAmount DECIMAL(10,2);
    v_OrderID INT;
BEGIN
    -- Check if Customer exists
    IF NOT EXISTS (SELECT 1 FROM Customers WHERE CustomerID = p_CustomerID) THEN
        RAISE EXCEPTION 'Customer ID % does not exist.', p_CustomerID;
    END IF;

    -- Fetch Product Price and Stock
    SELECT Price, Stock INTO v_ProductPrice, v_AvailableStock 
    FROM Products WHERE ProductID = p_ProductID;

    -- Check if Product exists
    IF v_ProductPrice IS NULL THEN
        RAISE EXCEPTION 'Product ID % does not exist.', p_ProductID;
    END IF;

    -- Check if stock is available
    IF v_AvailableStock < p_Quantity THEN
        RAISE EXCEPTION 'Insufficient stock for Product ID %. Available: %, Requested: %', p_ProductID, v_AvailableStock, p_Quantity;
    END IF;

    -- Calculate subtotal and total amount
    v_Subtotal := v_ProductPrice * p_Quantity;
    v_TotalAmount := v_Subtotal;

    -- Insert into Orders table
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
    VALUES (p_CustomerID, CURRENT_DATE, v_TotalAmount)
    RETURNING OrderID INTO v_OrderID;

    -- Insert into OrderDetails table
    INSERT INTO OrderDetails (OrderID, ProductID, Quantity, Subtotal)
    VALUES (v_OrderID, p_ProductID, p_Quantity, v_Subtotal);

    -- Update Product stock
    UPDATE Products SET Stock = Stock - p_Quantity WHERE ProductID = p_ProductID;

    RAISE NOTICE 'Order placed successfully! Order ID: %', v_OrderID;
END;
$$;

CALL PlaceOrder(5, 5, 2);

-- Create Function to Calculate Total Amount Spent by Customer
CREATE OR REPLACE FUNCTION total_spent_by_customer(p_customer_id INT) 
RETURNS DECIMAL(10,2) AS $$
DECLARE
    v_total_spent DECIMAL(10,2);
BEGIN
    -- Calculate total spent by the customer
    SELECT COALESCE(SUM(TotalAmount), 0) INTO v_total_spent
    FROM Orders
    WHERE CustomerID = p_customer_id;

    -- Return the total amount spent
    RETURN v_total_spent;
END;
$$ LANGUAGE plpgsql;

SELECT total_spent_by_customer(3);



-- TASK 3

-- Transaction to Place Order if All Products Are in Stock
BEGIN;  -- Start the transaction

-- Step 1: Check if all products in the order are in stock
DO $$ 
DECLARE
    insufficient_stock BOOLEAN := FALSE;
    product RECORD;
BEGIN
    -- Loop through each product in the order and check stock
    FOR product IN
        SELECT p.ProductID, p.Stock, od.Quantity
        FROM OrderDetails od
        JOIN Products p ON od.ProductID = p.ProductID
        WHERE od.OrderID = 1  -- Replace with your actual OrderID
    LOOP
        IF product.Stock < product.Quantity THEN
            -- Set flag to true if stock is insufficient
            insufficient_stock := TRUE;
            EXIT;  -- Exit the loop if any product is out of stock
        END IF;
    END LOOP;

    -- If there's insufficient stock, rollback the transaction
    IF insufficient_stock THEN
        RAISE EXCEPTION 'Insufficient stock for one or more products. Rolling back the transaction.';
    END IF;
END $$;

-- Step 2: If all products are in stock, proceed to update the stock levels and place the order
UPDATE Products
SET Stock = Stock - od.Quantity
FROM OrderDetails od
WHERE Products.ProductID = od.ProductID
AND od.OrderID = 1;  -- Replace with your actual OrderID

-- Step 3: Insert order into the Orders table
INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
VALUES (1, CURRENT_DATE, (SELECT SUM(od.Subtotal) FROM OrderDetails od WHERE od.OrderID = 1));

-- Step 4: Commit the transaction if everything is successful
COMMIT;
select * From Orders;

-----------------------------------------------------------------------------------------------------------------------
BEGIN;  -- Start the transaction

-- Create the function to handle deadlocks during the update of order details
CREATE OR REPLACE FUNCTION UpdateOrderDetailWithRetry(
    p_OrderDetailID INT,
    p_NewQuantity INT,
    p_RetryLimit INT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_RetryCount INT := 0;
    v_UpdateSuccess BOOLEAN := FALSE;
    v_Stock INT;
    v_ProductID INT;
BEGIN
    -- Loop to retry in case of deadlocks
    WHILE v_RetryCount < p_RetryLimit AND NOT v_UpdateSuccess LOOP
        BEGIN
            -- Get the ProductID and Stock for the given OrderDetailID
            SELECT ProductID INTO v_ProductID
            FROM OrderDetails
            WHERE OrderDetailID = p_OrderDetailID;

            SELECT Stock INTO v_Stock
            FROM Products
            WHERE ProductID = v_ProductID;

            -- Check if the stock is sufficient for the new quantity
            IF v_Stock < p_NewQuantity THEN
                RAISE EXCEPTION 'Not enough stock for ProductID: %, available stock: %, requested: %', 
                    v_ProductID, v_Stock, p_NewQuantity;
            END IF;

            -- Attempt to update the order details
            UPDATE OrderDetails
            SET Quantity = p_NewQuantity,
                Subtotal = (Price * p_NewQuantity)
            WHERE OrderDetailID = p_OrderDetailID;

            -- If the update is successful, set the success flag to TRUE
            v_UpdateSuccess := TRUE;

        EXCEPTION
            WHEN serialization_failure THEN
                -- Handle deadlock or serialization failure
                v_RetryCount := v_RetryCount + 1;
                RAISE NOTICE 'Deadlock detected. Retrying... Attempt: %', v_RetryCount;
                PERFORM pg_sleep(2); -- Sleep for 2 seconds before retrying
            WHEN OTHERS THEN
                -- Reraise any other exceptions
                RAISE;
        END;
    END LOOP;

    IF NOT v_UpdateSuccess THEN
        RAISE EXCEPTION 'Failed to update OrderDetailID % after % attempts', p_OrderDetailID, p_RetryLimit;
    END IF;
END;
$$;

----------------------------------------------------------------------------------------------------------------------
BEGIN;  -- Start the outer transaction

-- Optionally create an initial savepoint
SAVEPOINT start_point;

DO $$
DECLARE
    v_product_id   INT;
    v_quantity     INT;
    v_stock        INT;
    v_price        DECIMAL(10, 2);
    v_subtotal     DECIMAL(10, 2);
    v_total_amount DECIMAL(10, 2) := 0;
    v_order_id     INT;
BEGIN
    -- Set a savepoint for a partial update
    SAVEPOINT partial_update;
    
    -- Step 1: Insert into Orders and get the generated OrderID
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
    VALUES (101, CURRENT_DATE, 0)
    RETURNING OrderID INTO v_order_id;
    
    -- Optionally mark that order data was added
    SAVEPOINT order_data_added;

    -- Step 2: Process each order item from JSON
    FOR v_product_id, v_quantity IN 
        SELECT (key)::INT, (value)::INT 
        FROM jsonb_each_text('{"1": 2, "3": 1, "5": 3}'::jsonb)
    LOOP
        -- Step 3: Check product stock
        SELECT Stock INTO v_stock
        FROM Products
        WHERE ProductID = v_product_id;

        -- Step 4: If stock is insufficient, log and skip this item
        IF v_stock < v_quantity THEN
            RAISE NOTICE 'Product % is out of stock. Skipping...', v_product_id;
            CONTINUE;
        ELSE
            -- Step 5: Retrieve product price
            SELECT Price INTO v_price FROM Products WHERE ProductID = v_product_id;
            
            -- Step 6: Compute subtotal
            v_subtotal := v_price * v_quantity;
            
            -- Step 7: Deduct stock
            UPDATE Products
            SET Stock = Stock - v_quantity
            WHERE ProductID = v_product_id;
            
            -- Step 8: Insert order detail record
            INSERT INTO OrderDetails (OrderID, ProductID, Quantity, Subtotal)
            VALUES (v_order_id, v_product_id, v_quantity, v_subtotal);
            
            -- Step 9: Update total amount
            v_total_amount := v_total_amount + v_subtotal;
        END IF;
    END LOOP;

    -- Step 10: Update the total amount in the Orders table
    UPDATE Orders
    SET TotalAmount = v_total_amount
    WHERE OrderID = v_order_id;
    
    -- Log success (no explicit COMMIT inside the DO block)
    RAISE NOTICE 'Order placed successfully with OrderID: %', v_order_id;
EXCEPTION
    WHEN OTHERS THEN
        -- Instead of issuing an explicit ROLLBACK TO SAVEPOINT inside the DO block,
        -- re-raise the exception so that you can handle it outside.
        RAISE NOTICE 'An error occurred; rolling back to the partial_update savepoint.';
        RAISE;
END $$;


-- If an error was raised inside the DO block, you can now rollback externally:
-- ROLLBACK TO SAVEPOINT partial_update;
-- (or simply ROLLBACK if you want to undo the entire transaction)

COMMIT;  -- If no error occurred, commit the outer transaction

ROLLBACK;

-- TASK 4
----Generate a Customer Purchase Report Using ROLLUP-----------------------

SELECT 
    c.CustomerID,
    c.Name,
    COALESCE(SUM(od.Subtotal), 0) AS TotalPurchases
FROM 
    Customers c
JOIN 
    Orders o ON c.CustomerID = o.CustomerID
JOIN 
    OrderDetails od ON o.OrderID = od.OrderID
GROUP BY 
    GROUPING SETS (
        (c.CustomerID, c.Name),  -- Group by individual customer
        ()  -- This will provide the grand total for all customers
    )
ORDER BY 
    c.CustomerID;
----------------------------------------------------------------------------------------------------------------

--Use window functions (LEAD, LAG) to show how a customer's order amount compares to their previous order amount.
SELECT 
    o.CustomerID,
    o.OrderID,
    o.OrderDate,
    o.TotalAmount,
    LAG(o.TotalAmount) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS PreviousOrderAmount,
    LEAD(o.TotalAmount) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS NextOrderAmount
FROM 
    Orders o
ORDER BY 
    o.CustomerID, o.OrderDate;

