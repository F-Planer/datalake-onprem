-- ERP simulation procedures.
--
-- dbo.SimulateActivity: live activity, called in a loop by simulate.sh. Each call:
--   creates new orders (with items) and decrements stock;
--   moves open orders forward: PAID creates a payment, SHIPPED a shipment,
--   DELIVERED fills in the delivery date;
--   cancels a few orders (a paid one gets its payment REFUNDED);
--   occasionally removes an order item (produces a DELETE in CDC);
--   occasionally syncs a BACK-DATED order: a store that was offline sends an
--   order created 1 to 72 hours ago (late data, see docs/MAINTENANCE.md);
--   restocks;
--   occasionally changes a product price and creates/edits a customer (batch tables).
--
-- dbo.GenerateHistory: bulk load of past data (months of orders), run on
-- demand by history.sh. Tests volume in CDC, batch loads, gold and maintenance.
USE [$(DB)];
GO
CREATE OR ALTER PROCEDURE dbo.SimulateActivity
    @NewOrders INT = 5
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @i INT = 0, @r INT, @order BIGINT, @items INT, @createdAt DATETIME2(3), @channel VARCHAR(10);
    DECLARE @moved TABLE (OrderId BIGINT PRIMARY KEY, NewStatus VARCHAR(12), TotalAmount DECIMAL(14, 2));

    -- ~10% of the calls also sync one back-dated order from an offline store
    DECLARE @backdated INT = CASE WHEN ABS(CHECKSUM(NEWID())) % 10 = 0 THEN 1 ELSE 0 END;

    WHILE @i < @NewOrders + @backdated
    BEGIN
        IF @i = @NewOrders   -- the extra, back-dated order
            SELECT @createdAt = DATEADD(MINUTE, -(60 + ABS(CHECKSUM(NEWID())) % 4260), @now),
                   @channel = 'store';
        ELSE
        BEGIN
            -- Random number first: CHOOSE becomes a CASE, and NEWID() inside it
            -- would be drawn again for each branch (and could match none).
            SET @r = ABS(CHECKSUM(NEWID())) % 4;
            SELECT @createdAt = @now, @channel = CHOOSE(1 + @r, 'web', 'app', 'store', NULL);
        END

        BEGIN TRAN;
        INSERT dbo.Orders (CustomerId, Status, Channel, CreatedAt, UpdatedAt)
        SELECT TOP (1) CustomerId, 'CREATED', @channel, @createdAt, @now
        FROM dbo.Customers ORDER BY NEWID();
        SET @order = SCOPE_IDENTITY();
        SET @items = 1 + ABS(CHECKSUM(NEWID())) % 4;

        INSERT dbo.OrderItems (OrderId, ItemNumber, ProductId, Quantity, UnitPrice)
        SELECT @order, ROW_NUMBER() OVER (ORDER BY p.ProductId), p.ProductId,
               1 + ABS(CHECKSUM(NEWID())) % 3, p.Price
        FROM (SELECT TOP (@items) ProductId, Price FROM dbo.Products
              WHERE Active = 1 ORDER BY NEWID()) p;

        UPDATE s SET Quantity = s.Quantity - i.Quantity, UpdatedAt = @now
        FROM dbo.Stock s JOIN dbo.OrderItems i ON i.ProductId = s.ProductId
        WHERE i.OrderId = @order;

        UPDATE dbo.Orders
        SET TotalAmount = (SELECT SUM(Quantity * UnitPrice) FROM dbo.OrderItems
                           WHERE OrderId = @order)
        WHERE OrderId = @order;
        COMMIT;
        SET @i += 1;
    END

    -- Moves some open orders to the next status, with its side effects
    BEGIN TRAN;
    UPDATE o SET
        Status = CASE o.Status WHEN 'CREATED' THEN 'PAID' WHEN 'PAID' THEN 'SHIPPED'
                               ELSE 'DELIVERED' END,
        UpdatedAt = @now
    OUTPUT inserted.OrderId, inserted.Status, inserted.TotalAmount INTO @moved
    FROM (SELECT TOP (3) * FROM dbo.Orders
          WHERE Status IN ('CREATED', 'PAID', 'SHIPPED') ORDER BY NEWID()) o;

    INSERT dbo.Payments (OrderId, Method, Amount, Status, PaidAt, UpdatedAt)
    SELECT OrderId, CHOOSE(1 + ABS(CHECKSUM(OrderId * 7919)) % 3, 'pix', 'credit_card', 'boleto'),
           TotalAmount, 'CAPTURED', @now, @now
    FROM @moved WHERE NewStatus = 'PAID';

    INSERT dbo.Shipments (OrderId, CarrierId, TrackingCode, ShippedAt, UpdatedAt)
    SELECT m.OrderId, c.CarrierId, CONCAT('TRK', FORMAT(m.OrderId, '0000000000')), @now, @now
    FROM @moved m
    CROSS APPLY (SELECT TOP (1) CarrierId FROM dbo.Carriers
                 WHERE m.OrderId IS NOT NULL ORDER BY NEWID()) c   -- correlated: one draw per row
    WHERE m.NewStatus = 'SHIPPED';

    UPDATE s SET DeliveredAt = @now, UpdatedAt = @now
    FROM dbo.Shipments s JOIN @moved m ON m.OrderId = s.OrderId
    WHERE m.NewStatus = 'DELIVERED' AND s.DeliveredAt IS NULL;
    COMMIT;

    -- Cancels an open order in ~20% of the calls; a paid one is refunded
    IF ABS(CHECKSUM(NEWID())) % 5 = 0
    BEGIN
        DELETE @moved;
        BEGIN TRAN;
        UPDATE o SET Status = 'CANCELLED', UpdatedAt = @now
        OUTPUT inserted.OrderId, inserted.Status, inserted.TotalAmount INTO @moved
        FROM (SELECT TOP (1) * FROM dbo.Orders WHERE Status IN ('CREATED', 'PAID')
              ORDER BY NEWID()) o;
        UPDATE p SET Status = 'REFUNDED', UpdatedAt = @now
        FROM dbo.Payments p JOIN @moved m ON m.OrderId = p.OrderId;
        COMMIT;
    END

    -- Removes the last item of an open order in ~10% of the calls
    IF ABS(CHECKSUM(NEWID())) % 10 = 0
    BEGIN
        DECLARE @target BIGINT = (SELECT TOP (1) OrderId FROM dbo.OrderItems i
                                  WHERE ItemNumber > 1 AND EXISTS (SELECT 1 FROM dbo.Orders o
                                      WHERE o.OrderId = i.OrderId AND o.Status = 'CREATED')
                                  ORDER BY NEWID());
        IF @target IS NOT NULL
        BEGIN
            BEGIN TRAN;
            DELETE dbo.OrderItems WHERE OrderId = @target
               AND ItemNumber = (SELECT MAX(ItemNumber) FROM dbo.OrderItems WHERE OrderId = @target);
            UPDATE dbo.Orders SET UpdatedAt = @now,
                TotalAmount = (SELECT SUM(Quantity * UnitPrice) FROM dbo.OrderItems
                               WHERE OrderId = @target)
            WHERE OrderId = @target;
            COMMIT;
        END
    END

    -- Restocks low stock
    UPDATE dbo.Stock SET Quantity = Quantity + 300, UpdatedAt = @now
    WHERE Quantity < 20;

    -- Scheduled-load tables: rarer changes
    IF ABS(CHECKSUM(NEWID())) % 10 = 0
        UPDATE p SET Price = CAST(Price * (0.95 + (ABS(CHECKSUM(NEWID())) % 11) / 100.0)
                                  AS DECIMAL(12, 2)),
                     UpdatedAt = @now
        FROM (SELECT TOP (1) * FROM dbo.Products ORDER BY NEWID()) p;

    IF ABS(CHECKSUM(NEWID())) % 10 = 0
        INSERT dbo.Customers (Name, Email, City, State)
        VALUES (N'New customer', CONCAT('new', NEXT VALUE FOR dbo.NewCustomerSeq,
                                        '@example.com'), N'Campinas', 'SP');

    IF ABS(CHECKSUM(NEWID())) % 10 = 0
        UPDATE c SET City = N'Florianópolis', State = 'SC', UpdatedAt = @now
        FROM (SELECT TOP (1) * FROM dbo.Customers ORDER BY NEWID()) c;
END
GO

-- Bulk load of past data. Inserts in transactions of @BatchSize orders, like a
-- real backfill (a single huge transaction would bloat the log and reach CDC
-- as one giant block). Each order gets 1 to 4 items; paid orders get a
-- payment, shipped ones a shipment; the status depends on the order's age.
--   @Orders        orders to create, spread over the last @Days days
--   @NewCustomers  customers to add first (CreatedAt in the past)
--   @NewProducts   products to add first (with stock)
CREATE OR ALTER PROCEDURE dbo.GenerateHistory
    @Orders INT = 100000,
    @Days INT = 180,
    @NewCustomers INT = 20000,
    @NewProducts INT = 300,
    @BatchSize INT = 5000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @tag VARCHAR(14) = FORMAT(@now, 'yyyyMMddHHmmss');
    DECLARE @msg NVARCHAR(200);

    -- 1. Customers
    IF @NewCustomers > 0
    BEGIN
        ;WITH n AS (SELECT TOP (@NewCustomers) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                    FROM sys.all_objects a CROSS JOIN sys.all_objects b)
        SELECT i, ABS(CHECKSUM(NEWID())) % 10 AS city,
               DATEADD(MINUTE, -(ABS(CHECKSUM(NEWID())) % (@Days * 1440)), @now) AS createdAt
        INTO #newCustomers FROM n;

        INSERT dbo.Customers (Name, Email, City, State, CreatedAt, UpdatedAt)
        SELECT CONCAT(CHOOSE(1 + i % 12, N'Ana', N'Bruno', N'Carla', N'Diego', N'Elisa', N'Fábio',
                             N'Gabriela', N'Hugo', N'Isabela', N'João', N'Larissa', N'Marcos'), N' ',
                      CHOOSE(1 + (i / 12) % 10, N'Silva', N'Souza', N'Oliveira', N'Santos', N'Lima',
                             N'Pereira', N'Costa', N'Almeida', N'Ribeiro', N'Carvalho')),
               CONCAT('h', @tag, '-', i, '@example.com'),
               CHOOSE(1 + city, N'São Paulo', N'Rio de Janeiro', N'Belo Horizonte', N'Curitiba',
                      N'Porto Alegre', N'Recife', N'Salvador', N'Brasília', N'Goiânia', N'Fortaleza'),
               CHOOSE(1 + city, 'SP', 'RJ', 'MG', 'PR', 'RS', 'PE', 'BA', 'DF', 'GO', 'CE'),
               createdAt, createdAt
        FROM #newCustomers;
        SET @msg = CONCAT(@NewCustomers, ' customers created');
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
    END

    -- 2. Products (and their stock)
    IF @NewProducts > 0
    BEGIN
        DECLARE @base INT = (SELECT MAX(CAST(SUBSTRING(Sku, 5, 16) AS INT)) FROM dbo.Products);
        ;WITH n AS (SELECT TOP (@NewProducts) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                    FROM sys.all_objects)
        INSERT dbo.Products (Sku, Name, CategoryId, Price)
        SELECT CONCAT('SKU-', FORMAT(@base + i, '00000')),
               CONCAT(CHOOSE(1 + i % 8, N'Laptop', N'Mouse', N'Monitor', N'Headset',
                             N'Chair', N'SSD', N'Router', N'Cable'), N' model ', @base + i),
               1 + i % 8,
               CAST(ROUND(CHOOSE(1 + i % 8, 3500, 80, 1200, 300, 1100, 450, 380, 40)
                          * (0.7 + (ABS(CHECKSUM(NEWID())) % 60) / 100.0), 2) AS DECIMAL(12, 2))
        FROM n;

        INSERT dbo.Stock (ProductId, Quantity)
        SELECT p.ProductId, 50 + ABS(CHECKSUM(NEWID())) % 500
        FROM dbo.Products p WHERE NOT EXISTS (SELECT 1 FROM dbo.Stock s WHERE s.ProductId = p.ProductId);
        SET @msg = CONCAT(@NewProducts, ' products created');
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
    END

    -- 3. Orders, in batches. Customers, products and carriers are numbered
    --    1..N so a random number picks one with a simple join.
    SELECT CustomerId, ROW_NUMBER() OVER (ORDER BY CustomerId) AS rn INTO #c FROM dbo.Customers;
    SELECT ProductId, Price, ROW_NUMBER() OVER (ORDER BY ProductId) AS rn INTO #p
    FROM dbo.Products WHERE Active = 1;
    SELECT CarrierId, ROW_NUMBER() OVER (ORDER BY CarrierId) AS rn INTO #k FROM dbo.Carriers;
    CREATE UNIQUE CLUSTERED INDEX c ON #c (rn);
    CREATE UNIQUE CLUSTERED INDEX p ON #p (rn);
    DECLARE @cc INT = (SELECT COUNT(*) FROM #c), @pc INT = (SELECT COUNT(*) FROM #p),
            @kc INT = (SELECT COUNT(*) FROM #k);

    CREATE TABLE #batch (n INT PRIMARY KEY, CustomerId INT, Status VARCHAR(12), Channel VARCHAR(10),
                         CreatedAt DATETIME2(3), UpdatedAt DATETIME2(3));
    CREATE TABLE #new (OrderId BIGINT PRIMARY KEY, Status VARCHAR(12), CreatedAt DATETIME2(3));
    CREATE TABLE #items (OrderId BIGINT, ItemNumber SMALLINT, prn INT, qty INT,
                         PRIMARY KEY (OrderId, ItemNumber));
    DECLARE @done INT = 0, @size INT;

    WHILE @done < @Orders
    BEGIN
        SET @size = IIF(@Orders - @done < @BatchSize, @Orders - @done, @BatchSize);
        TRUNCATE TABLE #batch;
        TRUNCATE TABLE #new;

        -- Random values are materialized first: SQL Server may evaluate NEWID()
        -- again each time a CTE column is referenced.
        ;WITH n AS (SELECT TOP (@size) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                    FROM sys.all_objects a CROSS JOIN sys.all_objects b)
        INSERT #batch (n, CustomerId, Status, Channel, CreatedAt)
        SELECT n.i, 1 + ABS(CHECKSUM(NEWID())) % @cc, CAST(ABS(CHECKSUM(NEWID())) % 100 AS VARCHAR(12)),
               CAST(ABS(CHECKSUM(NEWID())) % 4 AS VARCHAR(10)),   -- channel number, mapped below
               DATEADD(SECOND, -(ABS(CHECKSUM(NEWID())) % (@Days * 86400)), @now)
        FROM n;

        -- Status holds the dice roll (0-99) and Channel a number (0-3) until here
        UPDATE b SET
            CustomerId = c.CustomerId,
            Channel = CHOOSE(1 + CAST(b.Channel AS INT), 'web', 'app', 'store', NULL),
            Status = CASE
                WHEN CAST(b.Status AS INT) < 6 THEN 'CANCELLED'
                WHEN DATEDIFF(HOUR, b.CreatedAt, @now) < 24
                    THEN CHOOSE(1 + CAST(b.Status AS INT) % 3, 'CREATED', 'PAID', 'SHIPPED')
                WHEN DATEDIFF(DAY, b.CreatedAt, @now) < 7
                    THEN CHOOSE(1 + CAST(b.Status AS INT) % 3, 'PAID', 'SHIPPED', 'DELIVERED')
                ELSE 'DELIVERED' END
        FROM #batch b JOIN #c c ON c.rn = b.CustomerId;
        UPDATE #batch SET UpdatedAt = IIF(Status = 'CREATED', CreatedAt,
                                          LEAST(DATEADD(HOUR, 6 + n % 160, CreatedAt), @now));

        BEGIN TRAN;
        INSERT dbo.Orders (CustomerId, Status, Channel, CreatedAt, UpdatedAt)
        OUTPUT inserted.OrderId, inserted.Status, inserted.CreatedAt INTO #new
        SELECT CustomerId, Status, Channel, CreatedAt, UpdatedAt FROM #batch ORDER BY CreatedAt;

        TRUNCATE TABLE #items;
        INSERT #items (OrderId, ItemNumber, prn, qty)
        SELECT o.OrderId, k.k, 1 + ABS(CHECKSUM(NEWID())) % @pc, 1 + ABS(CHECKSUM(NEWID())) % 3
        FROM #new o
        JOIN (VALUES (1), (2), (3), (4)) k(k) ON k.k <= 1 + o.OrderId % 4;

        INSERT dbo.OrderItems (OrderId, ItemNumber, ProductId, Quantity, UnitPrice)
        SELECT i.OrderId, i.ItemNumber, p.ProductId, i.qty, p.Price
        FROM #items i JOIN #p p ON p.rn = i.prn;

        UPDATE o SET TotalAmount = t.total
        FROM dbo.Orders o
        JOIN (SELECT i.OrderId, SUM(i.Quantity * i.UnitPrice) AS total
              FROM dbo.OrderItems i JOIN #new n ON n.OrderId = i.OrderId
              GROUP BY i.OrderId) t ON t.OrderId = o.OrderId;

        INSERT dbo.Payments (OrderId, Method, Amount, Status, PaidAt, UpdatedAt)
        SELECT n.OrderId, CHOOSE(1 + ABS(CHECKSUM(n.OrderId * 7919)) % 3, 'pix', 'credit_card', 'boleto'),
               o.TotalAmount, IIF(n.Status = 'CANCELLED', 'REFUNDED', 'CAPTURED'),
               LEAST(DATEADD(MINUTE, 5 + n.OrderId % 600, n.CreatedAt), @now), o.UpdatedAt
        FROM #new n JOIN dbo.Orders o ON o.OrderId = n.OrderId
        WHERE n.Status IN ('PAID', 'SHIPPED', 'DELIVERED')
           OR (n.Status = 'CANCELLED' AND n.OrderId % 2 = 0);

        INSERT dbo.Shipments (OrderId, CarrierId, TrackingCode, ShippedAt, DeliveredAt, UpdatedAt)
        SELECT s.OrderId, k.CarrierId, CONCAT('TRK', FORMAT(s.OrderId, '0000000000')), s.shippedAt,
               IIF(s.Status = 'DELIVERED',
                   LEAST(DATEADD(HOUR, 24 + ABS(CHECKSUM(s.OrderId * 31)) % 144, s.shippedAt), @now),
                   NULL),
               s.updatedAt
        FROM (SELECT n.OrderId, n.Status, o.UpdatedAt AS updatedAt,
                     LEAST(DATEADD(HOUR, 4 + n.OrderId % 68, n.CreatedAt), @now) AS shippedAt
              FROM #new n JOIN dbo.Orders o ON o.OrderId = n.OrderId
              WHERE n.Status IN ('SHIPPED', 'DELIVERED')) s
        JOIN #k k ON k.rn = 1 + s.OrderId % @kc;
        COMMIT;

        SET @done += @size;
        SET @msg = CONCAT(@done, ' of ', @Orders, ' orders');
        RAISERROR(@msg, 0, 1) WITH NOWAIT;
    END
END
GO
