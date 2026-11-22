-- Sample database that mimics a sales ERP. Idempotent.
--   CDC tables (near real time): Orders, OrderItems, Stock, Payments, Shipments
--   Scheduled-load tables (Airflow): Customers, Products, Categories, Carriers
SET NOCOUNT ON;
IF DB_ID(N'$(DB)') IS NULL
BEGIN
    CREATE DATABASE [$(DB)];
    PRINT 'database $(DB) created';
END
GO
USE [$(DB)];
GO

IF OBJECT_ID(N'dbo.Categories') IS NULL
CREATE TABLE dbo.Categories (
    CategoryId   INT IDENTITY PRIMARY KEY,
    Name         NVARCHAR(60)  NOT NULL UNIQUE,
    UpdatedAt    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'dbo.Products') IS NULL
CREATE TABLE dbo.Products (
    ProductId    INT IDENTITY PRIMARY KEY,
    Sku          VARCHAR(20)    NOT NULL UNIQUE,
    Name         NVARCHAR(120)  NOT NULL,
    CategoryId   INT            NOT NULL REFERENCES dbo.Categories (CategoryId),
    Price        DECIMAL(12, 2) NOT NULL,
    Active       BIT            NOT NULL DEFAULT 1,
    UpdatedAt    DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'dbo.Customers') IS NULL
CREATE TABLE dbo.Customers (
    CustomerId   INT IDENTITY PRIMARY KEY,
    Name         NVARCHAR(120) NOT NULL,
    Email        VARCHAR(120)  NOT NULL,
    City         NVARCHAR(80)  NOT NULL,
    State        CHAR(2)       NOT NULL,   -- Brazilian state (UF)
    CreatedAt    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'dbo.Orders') IS NULL
CREATE TABLE dbo.Orders (
    OrderId      BIGINT IDENTITY PRIMARY KEY,
    CustomerId   INT            NOT NULL REFERENCES dbo.Customers (CustomerId),
    Status       VARCHAR(12)    NOT NULL,   -- CREATED, PAID, SHIPPED, DELIVERED, CANCELLED
    Channel      VARCHAR(10)    NULL,       -- web, app, store
    TotalAmount  DECIMAL(14, 2) NOT NULL DEFAULT 0,
    CreatedAt    DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt    DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'dbo.OrderItems') IS NULL
CREATE TABLE dbo.OrderItems (
    OrderId      BIGINT         NOT NULL REFERENCES dbo.Orders (OrderId),
    ItemNumber   SMALLINT       NOT NULL,
    ProductId    INT            NOT NULL REFERENCES dbo.Products (ProductId),
    Quantity     INT            NOT NULL,
    UnitPrice    DECIMAL(12, 2) NOT NULL,
    CONSTRAINT PK_OrderItems PRIMARY KEY (OrderId, ItemNumber)
);

IF OBJECT_ID(N'dbo.NewCustomerSeq') IS NULL
CREATE SEQUENCE dbo.NewCustomerSeq AS INT START WITH 1;

IF OBJECT_ID(N'dbo.Stock') IS NULL
CREATE TABLE dbo.Stock (
    ProductId    INT          NOT NULL PRIMARY KEY REFERENCES dbo.Products (ProductId),
    Quantity     INT          NOT NULL,
    UpdatedAt    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'dbo.Carriers') IS NULL
CREATE TABLE dbo.Carriers (
    CarrierId    INT IDENTITY PRIMARY KEY,
    Name         NVARCHAR(60) NOT NULL UNIQUE,
    ServiceLevel VARCHAR(10)  NOT NULL,   -- standard, express
    UpdatedAt    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);

-- One payment per paid order; REFUNDED when a paid order is cancelled.
IF OBJECT_ID(N'dbo.Payments') IS NULL
CREATE TABLE dbo.Payments (
    PaymentId    BIGINT IDENTITY PRIMARY KEY,
    OrderId      BIGINT         NOT NULL REFERENCES dbo.Orders (OrderId),
    Method       VARCHAR(12)    NOT NULL,   -- pix, credit_card, boleto
    Amount       DECIMAL(14, 2) NOT NULL,
    Status       VARCHAR(10)    NOT NULL,   -- CAPTURED, REFUNDED
    PaidAt       DATETIME2(3)   NOT NULL,
    UpdatedAt    DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    INDEX IX_Payments_OrderId (OrderId)
);

-- Created when the order ships; DeliveredAt is filled in on delivery (an UPDATE in CDC).
IF OBJECT_ID(N'dbo.Shipments') IS NULL
CREATE TABLE dbo.Shipments (
    ShipmentId   BIGINT IDENTITY PRIMARY KEY,
    OrderId      BIGINT       NOT NULL REFERENCES dbo.Orders (OrderId),
    CarrierId    INT          NOT NULL REFERENCES dbo.Carriers (CarrierId),
    TrackingCode VARCHAR(20)  NOT NULL,
    ShippedAt    DATETIME2(3) NOT NULL,
    DeliveredAt  DATETIME2(3) NULL,
    UpdatedAt    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    INDEX IX_Shipments_OrderId (OrderId)
);
GO

-- ----------------------------------------------------------------- data ----
IF NOT EXISTS (SELECT 1 FROM dbo.Categories)
BEGIN
    INSERT dbo.Categories (Name) VALUES
        (N'Computers'), (N'Peripherals'), (N'Monitors'), (N'Audio'),
        (N'Office furniture'), (N'Storage'), (N'Networking'), (N'Accessories');

    ;WITH n AS (SELECT TOP (200) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                FROM sys.all_objects)
    INSERT dbo.Products (Sku, Name, CategoryId, Price)
    SELECT CONCAT('SKU-', FORMAT(i, '00000')),
           CONCAT(CHOOSE(1 + i % 8, N'Laptop', N'Mouse', N'Monitor', N'Headset',
                         N'Chair', N'SSD', N'Router', N'Cable'), N' model ', i),
           1 + i % 8,
           CAST(ROUND(CHOOSE(1 + i % 8, 3500, 80, 1200, 300, 1100, 450, 380, 40)
                      * (0.7 + (ABS(CHECKSUM(NEWID())) % 60) / 100.0), 2) AS DECIMAL(12, 2))
    FROM n;

    INSERT dbo.Stock (ProductId, Quantity)
    SELECT ProductId, 50 + ABS(CHECKSUM(NEWID())) % 500 FROM dbo.Products;

    ;WITH n AS (SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                FROM sys.all_objects a CROSS JOIN sys.all_objects b)
    INSERT dbo.Customers (Name, Email, City, State)
    SELECT CONCAT(CHOOSE(1 + i % 10, N'Ana', N'Bruno', N'Carla', N'Diego', N'Elisa',
                         N'Fábio', N'Gabriela', N'Hugo', N'Isabela', N'João'), N' ',
                  CHOOSE(1 + (i / 10) % 8, N'Silva', N'Souza', N'Oliveira', N'Santos',
                         N'Lima', N'Pereira', N'Costa', N'Almeida')),
           CONCAT('customer', i, '@example.com'),
           CHOOSE(1 + i % 6, N'São Paulo', N'Rio de Janeiro', N'Belo Horizonte',
                  N'Curitiba', N'Porto Alegre', N'Recife'),
           CHOOSE(1 + i % 6, 'SP', 'RJ', 'MG', 'PR', 'RS', 'PE')
    FROM n;

    PRINT 'initial data loaded';
END
GO

-- Carriers came after the first version: separate block, so it also runs on an existing database.
IF NOT EXISTS (SELECT 1 FROM dbo.Carriers)
    INSERT dbo.Carriers (Name, ServiceLevel) VALUES
        (N'FastShip Express', 'express'), (N'BlueRoute', 'standard'),
        (N'Sul Cargo', 'standard'), (N'Norte Log', 'standard'), (N'City Courier', 'express');
GO
