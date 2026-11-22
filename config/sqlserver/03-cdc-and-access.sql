-- Enables CDC on the near-real-time tables and creates the data lake logins
-- with least privilege. Idempotent (passwords are reapplied from .env on
-- every run).
--
-- On the REAL SQL Server this script is a runbook for the DBA: enabling CDC
-- requires sysadmin (database level) and db_owner (table level).
SET NOCOUNT ON;
USE [$(DB)];
GO

-- 1. CDC on the database
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled on the database';
END
GO

-- 2. CDC "gatekeeper" role: only its members can read the captured changes
IF DATABASE_PRINCIPAL_ID(N'cdc_reader') IS NULL CREATE ROLE cdc_reader;
IF DATABASE_PRINCIPAL_ID(N'batch_reader') IS NULL CREATE ROLE batch_reader;
GO

-- 3. CDC on the near-real-time tables
DECLARE @t SYSNAME;
DECLARE cdc_tables CURSOR LOCAL FOR
    SELECT v.name FROM (VALUES (N'Orders'), (N'OrderItems'), (N'Stock'), (N'Payments'), (N'Shipments')) v(name)
    JOIN sys.tables t ON t.name = v.name AND t.schema_id = SCHEMA_ID(N'dbo')
    WHERE t.is_tracked_by_cdc = 0;
OPEN cdc_tables;
FETCH NEXT FROM cdc_tables INTO @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo', @source_name = @t,
        @role_name = N'cdc_reader', @supports_net_changes = 1;
    PRINT CONCAT('CDC enabled on dbo.', @t);
    FETCH NEXT FROM cdc_tables INTO @t;
END
CLOSE cdc_tables; DEALLOCATE cdc_tables;
GO

-- 3b. Debezium signal table. Used to request the initial load (snapshot) of
--     a table added to CDC after the connector was already running: one
--     INSERT here and Debezium copies the existing rows without pausing the
--     capture. Debezium also writes control markers to it during that copy,
--     which is why the CDC login needs INSERT on it.
IF OBJECT_ID(N'dbo.DebeziumSignal') IS NULL
    CREATE TABLE dbo.DebeziumSignal (
        id   VARCHAR(42) PRIMARY KEY,
        type VARCHAR(32) NOT NULL,
        data VARCHAR(2048) NULL
    );
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DebeziumSignal' AND is_tracked_by_cdc = 1)
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo', @source_name = N'DebeziumSignal',
        @role_name = N'cdc_reader', @supports_net_changes = 0;
GO

-- 4. Role permissions
--    cdc_reader: CDC tables (initial load/snapshot) + cdc schema (changes)
GRANT SELECT ON dbo.Orders     TO cdc_reader;
GRANT SELECT ON dbo.OrderItems TO cdc_reader;
GRANT SELECT ON dbo.Stock      TO cdc_reader;
GRANT SELECT ON dbo.Payments   TO cdc_reader;
GRANT SELECT ON dbo.Shipments  TO cdc_reader;
GRANT SELECT ON SCHEMA::cdc    TO cdc_reader;
GRANT VIEW DATABASE PERFORMANCE STATE TO cdc_reader;
GRANT SELECT, INSERT ON dbo.DebeziumSignal TO cdc_reader;
--    batch_reader: only the scheduled-load tables
GRANT SELECT ON dbo.Customers  TO batch_reader;
GRANT SELECT ON dbo.Products   TO batch_reader;
GRANT SELECT ON dbo.Categories TO batch_reader;
GRANT SELECT ON dbo.Carriers   TO batch_reader;
GO

-- 5. Logins (SQL Server authentication) and database users
USE master;
IF SUSER_ID(N'$(CDC_USER)') IS NULL
    CREATE LOGIN [$(CDC_USER)] WITH PASSWORD = N'$(CDC_PASSWORD)',
        DEFAULT_DATABASE = [$(DB)], CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;
ELSE
    ALTER LOGIN [$(CDC_USER)] WITH PASSWORD = N'$(CDC_PASSWORD)';

IF SUSER_ID(N'$(BATCH_USER)') IS NULL
    CREATE LOGIN [$(BATCH_USER)] WITH PASSWORD = N'$(BATCH_PASSWORD)',
        DEFAULT_DATABASE = [$(DB)], CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;
ELSE
    ALTER LOGIN [$(BATCH_USER)] WITH PASSWORD = N'$(BATCH_PASSWORD)';

-- Read-only server state: Debezium uses it to check that SQL Server Agent
-- (which feeds CDC) is running.
GRANT VIEW SERVER PERFORMANCE STATE TO [$(CDC_USER)];
GO
USE [$(DB)];
IF DATABASE_PRINCIPAL_ID(N'$(CDC_USER)') IS NULL
    CREATE USER [$(CDC_USER)] FOR LOGIN [$(CDC_USER)];
IF DATABASE_PRINCIPAL_ID(N'$(BATCH_USER)') IS NULL
    CREATE USER [$(BATCH_USER)] FOR LOGIN [$(BATCH_USER)];
ALTER ROLE cdc_reader   ADD MEMBER [$(CDC_USER)];
ALTER ROLE batch_reader ADD MEMBER [$(BATCH_USER)];
PRINT 'logins $(CDC_USER) and $(BATCH_USER) ready';
GO
