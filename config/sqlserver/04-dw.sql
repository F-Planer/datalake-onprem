-- DW database: receives the data lake's gold layer; this is where Tableau connects.
-- Idempotent. On the real SQL Server, a runbook for the DBA.
--   datalake_dw: the lake's write login, owner of this database only (creates and reloads tables)
--   dw_reader:   read role for BI tools and analysts
SET NOCOUNT ON;
IF DB_ID(N'$(DW_DB)') IS NULL
BEGIN
    CREATE DATABASE [$(DW_DB)];
    ALTER DATABASE [$(DW_DB)] SET RECOVERY SIMPLE;   -- can be reloaded from the lake
    PRINT 'database $(DW_DB) created';
END
GO
USE master;
IF SUSER_ID(N'$(DW_USER)') IS NULL
    CREATE LOGIN [$(DW_USER)] WITH PASSWORD = N'$(DW_PASSWORD)',
        DEFAULT_DATABASE = [$(DW_DB)], CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;
ELSE
    ALTER LOGIN [$(DW_USER)] WITH PASSWORD = N'$(DW_PASSWORD)';
GO
USE [$(DW_DB)];
IF SCHEMA_ID(N'stg') IS NULL EXEC (N'CREATE SCHEMA stg');   -- staging tables for the load
IF DATABASE_PRINCIPAL_ID(N'$(DW_USER)') IS NULL
    CREATE USER [$(DW_USER)] FOR LOGIN [$(DW_USER)];
ALTER ROLE db_owner ADD MEMBER [$(DW_USER)];
IF DATABASE_PRINCIPAL_ID(N'dw_reader') IS NULL CREATE ROLE dw_reader;
GRANT SELECT ON SCHEMA::dbo TO dw_reader;
PRINT 'DW $(DW_DB) and login $(DW_USER) ready';
GO
