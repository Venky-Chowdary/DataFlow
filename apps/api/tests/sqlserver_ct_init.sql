-- Enable Change Tracking for DataFlow CDC integration tests.
-- Run against the sqlserver compose service after it is healthy:
--   sqlcmd -S localhost -U sa -P 'DataFlow_CDC_2022!' -C -i sqlserver_ct_init.sql

IF DB_ID('dataflow') IS NULL
    CREATE DATABASE dataflow;
GO

USE dataflow;
GO

IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
    ALTER DATABASE dataflow SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
GO

IF OBJECT_ID('dbo.cdc_orders') IS NULL
BEGIN
    CREATE TABLE dbo.cdc_orders (
        id INT NOT NULL PRIMARY KEY,
        amount DECIMAL(12, 2) NOT NULL,
        status NVARCHAR(32) NOT NULL DEFAULT N'open'
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.change_tracking_tables ct
    JOIN sys.tables t ON t.object_id = ct.object_id
    WHERE t.name = 'cdc_orders'
)
    ALTER TABLE dbo.cdc_orders ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
GO
