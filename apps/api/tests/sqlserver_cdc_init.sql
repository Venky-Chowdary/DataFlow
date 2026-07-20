-- Enable SQL Server *native* CDC for DataFlow integration tests.
-- Requires Developer/Enterprise edition. Capture jobs normally need SQL Agent;
-- tests call EXEC sys.sp_cdc_scan after DML when Agent is unavailable.
--
-- Run after sqlserver_ct_init.sql (creates database dataflow):
--   sqlcmd -S localhost -U sa -P 'DataFlow_CDC_2022!' -C -i sqlserver_cdc_init.sql

USE dataflow;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'dataflow' AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

IF OBJECT_ID(N'dbo.cdc_native_orders', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.cdc_native_orders (
        id INT NOT NULL PRIMARY KEY,
        amount DECIMAL(12, 2) NOT NULL,
        status NVARCHAR(32) NOT NULL DEFAULT N'open'
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables ct
    JOIN sys.tables t ON t.object_id = ct.source_object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = N'cdc_native_orders' AND s.name = N'dbo'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'cdc_native_orders',
        @role_name = NULL,
        @supports_net_changes = 1;
END
GO

-- Best-effort Agent start (ignored when unavailable in containers).
BEGIN TRY
    EXEC xp_servicecontrol N'START', N'SQLServerAGENT';
END TRY
BEGIN CATCH
    -- Agent optional; tests use sys.sp_cdc_scan.
END CATCH
GO
