CREATE DATABASE $(MSSQL_DB);
GO
CREATE LOGIN $(MSSQL_USER) WITH PASSWORD = 'scraper1234A';
GO

USE $(MSSQL_DB);
GO

CREATE USER $(MSSQL_USER) FOR LOGIN $(MSSQL_USER);
GO
ALTER SERVER ROLE sysadmin ADD MEMBER $(MSSQL_USER);
GO


-- Comprobar y eliminar el procedimiento almacenado si ya existe
IF OBJECT_ID('EliminarDuplicados', 'P') IS NOT NULL
    DROP PROCEDURE EliminarDuplicados;
GO

-- Creación del procedimiento almacenado
CREATE PROCEDURE EliminarDuplicados
    @tableName NVARCHAR(128),
    @idColumn NVARCHAR(128),
    @columns NVARCHAR(MAX)
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SET @sql = N'
    WITH Duplicados AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY ' + @columns + ' ORDER BY (SELECT NULL)) AS fila
        FROM ' + @tableName + '
    )
    DELETE 
    FROM ' + @tableName + '
    WHERE ' + @idColumn + ' IN (SELECT ' + @idColumn + ' FROM Duplicados WHERE fila > 1);
    ';

    EXEC sp_executesql @sql;
END;
GO