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

USE [tfm_lesiones];
GO

-- Creación de la tabla temporada
IF OBJECT_ID('dbo.temporada', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[temporada](
        [id_temporada] [int] NULL,
        [fecha_inicio] [date] NULL,
        [fecha_fin] [date] NULL,
        [temporada] [varchar](max) NULL
    );
END
GO

Inserción de datos en la tabla temporada
IF NOT EXISTS (SELECT * FROM [dbo].[temporada] WHERE [id_temporada] = 2010)
BEGIN
    INSERT INTO [dbo].[temporada] ([id_temporada], [fecha_inicio], [fecha_fin], [temporada])
    VALUES
        (2010, '2010-08-01', '2011-07-31', '10/11'),
        (2011, '2011-08-01', '2012-07-31', '11/12'),
        (2012, '2012-08-01', '2013-07-31', '12/13'),
        (2013, '2013-08-01', '2014-07-31', '13/14'),
        (2014, '2014-08-01', '2015-07-31', '14/15'),
        (2015, '2015-08-01', '2016-07-31', '15/16'),
        (2016, '2016-08-01', '2017-07-31', '16/17'),
        (2017, '2017-08-01', '2018-07-31', '17/18'),
        (2018, '2018-08-01', '2019-07-31', '18/19'),
        (2019, '2019-08-01', '2020-07-31', '19/20'),
        (2020, '2020-08-01', '2021-07-31', '20/21'),
        (2021, '2021-08-01', '2022-07-31', '21/22'),
        (2022, '2022-08-01', '2023-07-31', '22/23'),
        (2023, '2023-08-01', '2024-07-31', '23/24'),
        (2024, '2024-08-01', '2025-07-31', '24/25');
END
GO

-- Creación de la tabla tipos_lesion
IF OBJECT_ID('dbo.tipos_lesion', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[tipos_lesion](
        [id] [int] NULL,
        [descripcion] [varchar](max) NULL
    );
END
GO

-- Inserción de datos en la tabla tipos_lesion
IF NOT EXISTS (SELECT * FROM [dbo].[tipos_lesion] WHERE [id] = 1)
BEGIN
    INSERT INTO [dbo].[tipos_lesion] ([id], [descripcion])
    VALUES
        (1, 'Lesión Muscular'),
        (2, 'Lesión Ósea'),
        (3, 'Golpe'),
        (4, 'Problemas Cardíacos'),
        (5, 'Enfermedad Vírica'),
        (6, 'Lesión Ligamentosa'),
        (7, 'Otras lesiones');
END
GO
