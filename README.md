
## Introducción

El siguiente es un ejemplo de como hacer un proceso ETL usando Visual Studio 2022 junto con su herramienta SSIS.

El proceso consiste en cargar un archivo excel como tabla dentro de la base de datos Microsoft SQL Server, seguidamente, hay que identificar duplicados, desechar duplicados.

Seguidamente a pedido del cliente en su momento se tenian que convertir columnas a filas como parte de sus análisis para esto se uso la funcion "PIVOT" de MSSQL.
![Imgur](https://i.imgur.com/RCNzAiC.png)

1. Hay que subir los archivos excel a SQL usando las herramientas de SSIS. Aqui hay que tener detalle con los conectores a usar:


![Imgur](https://i.imgur.com/ESguGDC.png?1)

2. Creamos dos tablas una para cargar los datos de la tabla excel y otra ya con datos duplicados filtrados: 

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Desempeno_Final')
BEGIN
    CREATE TABLE Desempeno_Final (
        [Periodo] float,
        [Unidad de Negocio] nvarchar(255),
        [Empresa] nvarchar(255),
        [Nro de documento] nvarchar(255),
        [Apellidos_Nombres] nvarchar(255),
        [A# Scorecard - Objetivos] float,
        [Comp 1# Estratega] float,
        [Comp 2# Ejecutor] float,
        [Comp 3# Desarrollador de Personas] float,
        [Comp 4# Comunicador] float,
        [Comp 5# Generador de Relaciones] float,
        [B# Competencias] float,
        [Desempeño - Resultado Cuantitativo UN] float,
        [Desempeño - Resultado Cualitativo UN] nvarchar(255),
        [TC Moneda/PEN] nvarchar(255),
        [TC PEN/USD] nvarchar(255),
        [% Incremento en Sueldo Basico] nvarchar(255),
        [% Bono Target Ajustado] nvarchar(255),
        [% Bono Anual Real] nvarchar(255)
    );
END;


 

     Crear la tabla Desempeno_Sin_Duplicados si no existe
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Desempeno_Sin_Duplicados')
BEGIN
    CREATE TABLE Desempeno_Sin_Duplicados (
        [Periodo] float,
        [Unidad de Negocio] nvarchar(255),
        [Empresa] nvarchar(255),
        [Nro de documento] nvarchar(255),
        [Apellidos_Nombres] nvarchar(255),
        [A# Scorecard - Objetivos] float,
        [Comp 1# Estratega] float,
        [Comp 2# Ejecutor] float,
        [Comp 3# Desarrollador de Personas] float,
        [Comp 4# Comunicador] float,
        [Comp 5# Generador de Relaciones] float,
        [B# Competencias] float,
        [Desempeño - Resultado Cuantitativo UN] float,
        [Desempeño - Resultado Cualitativo UN] nvarchar(255),
        [TC Moneda/PEN] nvarchar(255),
        [TC PEN/USD] nvarchar(255),
        [% Incremento en Sueldo Basico] nvarchar(255),
        [% Bono Target Ajustado] nvarchar(255),
        [% Bono Anual Real] nvarchar(255)
    );
END;

    -- Agregar la restricción única
--ALTER TABLE Desempeno_Sin_Duplicados
--ADD CONSTRAINT UCT_Desempeno_Unico
--UNIQUE (Periodo, [Unidad de Negocio], Empresa, [Nro --de documento]);






 1. Cargamos la tabla sin duplicados:

     WITH CTE AS (
    SELECT 
        Periodo, 
        [Unidad de Negocio], 
        Empresa, 
        [Nro de documento], 
        [Apellidos_Nombres], 
        [A# Scorecard - Objetivos], 
        [Comp 1# Estratega], 
        [Comp 2# Ejecutor], 
        [Comp 3# Desarrollador de Personas], 
        [Comp 4# Comunicador], 
        [Comp 5# Generador de Relaciones], 
        [B# Competencias], 
        [Desempeño - Resultado Cuantitativo UN], 
        [Desempeño - Resultado Cualitativo UN], 
        [TC Moneda/PEN], 
        [TC PEN/USD], 
        [% Incremento en Sueldo Basico], 
        [% Bono Target Ajustado], 
        [% Bono Anual Real],
        ROW_NUMBER() OVER (PARTITION BY Periodo, [Unidad de Negocio], Empresa, [Nro de documento] ORDER BY (SELECT NULL)) AS RowNum
    FROM 
        Desempeno
)
MERGE 
    Desempeno_Sin_Duplicados AS target
USING 
    (SELECT 
         Periodo, 
         [Unidad de Negocio], 
         Empresa, 
         [Nro de documento], 
         [Apellidos_Nombres], 
         [A# Scorecard - Objetivos], 
         [Comp 1# Estratega], 
         [Comp 2# Ejecutor], 
         [Comp 3# Desarrollador de Personas], 
         [Comp 4# Comunicador], 
         [Comp 5# Generador de Relaciones], 
         [B# Competencias], 
         [Desempeño - Resultado Cuantitativo UN], 
         [Desempeño - Resultado Cualitativo UN], 
         [TC Moneda/PEN], 
         [TC PEN/USD], 
         [% Incremento en Sueldo Basico], 
         [% Bono Target Ajustado], 
         [% Bono Anual Real]
     FROM 
         CTE
     WHERE 
         RowNum = 1
    ) AS source
ON 
    target.Periodo = source.Periodo
    AND target.[Unidad de Negocio] = source.[Unidad de Negocio]
    AND target.Empresa = source.Empresa
    AND target.[Nro de documento] = source.[Nro de documento]
WHEN MATCHED THEN 
    UPDATE SET 
        [Apellidos_Nombres] = source.[Apellidos_Nombres],
        [A# Scorecard - Objetivos] = source.[A# Scorecard - Objetivos],
        [Comp 1# Estratega] = source.[Comp 1# Estratega],
        [Comp 2# Ejecutor] = source.[Comp 2# Ejecutor],
        [Comp 3# Desarrollador de Personas] = source.[Comp 3# Desarrollador de Personas],
        [Comp 4# Comunicador] = source.[Comp 4# Comunicador],
        [Comp 5# Generador de Relaciones] = source.[Comp 5# Generador de Relaciones],
        [B# Competencias] = source.[B# Competencias],
        [Desempeño - Resultado Cuantitativo UN] = source.[Desempeño - Resultado Cuantitativo UN],
        [Desempeño - Resultado Cualitativo UN] = source.[Desempeño - Resultado Cualitativo UN],
        [TC Moneda/PEN] = source.[TC Moneda/PEN],
        [TC PEN/USD] = source.[TC PEN/USD],
        [% Incremento en Sueldo Basico] = source.[% Incremento en Sueldo Basico],
        [% Bono Target Ajustado] = source.[% Bono Target Ajustado],
        [% Bono Anual Real] = source.[% Bono Anual Real]
WHEN NOT MATCHED THEN 
    INSERT (
        Periodo, 
        [Unidad de Negocio], 
        Empresa, 
        [Nro de documento], 
        [Apellidos_Nombres], 
        [A# Scorecard - Objetivos], 
        [Comp 1# Estratega], 
        [Comp 2# Ejecutor], 
        [Comp 3# Desarrollador de Personas], 
        [Comp 4# Comunicador], 
        [Comp 5# Generador de Relaciones], 
        [B# Competencias], 
        [Desempeño - Resultado Cuantitativo UN], 
        [Desempeño - Resultado Cualitativo UN], 
        [TC Moneda/PEN], 
        [TC PEN/USD], 
        [% Incremento en Sueldo Basico], 
        [% Bono Target Ajustado], 
        [% Bono Anual Real]
    ) 
    VALUES (
        source.Periodo, 
        source.[Unidad de Negocio], 
        source.Empresa, 
        source.[Nro de documento], 
        source.[Apellidos_Nombres], 
        source.[A# Scorecard - Objetivos], 
        source.[Comp 1# Estratega], 
        source.[Comp 2# Ejecutor], 
        source.[Comp 3# Desarrollador de Personas], 
        source.[Comp 4# Comunicador], 
        source.[Comp 5# Generador de Relaciones], 
        source.[B# Competencias], 
        source.[Desempeño - Resultado Cuantitativo UN], 
        source.[Desempeño - Resultado Cualitativo UN], 
        source.[TC Moneda/PEN], 
        source.[TC PEN/USD], 
        source.[% Incremento en Sueldo Basico], 
        source.[% Bono Target Ajustado], 
        source.[% Bono Anual Real]
    );

 2. Con este codigo garantizamos que las nuevas inserciones de datos no nos generara problemas de duplicados:

    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Desempeno_Final')
BEGIN
    MERGE INTO Desempeno_Final AS target
    USING Desempeno_Sin_Duplicados AS source
    ON target.Periodo = source.Periodo
       AND target.[Unidad de Negocio] = source.[Unidad de Negocio]
       AND target.Empresa = source.Empresa
       AND target.[Nro de documento] = source.[Nro de documento]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            Periodo, 
            [Unidad de Negocio], 
            Empresa, 
            [Nro de documento], 
            [Apellidos_Nombres], 
            [A# Scorecard - Objetivos], 
            [Comp 1# Estratega], 
            [Comp 2# Ejecutor], 
            [Comp 3# Desarrollador de Personas], 
            [Comp 4# Comunicador], 
            [Comp 5# Generador de Relaciones], 
            [B# Competencias], 
            [Desempeño - Resultado Cuantitativo UN], 
            [Desempeño - Resultado Cualitativo UN], 
            [TC Moneda/PEN], 
            [TC PEN/USD], 
            [% Incremento en Sueldo Basico], 
            [% Bono Target Ajustado], 
            [% Bono Anual Real]
        )
        VALUES (
            source.Periodo, 
            source.[Unidad de Negocio], 
            source.Empresa, 
            source.[Nro de documento], 
            source.[Apellidos_Nombres], 
            source.[A# Scorecard - Objetivos], 
            source.[Comp 1# Estratega], 
            source.[Comp 2# Ejecutor], 
            source.[Comp 3# Desarrollador de Personas], 
            source.[Comp 4# Comunicador], 
            source.[Comp 5# Generador de Relaciones], 
            source.[B# Competencias], 
            source.[Desempeño - Resultado Cuantitativo UN], 
            source.[Desempeño - Resultado Cualitativo UN], 
            source.[TC Moneda/PEN], 
            source.[TC PEN/USD], 
            source.[% Incremento en Sueldo Basico], 
            source.[% Bono Target Ajustado], 
            source.[% Bono Anual Real]
        );
END

 3. Finlamente esta es la funcion UNPIVOT que convertira columnas en filas y las guardara en una tabla:

    SELECT 
    Periodo,
    [Unidad de Negocio],
    Empresa,
    [Nro de documento],
    [Apellidos_Nombres],
    Indicador,
    Valor
FROM 
    (SELECT 
        Periodo,
        [Unidad de Negocio],
        Empresa,
        [Nro de documento],
        [Apellidos_Nombres],
        COALESCE(CAST([A# Scorecard - Objetivos] AS VARCHAR(100)), '') AS [A# Scorecard - Objetivos],
        COALESCE(CAST([Comp 1# Estratega] AS VARCHAR(100)), '') AS [Comp 1# Estratega],
        COALESCE(CAST([Comp 2# Ejecutor] AS VARCHAR(100)), '') AS [Comp 2# Ejecutor],
        COALESCE(CAST([Comp 3# Desarrollador de Personas] AS VARCHAR(100)), '') AS [Comp 3# Desarrollador de Personas],
        COALESCE(CAST([Comp 4# Comunicador] AS VARCHAR(100)), '') AS [Comp 4# Comunicador],
        COALESCE(CAST([Comp 5# Generador de Relaciones] AS VARCHAR(100)), '') AS [Comp 5# Generador de Relaciones],
        COALESCE(CAST([B# Competencias] AS VARCHAR(100)), '') AS [B# Competencias],
        COALESCE(CAST([Desempeño - Resultado Cuantitativo UN] AS VARCHAR(100)), '') AS [Desempeño - Resultado Cuantitativo UN],
        COALESCE(CAST([Desempeño - Resultado Cualitativo UN] AS VARCHAR(100)), '') AS [Desempeño - Resultado Cualitativo UN]
    FROM 
        Desempeno_Final
    ) AS SourceTable
UNPIVOT
(
    Valor FOR Indicador IN (
        [A# Scorecard - Objetivos],
        [Comp 1# Estratega],
        [Comp 2# Ejecutor],
        [Comp 3# Desarrollador de Personas],
        [Comp 4# Comunicador],
        [Comp 5# Generador de Relaciones],
        [B# Competencias],
        [Desempeño - Resultado Cuantitativo UN],
        [Desempeño - Resultado Cualitativo UN]
    )
) AS PivotTable;


 4. Algunas consideraciones:

 

 - Hay que implementar el tema de las Key para implementear el tema de modelamiento de datos:
 - La razon poderosa por la cual es necesario manejar los duplciados es por el tema del uso de la funcion UNPIVOT ya que los duplicados generaran muchos errores al usar esta funcion:

si la tabla **Desempeno_Final** tiene **8,378 registros**, el cálculo para los registros finales después del pivoteo cambiaría. Vamos a hacer nuevamente el cálculo con base en esta cantidad de registros.

### Proceso de cálculo:

-   Estás pivotando **9 columnas**:  
    `[A# Scorecard - Objetivos]`, `[Comp 1# Estratega]`, `[Comp 2# Ejecutor]`, `[Comp 3# Desarrollador de Personas]`, `[Comp 4# Comunicador]`, `[Comp 5# Generador de Relaciones]`, `[B# Competencias]`, `[Desempeño - Resultado Cuantitativo UN]`, y `[Desempeño - Resultado Cualitativo UN]`.
    
-   Si la tabla original tiene **8,378 registros**, y después del pivoteo, se generarían 9 filas por cada uno de esos registros (asumiendo que no hay valores nulos).
- 8,378×9=75,402 registros8,378 


6. Algunos codigos que pueden servir para identificar duplicados:

    select count(*) from Desempeno; --8380
--2 duplicados
SELECT 
    Periodo,
    [Unidad de Negocio],
    Empresa,
    [Nro de documento],
    COUNT(*) AS DuplicateCount
FROM 
    Desempeno
GROUP BY 
    Periodo,
    [Unidad de Negocio],
    Empresa,
    [Nro de documento]
HAVING 
    COUNT(*) > 1;

select count(*) from Desempeno_Sin_Duplicados; --8378 se quitaron los 2 duplicados

select count(*) from Desempeno_Final; --8378 es la unica que cumple con el merge el restod e tablas crece en numero de registros
