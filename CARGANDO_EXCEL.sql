use ACCENTURE;

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


SELECT 
    COUNT(*) AS TotalRegistros,
    SUM(CASE WHEN [A# Scorecard - Objetivos] IS NULL THEN 1 ELSE 0 END) AS Nulos_Scorecard_Objetivos,
    SUM(CASE WHEN [Comp 1# Estratega] IS NULL THEN 1 ELSE 0 END) AS Nulos_Comp_Estratega,
    SUM(CASE WHEN [Comp 2# Ejecutor] IS NULL THEN 1 ELSE 0 END) AS Nulos_Comp_Ejecutor,
    SUM(CASE WHEN [Comp 3# Desarrollador de Personas] IS NULL THEN 1 ELSE 0 END) AS Nulos_Comp_Desarrollador,
    SUM(CASE WHEN [Comp 4# Comunicador] IS NULL THEN 1 ELSE 0 END) AS Nulos_Comp_Comunicador,
    SUM(CASE WHEN [Comp 5# Generador de Relaciones] IS NULL THEN 1 ELSE 0 END) AS Nulos_Comp_Generador,
    SUM(CASE WHEN [B# Competencias] IS NULL THEN 1 ELSE 0 END) AS Nulos_Competencias,
    SUM(CASE WHEN [Desempeño - Resultado Cuantitativo UN] IS NULL THEN 1 ELSE 0 END) AS Nulos_Resultado_Cuantitativo,
    SUM(CASE WHEN [Desempeño - Resultado Cualitativo UN] IS NULL THEN 1 ELSE 0 END) AS Nulos_Resultado_Cualitativo
FROM 
    Desempeno_Final;


SELECT 
    Periodo,
    [Unidad de Negocio],
    Empresa,
    [Nro de documento],
    COUNT(*) AS DuplicateCount
FROM 
    Desempeno_Final
GROUP BY 
    Periodo,
    [Unidad de Negocio],
    Empresa,
    [Nro de documento]
HAVING 
    COUNT(*) > 1;


use Northwind;

--select COUNT(*) from Desempeno_Final_2021; --177093

select COUNT(*) from Desempeno_Final_2021;  --59031 --75402 este cumple con lo estimado

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Desempeno_Final'

EXCEPT

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Desempeno_Sin_Duplicados';



SELECT 
    Periodo,
    [Unidad de Negocio],
    Empresa,
    [Nro de documento],
    COUNT(*) AS DuplicateCount
FROM 
    Desempeno_Sin_Duplicados
GROUP BY 
    Periodo,
    [Unidad de Negocio],
    Empresa,
    [Nro de documento]
HAVING 
    COUNT(*) > 1;

truncate table Desempeno_Final;


WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Periodo, [Unidad de Negocio], Empresa, [Nro de documento] ORDER BY (SELECT NULL)) AS RowNum
    FROM Desempeno
)
SELECT * 
INTO Desempeno_Sin_Duplicados2
FROM CTE
WHERE RowNum = 1;



-- Crea la CTE para seleccionar los registros únicos
WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Periodo, [Unidad de Negocio], Empresa, [Nro de documento] ORDER BY (SELECT NULL)) AS RowNum
    FROM Desempeño
)

-- Inserta en la nueva tabla especificando las columnas
INSERT INTO Desempeño_Sin_Duplicados (Periodo, [Unidad de Negocio], Empresa, [Nro de documento], [Apellidos_Nombres], [A# Scorecard - Objetivos], [Comp 1# Estratega], [Comp 2# Ejecutor], [Comp 3# Desarrollador de Personas], [Comp 4# Comunicador], [Comp 5# Generador de Relaciones], [B# Competencias], [Desempeño - Resultado Cuantitativo UN], [Desempeño - Resultado Cualitativo UN], [TC Moneda/PEN], [TC PEN/USD], [% Incremento en Sueldo Basico], [% Bono Target Ajustado], [% Bono Anual Real])
SELECT Periodo, [Unidad de Negocio], Empresa, [Nro de documento], [Apellidos_Nombres], [A# Scorecard - Objetivos], [Comp 1# Estratega], [Comp 2# Ejecutor], [Comp 3# Desarrollador de Personas], [Comp 4# Comunicador], [Comp 5# Generador de Relaciones], [B# Competencias], [Desempeño - Resultado Cuantitativo UN], [Desempeño - Resultado Cualitativo UN], [TC Moneda/PEN], [TC PEN/USD], [% Incremento en Sueldo Basico], [% Bono Target Ajustado], [% Bono Anual Real]
FROM CTE
WHERE RowNum = 1;



SELECT COUNT(*) FROM DATA_SET_DESEMPEÑO_2021;

SELECT * FROM DATA_SET_DESEMPEÑO_2021;


SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'DATA_SET_DESEMPEÑO_2021';



SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'DATA_SET_DESEMPEÑO_2021';




SELECT
    Apellidos_Nombres,
    Valor,
    Competencia
FROM DATA_SET_DESEMPEÑO_2021
UNPIVOT (
    Valor FOR Competencia IN (
        "A#_Scorecard_-_Objetivos" AS Scorecard,
        COMP1_ESTRATEGA AS Estratega,
        COMP2_EJECUTOR AS Ejecutor,
        COMP3_DESARROLLADOR_DE_PERSONAS AS Desarrollador,
        COMP4_COMUNICADOR AS Comunicador,
        COMP5_GENERADOR_DE_RELACIONES AS Relaciones,
        B_COMPETENCIAS AS CompetenciasGenerales,
        DESEMPENO_RESULTADO_CUANTITATIVO_UN AS DesempeñoCuantitativo,
        DESEMPENO_RESULTADO_CUALITATIVO_UN AS DesempeñoCualitativo
    )
) AS UnpivotTable;



SELECT 
    Periodo,
    [Unidad de Negocio],
    Empresa,
    [Nro de documento],
    [Apellidos_Nombres],
    Indicador,
    Valor
FROM 
    DATA_SET_DESEMPEÑO_2021
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
) AS Unpvt;



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
        CAST([A# Scorecard - Objetivos] AS VARCHAR(100)) AS [A# Scorecard - Objetivos],
        CAST([Comp 1# Estratega] AS VARCHAR(100)) AS [Comp 1# Estratega],
        CAST([Comp 2# Ejecutor] AS VARCHAR(100)) AS [Comp 2# Ejecutor],
        CAST([Comp 3# Desarrollador de Personas] AS VARCHAR(100)) AS [Comp 3# Desarrollador de Personas],
        CAST([Comp 4# Comunicador] AS VARCHAR(100)) AS [Comp 4# Comunicador],
        CAST([Comp 5# Generador de Relaciones] AS VARCHAR(100)) AS [Comp 5# Generador de Relaciones],
        CAST([B# Competencias] AS VARCHAR(100)) AS [B# Competencias],
        CAST([Desempeño - Resultado Cuantitativo UN] AS VARCHAR(100)) AS [Desempeño - Resultado Cuantitativo UN],
        CAST([Desempeño - Resultado Cualitativo UN] AS VARCHAR(100)) AS [Desempeño - Resultado Cualitativo UN]
    FROM 
        DATA_SET_DESEMPEÑO_2021
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
) AS Unpvt;
