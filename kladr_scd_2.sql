-- 1. INSERT NEW

DECLARE @MaxBusinessDT DATE = (SELECT MAX([BusinessDT]) AS [MaxBusinessDT] FROM stg.[KLADR]);

;WITH [selectOds] AS (
    SELECT 
        [Code], [Name], [Socr], [Index], [Gninmb], [Uno], [Ocatd], [Status],
        [dttmFrom], [dttmTo], [IsActive], [IsDeleted]
    FROM ods.[Kladr]
    WHERE [IsActive] = 1
),
[selectStg] AS (
    SELECT [CODE], [NAME], [SOCR], [INDEX], [GNINMB], [UNO], [OCATD], [STATUS], [BusinessDT]
    FROM stg.[KLADR]
),
[forInsert] AS (
    SELECT stg.[CODE], stg.[NAME], stg.[SOCR], stg.[INDEX], stg.[GNINMB], stg.[UNO], stg.[OCATD], stg.[STATUS],
        [dttmFROM] = CAST(stg.[BusinessDT] AS DATETIME),
        [dttmTO] = CAST('2999-01-01' AS DATETIME),
        [IsActive] = 1,
        [IsDeleted] = 0       
    FROM [selectStg] stg
    LEFT JOIN [selectOds] ods ON ods.[Code] = stg.[CODE]
    WHERE ods.[Code] IS NULL
)
INSERT INTO ods.[Kladr] (
    [Code], [Name], [Socr], [Index], [Gninmb], [Uno], [Ocatd], [Status],
    [dttmFrom], [dttmTo], [IsActive], [IsDeleted]
)
SELECT [CODE], [NAME], [SOCR], [INDEX], [GNINMB], [UNO], [OCATD], [STATUS],
       [dttmFrom], [dttmTo], [IsActive], [IsDeleted]
FROM [forInsert];

-- 2. DELETE
;WITH [selectOds] AS (
    SELECT 
        [Code], [Name], [Socr], [Index], [Gninmb], [Uno], [Ocatd], [Status],
        [dttmFrom], [dttmTo], [IsActive], [IsDeleted]
    FROM ods.[Kladr]
    WHERE [IsActive] = 1
),
[selectStg] AS (
    SELECT [CODE], [NAME], [SOCR], [INDEX], [GNINMB], [UNO], [OCATD], [STATUS], [BusinessDT]
    FROM stg.[KLADR]
),
[forDelete] AS (
    SELECT ods.[Code], MAX(stg.[BusinessDT]) OVER() AS [LastDTTM]
    FROM [selectStg] stg
    RIGHT JOIN [selectOds] ods ON ods.[Code] = stg.[CODE]
    WHERE stg.[CODE] IS NULL
)
UPDATE ods.[Kladr]
SET [IsDeleted] = 1,
    [IsActive] = 0,
    [dttmTO] = @MaxBusinessDT
WHERE ods.[Kladr].[Code] IN ( -- EXISTS
    SELECT DISTINCT [Code] FROM [forDelete]
);

-- 3. UPDATE -1 фаза
;WITH [selectOds] AS (
    SELECT 
        [Code], [Name], [Socr], [Index], [Gninmb], [Uno], [Ocatd], [Status],
        [dttmFrom], [dttmTo], [IsActive], [IsDeleted]
    FROM ods.[Kladr]
    WHERE [IsActive] = 1
),
[selectStg] AS (
    SELECT [CODE], [NAME], [SOCR], [INDEX], [GNINMB], [UNO], [OCATD], [STATUS], [BusinessDT]
    FROM stg.[KLADR]
),
[forUpdateBase] AS (
    SELECT stg.[CODE] AS [stg_CODE], ods.[Code] AS [ods_Code],
           stg.[NAME] AS [stg_NAME], ods.[Name] AS [ods_Name],
           stg.[SOCR] AS [stg_SOCR], ods.[Socr] AS [ods_Socr],
           stg.[INDEX] AS [stg_INDEX], ods.[Index] AS [ods_Index],
           stg.[GNINMB] AS [stg_GNINMB], ods.[Gninmb] AS [ods_Gninmb],
           stg.[UNO] AS [stg_UNO], ods.[Uno] AS [ods_Uno],
           stg.[OCATD] AS [stg_OCATD], ods.[Ocatd] AS [ods_Ocatd],
           stg.[STATUS] AS [stg_STATUS], ods.[Status] AS [ods_Status],
           stg.[BusinessDT] AS [BusinessDT] 
        FROM [selectStg] stg
        INNER JOIN [selectOds] ods ON ods.[CODE] = stg.[CODE]
),
[needToSkip] AS ( -- do not touch
    SELECT [ods_CODE], [BusinessDT]
    FROM [forUpdateBase]
    WHERE (stg_CODE = ods_Code)
          AND
          (stg_NAME = ods_Name)
          AND
          (stg_SOCR = ods_Socr)
          AND
          (stg_INDEX = ods_Index)
          AND
          (stg_GNINMB = ods_Gninmb)
          AND
          (stg_UNO = ods_Uno)
          AND
          (stg_OCATD = ods_Ocatd)
          AND
          (stg_STATUS = ods_Status)
),
[needUpdate] AS (
    SELECT [CODE], [BusinessDT]
    FROM stg.[KLADR]
    WHERE [CODE] NOT IN (SELECT ods_CODE FROM [needToSkip])
)
UPDATE trg
SET [IsActive] = 0,
    [IsDeleted] = 0,
    --[dttmTO] = @MaxSourceDT
    [dttmTO] = [needUpdate].[BusinessDT]
FROM ods.[Kladr] trg
INNER JOIN [needUpdate] ON [needUpdate].[CODE] = trg.[Code]
WHERE trg.[Code] IN (SELECT [CODE] FROM [needUpDate]);

-- 3. UPDATE -2 фаза
;WITH [selectOds] AS (
    SELECT 
        [Code], [Name], [Socr], [Index], [Gninmb], [Uno], [Ocatd], [Status],
        [dttmFrom], [dttmTo], [IsActive], [IsDeleted]
    FROM ods.[Kladr]
    WHERE [IsActive] = 1
),
[selectStg] AS (
    SELECT [CODE], [NAME], [SOCR], [INDEX], [GNINMB], [UNO], [OCATD], [STATUS], [BusinessDT]
    FROM stg.[KLADR]
),
[forUpdateBase] AS (
    SELECT stg.[CODE] AS [stg_CODE], ods.[Code] AS [ods_Code],
           stg.[NAME] AS [stg_NAME], ods.[Name] AS [ods_Name],
           stg.[SOCR] AS [stg_SOCR], ods.[Socr] AS [ods_Socr],
           stg.[INDEX] AS [stg_INDEX], ods.[Index] AS [ods_Index],
           stg.[GNINMB] AS [stg_GNINMB], ods.[Gninmb] AS [ods_Gninmb],
           stg.[UNO] AS [stg_UNO], ods.[Uno] AS [ods_Uno],
           stg.[OCATD] AS [stg_OCATD], ods.[Ocatd] AS [ods_Ocatd],
           stg.[STATUS] AS [stg_STATUS], ods.[Status] AS [ods_Status],
           stg.[BusinessDT] AS [BusinessDT] 
        FROM [selectStg] stg
        INNER JOIN [selectOds] ods ON ods.[CODE] = stg.[CODE]
),
[needToSkip] AS ( -- do not touch
    SELECT ods_CODE
    FROM [forUpdateBase]
    WHERE (stg_CODE = ods_Code)
          AND
          (stg_NAME = ods_Name)
          AND
          (stg_SOCR = ods_Socr)
          AND
          (stg_INDEX = ods_Index)
          AND
          (stg_GNINMB = ods_Gninmb)
          AND
          (stg_UNO = ods_Uno)
          AND
          (stg_OCATD = ods_Ocatd)
          AND
          (stg_STATUS = ods_Status)
),
[needUpdate] AS (
    SELECT [CODE], [BusinessDT]
    FROM stg.[KLADR]
    WHERE 
        [CODE] NOT IN (SELECT ods_CODE FROM [needToSkip])
)
INSERT INTO ods.[Kladr] (
    [Code], [Name], [Socr], [Index], [Gninmb], [Uno], [Ocatd], [Status],
    [dttmFrom], [dttmTo], [IsActive], [IsDeleted]
)
SELECT [CODE], [NAME], [SOCR], [INDEX], [GNINMB], [UNO], [OCATD], [STATUS],
       [BusinessDT], '2999-01-01', 1, 0
FROM stg.[Kladr]
WHERE stg.[Kladr].[Code] IN (SELECT [CODE] FROM [needUpDate]);
