namespace Shared.Transformations.pqsql;

public static class SocrBasePgSql
{
    public static string ModelName = "SocrBase";
    public static string ConnectionName = "KladrPgConnection";
    public static string TargetTableName = "socrbase_t";
    public static string TargetSchemaName = "public";
    public static string SQL = """
            SELECT level, scname, socrname, kod_t_st
            FROM public.socrbase
        """;
}

public static class AltNamesPgSql
{
    public static string ModelName = "AltNames";
    public static string ConnectionName = "KladrPgConnection";
    public static string TargetTableName = "altnames_t";
    public static string TargetSchemaName = "public";
    public static string SQL = """
            SELECT oldcode, newcode, level,
            FROM public.altnames
        """;
}

public static class KladrPgSql
{
    public static string ModelName = "Kladr";
    public static string ConnectionName = "KladrPgConnection";
    public static string TargetTableName = "kladr_t";
    public static string TargetSchemaName = "public";
    public static string SQL = """
            SELECT code,
                   name,
                   socr,
                   index,
                   ginmb,
                   uno,
                   ocatd,
                   status
            FROM public.kladr
        """;
}

// public class StreetPgSql
// {
//     public string ModelName = "Street";
//     public string ConnectionName = "KladrConnection";
//     public string TargetTableName = "[street_t]";
//     public string TargetSchemaName = "dbo";
//     public string SQL = """
//             SELECT [CODE],
//                    [NAME],
//                    [SOCR],
//                    [INDEX],
//                    [GNINMB],
//                    [UNO],
//                    [OCATD]
//             FROM dbo.[STREET]
//         """;
// }

// public class DomaPgSql
// {
//     public string ModelName = "Doma";
//     public string ConnectionName = "KladrConnection";
//     public string TargetTableName = "[doma_t]";
//     public string TargetSchemaName = "dbo";
//     public string SQL = """
//             SELECT [CODE],
//                    [NAME],
//                    [KORP],
//                    [SOCR],
//                    [INDEX],
//                    [GNINMB],
//                    [UNO],
//                    [OCATD]
//             FROM dbo.[DOMA]
//         """;
// }

// public class KladrOdsPgSql
// {
//     public string ModelName = "KladrOds";
//     public string ConnectionName = "KladrConnection";
//     public string TargetTableName = "[kladr_ods]";
//     public string TargetSchemaName = "dbo";
//     public string SQL = """
//             SELECT k.[code] AS [KladrCode],
//                    LEFT(k.[code], 11) AS [KladrSubCode],
//                    LEFT(k.[Code], 2) AS [AreaCode],
//                    SUBSTRING(k.[code], 3, 3) AS [DistrictCode],
//                    SUBSTRING(k.[code], 6, 3) AS [CityCode],
//                    SUBSTRING(k.[code], 9, 3) AS [TownCode],
//                    IIF(SUBSTRING(k.[code], 9, 3) <> N'000', 4,
//                        (IIF(SUBSTRING(k.[code], 6, 3) <> N'000', 3,
//                            (IIF(SUBSTRING(k.[code], 3, 3) <> N'000', 2, 1))
//                         )
//                       )
//                     ) AS [KladrLevel],
//                     RIGHT(k.[code], 2) AS [ActualityStatus],
//                     k.[name] AS [KladrName],
//                     k.[socr] AS [KladrSocr],
//                     k.[index] AS [KladrIndex],
//                     k.[gninmb] AS [KladrGninmb],
//                     k.[uno] AS [KladrUno],
//                     k.[ocatd] AS [KladrOcatd],
//                     k.[status] AS [KladrStatus]
//         FROM dbo.[kladr] k
//         WHERE (RIGHT(k.[code], 2) = N'00')
//         """;
// }

// public class StreetOdsPgSql
// {
//     public string ModelName = "StreetOds";
//     public string ConnectionName = "KladrConnection";
//     public string TargetTableName = "[street_ods]";
//     public string TargetSchemaName = "dbo";
//     public string SQL = """ 
//         SELECT s.[code] AS [KladrCode],
//                LEFT(s.[code], 11) AS [KladrSubCode],
//                LEFT(s.[code], 2) AS [AreaCode],
//                SUBSTRING(s.[code], 3, 3) AS [DistrictCode],
//                SUBSTRING(s.[code], 6, 3) AS [CityCode],
//                SUBSTRING(s.[code], 9, 3) AS [TownCode],
//                SUBSTRING(s.[code], 12, 4) AS [StreetCode],
//                RIGHT(s.[code], 2) AS [ActualityStatus],
//                5 AS [KladrLevel],
//                s.[name] AS [KladrName],
//                s.[socr] AS [KladrSocr],
//                s.[index] AS [KladrIndex],
//                s.[gninmb] AS [KladrGninmd],
//                s.[uno] AS [KladrUno],
//                s.[ocatd] AS [KLadrOkatd]
//         FROM dbo.[Street] s
//         WHERE (RIGHT(s.[code], 2) = N'00')
//         """;
// }

// public class DomaOdsPgSql
// {
//     public string ModelName = "DomaOds";
//     public string ConnectionName = "KladrConnection";
//     public string TargetTableName = "[doma_ods]";
//     public string TargetSchemaName = "dbo";
//     public string SQL =  """
//         SELECT d.[code] AS [KladrCode],
//                LEFT(d.[code], 11) AS [KladrSubCode],
//                LEFT(d.[code], 2) AS [AreaCodee],
//                SUBSTRING(d.[code],  3, 3) AS [DistrictCode],
//                SUBSTRING(d.[code],  6, 3) AS [CitryCode],
//                SUBSTRING(d.[code],  9, 3) AS [TownCode],
//                SUBSTRING(d.[code], 12, 4) AS [StreetCode],
//                SUBSTRING(d.[code], 16, 4) AS [BldCode],
//                6 AS [KladrLevel], 
//                d.[name] AS [KladrName],
//                d.[socr] AS [KladrSocr],
//                d.[index] AS [KladrIndex],
//                d.[gninmb] AS [KladrGninmb],
//                d.[uno] AS [KladrUno],
//                d.[ocatd] AS [KladrOcatd]
//         FROM dbo.[Doma] d
//         """;
// }
