namespace Datamech.mssql
{
    public class Kladr1Model : EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = "Kladr1";
        public override string SourceDbName { get; set; } = "kladrRaw";
        public override string TargetDbName { get; set; } = "kladrWork";
        public override string TargetTableName { get; set; } = "[kladr_1]";
        public override string TargetSchemaName { get; set; } = "dbo";
        public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT k.[code] AS [KladrCode],
                   LEFT(k.[code], 11) AS [KladrSubCode],
                   LEFT(k.[Code], 2) AS [AreaCode],
                   SUBSTRING(k.[code], 3, 3) AS [DistrictCode],
                   SUBSTRING(k.[code], 6, 3) AS [CityCode],
                   SUBSTRING(k.[code], 9, 3) AS [TownCode],
                   IIF(SUBSTRING(k.[code], 9, 3) <> N'000', 4,
                       (IIF(SUBSTRING(k.[code], 6, 3) <> N'000', 3,
                           (IIF(SUBSTRING(k.[code], 3, 3) <> N'000', 2, 1))
                        )
                      )
                    ) AS [KladrLevel],
                    RIGHT(k.[code], 2) AS [ActualityStatus],
                    k.[name] AS [KladrName],
                    k.[socr] AS [KladrSocr],
                    k.[index] AS [KladrIndex],
                    k.[gninmb] AS [KladrGninmb],
                    k.[uno] AS [KladrUno],
                    k.[ocatd] AS [KladrOcatd],
                    k.[status] AS [KladrStatus]
        FROM kladr.[kladr] k
        WHERE (RIGHT(k.[code], 2) = N'00')
        """;
    }

    public class Street1Model : EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = "Street1";
        public override string SourceDbName { get; set; } = "kladrRaw";
        public override string TargetDbName { get; set; } = "kladrWork";
        public override string TargetTableName { get; set; } = "[street_1]";
        public override string TargetSchemaName { get; set; } = "dbo";
        public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT s.[code] AS [KladrCode],
               LEFT(s.[code], 11) AS [KladrSubCode],
               LEFT(s.[code], 2) AS [AreaCode],
               SUBSTRING(s.[code], 3, 3) AS [DistrictCode],
               SUBSTRING(s.[code], 6, 3) AS [CityCode],
               SUBSTRING(s.[code], 9, 3) AS [TownCode],
               SUBSTRING(s.[code], 12, 4) AS [StreetCode],
               RIGHT(s.[code], 2) AS [ActualityStatus],
               5 AS [KladrLevel],
               s.[name] AS [KladrName],
               s.[socr] AS [KladrSocr],
               s.[index] AS [KladrIndex],
               s.[gninmb] AS [KladrGninmd],
               s.[uno] AS [KladrUno],
               s.[ocatd] AS [KLadrOkatd]
        FROM kladr.[Street] s
        WHERE (RIGHT(s.[code], 2) = N'00')
        """;
    }

    public class Doma1Model : EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = "Doma1";
        public override string SourceDbName { get; set; } = "kladrRaw";
        public override string TargetDbName { get; set; } = "kladrWork";
        public override string TargetTableName { get; set; } = "[doma_1]";
        public override string TargetSchemaName { get; set; } = "dbo";
        public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT d.[code] AS [KladrCode],
               LEFT(d.[code], 11) AS [KladrSubCode],
               LEFT(d.[code], 2) AS [AreaCodee],
               SUBSTRING(d.[code],  3, 3) AS [DistrictCode],
               SUBSTRING(d.[code],  6, 3) AS [CitryCode],
               SUBSTRING(d.[code],  9, 3) AS [TownCode],
               SUBSTRING(d.[code], 12, 4) AS [StreetCode],
               SUBSTRING(d.[code], 16, 4) AS [BldCode],
               6 AS [KladrLevel], 
               d.[name] AS [KladrName],
               d.[socr] AS [KladrSocr],
               d.[index] AS [KladrIndex],
               d.[gninmb] AS [KladrGninmb],
               d.[uno] AS [KladrUno],
               d.[ocatd] AS [KladrOcatd]
        FROM kladr.[Doma] d
        """;
    }

    public class ssisModel : EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = "ssisModel";
        public override string SourceDbName { get; set; } = "dwh";
        public override string TargetDbName { get; set; } = "etlSsisP02";
        public override string TargetTableName { get; set; } = "[dwh_event_log]";
        public override string TargetSchemaName { get; set; } = "dbo";
        public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[execution_id]"};
        public override string SourceSql { get; set; } = """
            SELECT gd.[msg],
                gd.[event_category_code],
                gd.[event_dttm],
                gd.[log_table],
                gd.[log_pk],
                gd.[log_solved_flag],
                gd.[suser],
                gd.[date_change],
                el.[control_process_id],
                cpi.[execution_id]
            FROM [DWH].[monitoring].[grafana_dashboard] gd
            LEFT JOIN meta.[event_log] el ON 
                el.[event_log_id] = gd.[log_pk]
            LEFT JOIN meta.[control_process_instance] cpi ON
                cpi.[control_process_id] = el.[control_process_id]
            WHERE CAST(gd.[event_dttm] AS DATE) = CAST(GETDATE() AS DATE)
                  AND (gd.[log_table] = 'event_log')
        """;
    }
}
