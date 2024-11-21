namespace Datamech.mssql.etlmodels
{
    public class NewModel : EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = "Kladr1";
        public override string SourceDbName { get; set; } = "kladrStage";
        public override string TargetDbName { get; set; } = "kladrStage";
        public override string TargetTableName { get; set; } = "[kladr_1]";
        public override string TargetSchemaName { get; set; } = "dbo";
        //-----------------------------------------------------------
        public string MaterializeType { get; set; } = "table"; // vs "view"
        public override List<string> SourceKeyFilelds { get; set; } = new List<string>() { "[KladrCode]" };
        public List<string> ModelOptions { get; set; } = new List<string>
        {
            "UseSourceKeyFieldsWhenValidate",
            "CreateTargetPK",
            "DropTableIfExists",
            "UsePreSql",
            "UsePostSql",
            "UsePullMotivation",   // Модель сканирует каждые 5 мин и "вытягивает" триггер условия на запуск
            "UsePushMotivation"    // Модель после отработки агркссивно "триггерит", пушит на обработку других моделей
        };
        public string preSql { get; set; } = """
            SELECT 1;
        """;
        public string postSql { get; set; } = """
            SELECT 2;
        """;
        public List<string> PullMotivationList = new List<string>
        {
            "modelName1",
            "modelName2"
        };
        public List<string> PushMotivationList = new List<string>
        {
            "modelName11",
            "modelName21"
        };

        //---------------------------------------------------------
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
              AND (k.[status] = @statusFilter)
              AND (k.[code] = @kladrCodeFilter)
        """;

        public List<string> GetParameterNames()
        {
            List<string> s = new List<string>();
    
            //int initialStringIndex = 0;
            int startStringIndex = 0;
            //int endStringIndex = 0;
            char[] symbols = { ' ', ')', '(', '/', '=', '*', '-', '+', '<', '>' };
            do
            {
                startStringIndex = this.SourceSql.IndexOf('@', startStringIndex);
                if (startStringIndex > -1)
                {
                    int endStringIndex = this.SourceSql.IndexOfAny(symbols, startStringIndex + 1);
                    string paramName = string.Empty;
                    if (endStringIndex > -1)
                    {
                        paramName = this.SourceSql.Substring(startStringIndex, endStringIndex - startStringIndex);
                    }
                    else
                    {
                        paramName = this.SourceSql.Substring(startStringIndex, this.SourceSql.Length - startStringIndex);
                    }
                    if (paramName.Length > 0)
                    {
                        s.Add(paramName);
                        Console.WriteLine(paramName);
                    }
                    startStringIndex += 1;
                }
            }
            while (startStringIndex > -1);
            return s;
        }
    }
}

// Вытащить названия параметров
// Для всех вхождений @ вытащить строку составом после @ до символов " " / = * - + )

