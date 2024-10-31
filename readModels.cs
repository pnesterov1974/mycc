namespace Dataread.mssql
{
    public class SocrBaseJustImportedModel : ReadModelBase, IReadModel
    {
        public override string ModelName { get; set; } = "SocrBase_justImported";
        public override string SourceDbName { get; set; } = "kladrRaw";
        //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT TOP (1000) 
                   [LEVEL],
                   [SCNAME],
                   [SOCRNAME],
                   [KOD_T_ST]
            FROM kladr.[SOCRBASE]
            """;
    }

    public class AltNamesJustImportedModel : ReadModelBase, IReadModel
    {
        public override string ModelName { get; set; } = "AltNames_justImported";
        public override string SourceDbName { get; set; } = "kladrRaw";
        //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT TOP (1000) 
                   [OLDCODE]
                   [NEWCODE]
                   [LEVEL]
            FROM kladr.[ALTNAMES]
            """;
    }

    public class KladrJustImportedModel : ReadModelBase, IReadModel
    {
        public override string ModelName { get; set; } = "Kladr_justImported";
        public override string SourceDbName { get; set; } = "kladrRaw";
        //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT TOP (1000) [NAME]
                    [SOCR]
                    [CODE]
                    [INDEX]
                    [GNINMB]
                    [UNO]
                    [OCATD]
                    [STATUS]
            FROM kladr.[KLADR];
            """;
    }

    public class StreetJustImportedModel : ReadModelBase, IReadModel
    {
        public override string ModelName { get; set; } = "Street_justImported";
        public override string SourceDbName { get; set; } = "kladrRaw";
        //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT TOP (1000) [NAME],
                        [SOCR],
                        [CODE],
                        [INDEX],
                        [GNINMB],
                        [UNO],
                        [OCATD]
            FROM kladr.[STREET];
            """;
    }

    public class DomaJustImportedModel : ReadModelBase, IReadModel
    {
        public override string ModelName { get; set; } = "Doma_justImported";
        public override string SourceDbName { get; set; } = "kladrRaw";
        //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT TOP (1000) [NAME],
                    [KORP],
                    [SOCR],
                    [CODE],
                    [INDEX],
                    [GNINMB],
                    [UNO],
                    [OCATD]
            FROM kladr.[DOMA]
            """;
    }
}
