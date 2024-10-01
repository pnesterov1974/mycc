using Newtonsoft.Json;
//using System.Xml.Serialization;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Datamech.mssql
{
    public interface IEtlModel
    {
        string ModelName { get; set; }
        string SourceName { get; set; }
        string TargetName { get; set; }
        string SourceSchema { get; set; }
        string SourceSql { get; set; }
        string TargetTableName { get; set; }
        string TargetSchemaName { get; set; }
        string SerializedJsonFileName { get; }
        string SerializedYamlFileName { get; }
        string SerializedXmlFileName { get; }
        string TargetTableFullName { get; }
    }

    public class EtlModelBase
    {
        public virtual string ModelName { get; set; } = string.Empty;
        public virtual string TargetTableName { get; set; } = string.Empty;
        public virtual string TargetSchemaName { get; set; } = string.Empty;
        public string SerializedJsonFileName
        {
            get => Path.Combine(Directory.GetCurrentDirectory(), "models", String.Join('.', this.ModelName, "json"));
        }
        public string SerializedYamlFileName
        {
            get => Path.Combine(Directory.GetCurrentDirectory(), "models", String.Join('.', this.ModelName, "yaml"));
        }
        public string SerializedXmlFileName
        {
            get => Path.Combine(Directory.GetCurrentDirectory(), "models", String.Join('.', this.ModelName, "xml"));
        }
        public string TargetTableFullName
        {
            get => string.Join('.', this.TargetSchemaName, this.TargetTableName);
        }
    }

    public class EmptyModel: EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = string.Empty;
        public string SourceName { get; set; } = string.Empty;
        public string TargetName { get; set; } = string.Empty;
        public string SourceSchema { get; set; } = string.Empty;
        public string SourceSql { get; set; } = string.Empty;
        public override string TargetTableName { get; set; } = string.Empty;
        public override string TargetSchemaName { get; set; } = string.Empty;
    }

    public class Kladr1Model: EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = "Kladr1";
        public string SourceName { get; set; } = "kladrRaw";
        public string TargetName { get; set; } = "kladrWork";
        public string SourceSchema { get; set; } = "dbo";
        public override string TargetTableName { get; set; } = "[kladr_1]";
        public override string TargetSchemaName { get; set; } = "dbo";
        public string SourceSql { get; set; } = """
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
        FROM dbo.[kladr] k
        WHERE (RIGHT(k.[code], 2) = N'00')
        """;
    }

    public class Street1Model: EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = "Street1";
        public string SourceName { get; set; } = "streetRaw";
        public string TargetName { get; set; } = "streetWork";
        public string SourceSchema { get; set; } = "dbo";
        public override string TargetTableName { get; set; } = "[street_1]";
        public override string TargetSchemaName { get; set; } = "dbo";
        public string SourceSql { get; set; } = """
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
        FROM dbo.[Street] s
        WHERE (RIGHT(s.[code], 2) = N'00')
        """;
    }

    public class Doma1Model: EtlModelBase, IEtlModel
    {
        public override string ModelName { get; set; } = "Doma1";
        public string SourceName { get; set; } = "domaRaw";
        public string TargetName { get; set; } = "domaWork";
        public string SourceSchema { get; set; } = "dbo";
        public override string TargetTableName { get; set; } = "[doma_1]";
        public override string TargetSchemaName { get; set; } = "dbo";
        public string SourceSql { get; set; } = """
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
        FROM dbo.[Doma] d
        """;
    }

    public class Serializator
    {
        public IEtlModel EtlModel;
        public Serializator(IEtlModel etlModel) => this.EtlModel = etlModel;
        public void SerializeToJson()
        {
            string serialized = JsonConvert.SerializeObject(this.EtlModel, Formatting.Indented);
            File.WriteAllText(this.EtlModel.SerializedJsonFileName, serialized);
        }
        public void SerializeToYaml()
        {
            var serializer = new SerializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();
            var yaml = serializer.Serialize(this.EtlModel);
            File.WriteAllText(this.EtlModel.SerializedYamlFileName, yaml);
        }
        public void SerializeToXml()
        {
            //XmlSerializer xmlSerializer = new XmlSerializer(typeof(this.EtlModel));
            //using (FileStream fs = new FileStream(this.EtlModel.SerializedXmlFileName, FileMode.OpenOrCreate))
            //{
            //    xmlSerializer.Serialize(fs, this.EtlModel);
            //    Console.WriteLine("Object model1 has been serialized in XML");
            //}
            ;
        }
    }
}
