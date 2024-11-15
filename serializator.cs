using Datamech.mssql.etlmodels;
using Newtonsoft.Json;
using Serilog;
using System.Xml.Serialization;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Datamech
{
    public class Serializator
    {
        public IEtlModel EtlModel;
        public Serializator(IEtlModel etlModel) => this.EtlModel = etlModel;
        public void SerializeToJson()
        {
            string serialized = JsonConvert.SerializeObject(this.EtlModel, Formatting.Indented);
            File.WriteAllText(this.EtlModel.SerializedJsonFileName, serialized);
        }
        public IEtlModel DeSerializeFromJson(string jsonStr)
        {
            IEtlModel? m = JsonConvert.DeserializeObject<IEtlModel>(jsonStr);
            if (m != null)
            {
                return m;
            }
            else
            {
                return null;
            }
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
            //var concreteMessageInstance = message as NetworkDataMessage;
            //if(concreteMessageInstance != null) { /* ... */ }
            Kladr1Model etlModel = this.EtlModel as Kladr1Model;
            if (etlModel != null)
            {
                Log.Information("Зашли...");
                XmlSerializer xmlSerializer = new XmlSerializer(typeof(Kladr1Model));
                using (FileStream fs = new FileStream(this.EtlModel.SerializedXmlFileName, FileMode.OpenOrCreate))
                {
                    xmlSerializer.Serialize(fs, etlModel);
                    Console.WriteLine("Object model1 has been serialized in XML");
                }
                //}
            }
        }
    }
}
