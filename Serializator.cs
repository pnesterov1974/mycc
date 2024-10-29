using Newtonsoft.Json;
//using System.Xml.Serialization;
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
