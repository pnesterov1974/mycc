namespace Dataread
{
    public interface IReadModel
    {
        string ModelName { get; set; }
        string SourceDbName { get; set; }
        string SourceSql { get; set; }
        List<string> SourceKeyFilelds { get; set; }
        string SerializedJsonFileName { get; }
        string SerializedYamlFileName { get; }
        string SerializedXmlFileName { get; }
    }

    public class ReadModelBase : IReadModel
    {
        public virtual string ModelName { get; set; } = string.Empty;
        public virtual string SourceDbName { get; set; } = string.Empty;
        public virtual string SourceSql { get; set; } = string.Empty;
        public virtual List<string> SourceKeyFilelds { get; set; }
        public string SerializedJsonFileName
        {
            get => Path.Combine(Directory.GetCurrentDirectory(), "read_models", String.Join('.', this.ModelName, "json"));
        }
        public string SerializedYamlFileName
        {
            get => Path.Combine(Directory.GetCurrentDirectory(), "read_models", String.Join('.', this.ModelName, "yaml"));
        }
        public string SerializedXmlFileName
        {
            get => Path.Combine(Directory.GetCurrentDirectory(), "read_models", String.Join('.', this.ModelName, "xml"));
        }
    }
}
