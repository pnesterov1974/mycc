namespace Datamech
{
    public interface IEtlModel
    {
        string ModelName { get; set; }
        string SourceDbName { get; set; }
        string TargetDbName { get; set; }
        string SourceSql { get; set; }
        List<string> SourceKeyFilelds { get; set; }
        string TargetTableName { get; set; }
        string TargetSchemaName { get; set; }
        string TargetTableFullName { get; }
        string SerializedJsonFileName { get; }
        string SerializedYamlFileName { get; }
        string SerializedXmlFileName { get; }
    }

    public class EtlModelBase : IEtlModel
    {
        public virtual string ModelName { get; set; } = string.Empty;
        public virtual string SourceDbName { get; set; } = string.Empty;
        public virtual string SourceSql { get; set; } = string.Empty;
        public virtual string TargetDbName { get; set; } = string.Empty;
        public virtual string TargetTableName { get; set; } = string.Empty;
        public virtual string TargetSchemaName { get; set; } = string.Empty;
        public virtual List<string> SourceKeyFilelds { get; set; }
        public virtual string TargetTableFullName
        {
            get => string.Join('.', this.TargetSchemaName, this.TargetTableName);
        }
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
    }
}
