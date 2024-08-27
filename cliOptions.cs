using CommandLine;

namespace Options;

public class CliOptions
{  
    [Option("geotype", Required = true, HelpText = "Указание типа классификатора kladr vs gar")]
    public string GeoType {get; set;} // myApp --geotype kladr/gar

    [Option("targetdb", Required = true, HelpText = "Указание целевой БД MSSQL vs PGSQL")]
    public string TargetDb {get; set;} // myApp --geotype kladr/gar

    [Option("objects", Required = false, HelpText = "Указание конкретных объектов соответствующего классификатора для обработки")]
    public IEnumerable<string> Objects { get; set; }
}
