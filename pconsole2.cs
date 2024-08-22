// https://spectreconsole.net/
// https://github.com/commandlineparser/commandline?tab=readme-ov-file
// dotnet add package CommandLineParser --version 2.9.1

using CommandLine;

Console.WriteLine("CLI App");

bool importKladr = false;
string targetDB = string.Empty;
List<string> objectList = new List<string>();
string[] allObjects = new string[]{ "SocrBase", "AltNames", "Kladr", "Street", "Doma", "NameMap" };
List<string> allObjects2 = new List<string>{ "SocrBase", "AltNames", "Kladr", "Street", "Doma", "NameMap" };

Parser.Default.ParseArguments<Options>(args)
    .WithParsed<Options>(opts =>
        {
            importKladr = opts.ImportKladr;

            if (opts.DestinationMsSql) targetDB = "mssql";
            else if (opts.DestinationPgSql) targetDB = "pgsql";
            else targetDB = string.Empty;
            
            objectList = opts.Objects.ToList();
        }
    );

Console.WriteLine($"importKladr = {importKladr}");
Console.WriteLine($"targetDB = {targetDB}");

if (objectList.Count > 0) foreach (string o in objectList) Console.WriteLine(o);
else Console.WriteLine("All objects");

foreach(var s in allObjects2)
{
   Console.WriteLine(s); 
}

public class Options
{
    [Option('k', "ImportKladr", Required = false, HelpText = "Do import KLADR from dbf files")]
    public bool ImportKladr { get; set; }

    [Option('m', "mssql", Required = false, HelpText = "Destination MSSQL")]
    public bool DestinationMsSql { get; set; }

    [Option('p', "pgsql", Required = false, HelpText = "Destination PgSql")]
    public bool DestinationPgSql { get; set; }

    // Список SocrBase, AltNames, Kladr, Street, Doma, NameMap
    [Option('o', "objects", HelpText = "List of objects")]
    public IEnumerable<string> Objects { get; set; } // Enum
}
