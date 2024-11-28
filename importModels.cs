namespace import_kladr_ef;

//string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "my_dev", "files", "kladr");
//string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");                                                           

public interface IImportModel
{
    public string SourceFileName { get; set; }
    public string SourceDirPath { get; set; }
    public int BufferRecs { get; set; }
}

public class SocrBaseImportModel: IImportModel
{
    public string SourceFileName { get; set; } = "SOCRBASE.DBF";
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 10_000;
}


public class AltNamesImportModel: IImportModel
{
    public string SourceFileName { get; set; } = "ALTNAMES.DBF";
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 100_000;
}

public class KladrImportModel: IImportModel
{
    public string SourceFileName { get; set; } = "KLADR.DBF";
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 200_000;
}

public class StreetImportModel: IImportModel
{
    public string SourceFileName { get; set; } = "STREET.DBF";
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 200_000;
}

public class DomaImportModel: IImportModel
{
    public string SourceFileName { get; set; } = "DOMA.DBF";
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 1_000_000;
}
