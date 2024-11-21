namespace import_kladr_efcms;

//string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "my_dev", "files", "kladr");
//string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");                                                           

public interface IImportObjectInfo
{
    public string SourceFileName { get; set; }
    public string SourceDirPath { get; set; }
    public int BufferRecs { get; set; }
}

public class KladrImportObjectInfo: IImportObjectInfo
{
    public string SourceFileName { get; set; } = "KLADR.DBF";
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 100000;
}

public class StreetImportObjectInfo: IImportObjectInfo
{
    public string SourceFileName { get; set; } = "STREET.DBF";
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 100000;
}

public class DomaImportObjectInfo: IImportObjectInfo
{
    public string SourceFileName { get; set; } = "DOMA.DBF";
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 100000;
}
