namespace import_kladr_efc.read_dbf;

//string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "my_dev", "files", "kladr");
//string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");                                                           

public class ImportObjectInfo : IObjectInfo
{
    public string SourceFileName { get; set; }
    public string SourceDirPath { get; set; }
    public int BufferRecs { get; set; }
}

public class KladrImportObjectInfo: ImportObjectInfo
{
    public string SourceFileName { get; set; } = 'KLADR.DBF';
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 100000;
}

public class StreetImportObjectInfo: ImportObjectInfo
{
    public string SourceFileName { get; set; } = 'STREET.DBF';
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 100000;
}

public class KladrImportObjectInfo: ImportObjectInfo
{
    public string SourceFileName { get; set; } = 'DOMA.DBF';
    public string SourceDirPath { get; set; } = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
    public int BufferRecs { get; set; } = 100000;
}
