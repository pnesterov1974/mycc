using Shared;
using Data.Import.kladr.mssql;

namespace Import.kladr.mssql;

public class MsSocrBaseImport : IKladrImport
{
    public ImportObjectInfo Ioi { get; set; } = new ImportObjectInfo
    {
        TargetTableName = "[SOCRBASE]",
        TargetSchemaName = "kladr",
        SourceFileName = "SOCRBASE.DBF",
        SourceDirPath = string.Empty,
        ConnectionString = string.Empty
    };

    public MsSocrBaseImport(string sourceDirPath, string connectionString)
    {
        this.Ioi.SourceDirPath = sourceDirPath;
        this.Ioi.ConnectionString = connectionString;
    }

    public void DoImport()
    {
        MsSqlImport sb = new MsSqlImport(this.Ioi);
        sb.ReadDbfInfo(this.Ioi.SourceFilePath);
        sb.BulkImport();
    }
}

public class MsAltNamesImport : IKladrImport
{
    public ImportObjectInfo Ioi { get; set; } = new ImportObjectInfo
    {
        TargetTableName = "[ALTNAMES]",
        TargetSchemaName = "kladr",
        SourceFileName = "ALTNAMES.DBF",
        SourceDirPath = string.Empty,
        ConnectionString = string.Empty
    };

    public MsAltNamesImport(string sourceDirPath, string connectionString)
    {
        this.Ioi.SourceDirPath = sourceDirPath;
        this.Ioi.ConnectionString = connectionString;
    }

    public void DoImport()
    {
        MsSqlImport an = new MsSqlImport(this.Ioi);
        an.ReadDbfInfo(this.Ioi.SourceFilePath);
        an.BulkImport();
    }
}

public class MsKladrImport : IKladrImport
{
    public ImportObjectInfo Ioi { get; set; } = new ImportObjectInfo
    {
        TargetTableName = "[KLADR]",
        TargetSchemaName = "kladr",
        SourceFileName = "KLADR.DBF",
        SourceDirPath = string.Empty,
        ConnectionString = string.Empty
    };

    public MsKladrImport(string sourceDirPath, string connectionString)
    {
        this.Ioi.SourceDirPath = sourceDirPath;
        this.Ioi.ConnectionString = connectionString;
    }

    public void DoImport()
    {
        MsSqlImport kl = new MsSqlImport(this.Ioi);
        kl.ReadDbfInfo(this.Ioi.SourceFilePath);
        kl.BulkImport();
    }
}

public class MsStreetImport : IKladrImport
{
    public ImportObjectInfo Ioi { get; set; } = new ImportObjectInfo
    {
        TargetTableName = "[STREET]",
        TargetSchemaName = "kladr",
        SourceFileName = "STREET.DBF",
        SourceDirPath = string.Empty,
        ConnectionString = string.Empty
    };

    public MsStreetImport(string sourceDirPath, string connectionString)
    {
        this.Ioi.SourceDirPath = sourceDirPath;
        this.Ioi.ConnectionString = connectionString;
    }

    public void DoImport()
    {
        MsSqlImport st = new MsSqlImport(this.Ioi);
        st.ReadDbfInfo(this.Ioi.SourceFilePath);
        st.BulkImport();
    }
}

public class MsDomaImport : IKladrImport
{
    public ImportObjectInfo Ioi { get; set; } = new ImportObjectInfo
    {
        TargetTableName = "[DOMA]",
        TargetSchemaName = "kladr",
        SourceFileName = "DOMA.DBF",
        SourceDirPath = string.Empty,
        ConnectionString = string.Empty
    };

    public MsDomaImport(string sourceDirPath, string connectionString)
    {
        this.Ioi.SourceDirPath = sourceDirPath;
        this.Ioi.ConnectionString = connectionString;
    }

    public void DoImport()
    {
        MsSqlImport dm = new MsSqlImport(this.Ioi);
        dm.ReadDbfInfo(this.Ioi.SourceFilePath);
        dm.BulkImport();
    }
}
