using Data;
using Import.kladr.mssql;
using Shared;

namespace ImportKladr.mssql;

static class ImportKladrToMsSql
{
    public static void DoImport(string connectionString, string sourceDirPath, List<string> kladrObjects)
    {
        BaseMsSql msb = new BaseMsSql();
        if (msb.TryMsSqlDbConnection(connectionString))
        {
            if (kladrObjects.Contains<string>("socrbase"))
            {
                ImportObjectInfo SocrBaseObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "[SOCRBASE]",
                    TargetSchemaName = "dbo",
                    SourceFileName = "SOCRBASE.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                MsSqlImport sb = new MsSqlImport(SocrBaseObjectInfo);
                sb.ReadDbfInfo(SocrBaseObjectInfo.SourceFilePath);
                sb.BulkImport();
            }

            if (kladrObjects.Contains<string>("namemap"))
            {
                ImportObjectInfo NamesMapObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "[NameMap]",
                    TargetSchemaName = "dbo",
                    SourceFileName = "NAMEMAP.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                MsSqlImport nm = new MsSqlImport(NamesMapObjectInfo);
                nm.ReadDbfInfo(NamesMapObjectInfo.SourceFilePath);
                nm.BulkImport();
            }

            if (kladrObjects.Contains<string>("altnames"))
            {

                ImportObjectInfo AltNamesObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "[ALTNAMES]",
                    TargetSchemaName = "dbo",
                    SourceFileName = "ALTNAMES.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                MsSqlImport an = new MsSqlImport(AltNamesObjectInfo);
                an.ReadDbfInfo(AltNamesObjectInfo.SourceFilePath);
                an.BulkImport();
            }

            if (kladrObjects.Contains<string>("kladr"))
            {
                ImportObjectInfo KladrObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "[KLADR]",
                    TargetSchemaName = "dbo",
                    SourceFileName = "KLADR.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                MsSqlImport kl = new MsSqlImport(KladrObjectInfo);
                kl.ReadDbfInfo(KladrObjectInfo.SourceFilePath);
                kl.BulkImport();
            }

            if (kladrObjects.Contains<string>("street"))
            {
                ImportObjectInfo StreetObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "[STREET]",
                    TargetSchemaName = "dbo",
                    SourceFileName = "STREET.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                MsSqlImport st = new MsSqlImport(StreetObjectInfo);
                st.ReadDbfInfo(StreetObjectInfo.SourceFilePath);
                st.BulkImport();
            }

            if (kladrObjects.Contains<string>("doma"))
            {
                ImportObjectInfo DomaObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "[DOMA]",
                    TargetSchemaName = "dbo",
                    SourceFileName = "DOMA.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                MsSqlImport dm = new MsSqlImport(DomaObjectInfo);
                dm.ReadDbfInfo(DomaObjectInfo.SourceFilePath);
                dm.BulkImport();
            }
        }
    }
}
