using Data;
using Import.kladr.pgsql;
using Shared;

namespace ImportKladr.pgsql;

static class ImportKladrToPgSql
{
    public static void DoImport(string connectionString, string sourceDirPath, List<string> kladrObjects)
    {
        BasePgSql bpg = new BasePgSql();
        if (bpg.TryPgDbConnection(connectionString))
        {
            if (kladrObjects.Contains<string>("socrbase"))
            {
                ImportObjectInfo SocrBaseObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "socrbase",
                    TargetSchemaName = "public",
                    SourceFileName = "SOCRBASE.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                PgImportSocrBase sb = new PgImportSocrBase(SocrBaseObjectInfo);
                sb.ReadDbfInfo(SocrBaseObjectInfo.SourceFilePath);
                sb.DoImport();
            }

            if (kladrObjects.Contains<string>("altnames"))
            {
                ImportObjectInfo AltNamesObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "altnames",
                    TargetSchemaName = "public",
                    SourceFileName = "ALTNAMES.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                PgImportAltNames an = new PgImportAltNames(AltNamesObjectInfo);
                an.ReadDbfInfo(AltNamesObjectInfo.SourceFilePath);
                an.DoImport();
            }

            if (kladrObjects.Contains<string>("kladr"))
            {
                ImportObjectInfo KladrObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "kladr",
                    TargetSchemaName = "public",
                    SourceFileName = "KLADR.DBF",
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                PgImportKladr kl = new PgImportKladr(KladrObjectInfo);
                kl.ReadDbfInfo(KladrObjectInfo.SourceFilePath);
                kl.DoImport();
            }

            if (kladrObjects.Contains<string>("street"))
            {
                ImportObjectInfo StreetObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "street",
                    TargetSchemaName = "public",
                    SourceFileName = "STREET.DBF",
                    BufferRecs = 200000,
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                PgImportStreet st = new PgImportStreet(StreetObjectInfo);
                st.ReadDbfInfo(StreetObjectInfo.SourceFilePath);
                st.DoImport();
            }

            if (kladrObjects.Contains<string>("doma"))
            {
                ImportObjectInfo DomaObjectInfo = new ImportObjectInfo
                {
                    TargetTableName = "doma",
                    TargetSchemaName = "public",
                    SourceFileName = "DOMA.DBF",
                    BufferRecs = 1000000,
                    SourceDirPath = sourceDirPath,
                    ConnectionString = connectionString
                };
                PgImportDoma dm = new PgImportDoma(DomaObjectInfo);
                dm.ReadDbfInfo(DomaObjectInfo.SourceFilePath);
                dm.DoImport();
            }
        }
    }
}
