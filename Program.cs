using Serilog;
using Serilog.Events;
using Shared;
using KladrData;
using KladrData.DbfToPg;
using KladrData.DbfToMsSQL;
using Data.xmlToMsSql;
using Microsoft.Data.SqlClient;
using KLadrData.MsSqlData;
using Npgsql;

string logFileName = string.Join('_', "log", DateTime.Now.ToString("yyyMMdd_HHmmss_ssms"));

string logFolder = Path.Join(Directory.GetCurrentDirectory(), "_log");
if (!Path.Exists(logFolder))
{
    Directory.CreateDirectory(logFolder);
}
string logFilePath = Path.Combine(logFolder, logFileName);

string destDirPath = Path.Join(Directory.GetCurrentDirectory(), "_dest");
if (!Path.Exists(destDirPath))
{
    Directory.CreateDirectory(destDirPath);
}

using var log = new LoggerConfiguration()
    //.WriteTo.Console(theme: AnsiConsoleTheme.Code)
    .MinimumLevel.Debug()
    .WriteTo.Console(restrictedToMinimumLevel: LogEventLevel.Information)
    .WriteTo.File(logFilePath)
    .CreateLogger();

//string pgConnectionString = "Host=localhost;Username=postgres;Password=my_pass;Database=kladr";
//string MsSqlconnectionString = "Data Source=192.168.1.79,1433;Initial Catalog=kladr;User ID=sa;Password=Exptsci123;Trust Server Certificate=True;Connection Timeout=500";
//string MsSqlConnectionString = "Data Source=ETL-SSIS-D-02;Initial Catalog=kladr;Integrated Security=True;Pooling=True;Trust Server Certificate=True;Connection Timeout=500";
//string sourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "my_dev", "files", "kladr");
string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
string garSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "gar");

string MsSqlConnectionString = GetMsSqlConnectionString();
string pgConnectionString = GetPgSqlConnectionString();

DBUtils.Log = log;

log.Debug("Source Dir Path: {kladrSourceDirPath}", kladrSourceDirPath);

//Import(importKind: "pg");
//Import();
//ReadProc();
importGarToMsSql();

void ReadProc()
{
    MsSqlSchema scs = new MsSqlSchema(MsSqlConnectionString, SocrBase.SQL)
    {
        Log = log
    };
    scs.Read();
    //string jsfp1 = Path.Combine(destDirPath, "socrbase_schema.json");
    //File.WriteAllText(jsfp1, scs.AsJson());
    var r = scs.GetFieldList();

    MsSqlData scd = new MsSqlData(MsSqlConnectionString, SocrBase.SQL)
    {
        Log = log
    };
    scd.Read();
    //string jsfp2 = Path.Combine(destDirPath, "socrbase.json");
    //File.WriteAllText(jsfp2, scd.AsJson());

    // InsertIntoMsSql isb = new InsertIntoMsSql(MsSqlconnectionString, SocrBase.SQL, "dbo", "[t_socrbase]")
    // {
    //     Log = log
    // };
    // isb.InsertInto();
}

string GetMsSqlConnectionString()
{
    SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
    {
        ApplicationName = "GEO DataPipe Application",
        ConnectTimeout = 500,
        DataSource = "ETL-SSIS-D-02",
        InitialCatalog = "kladr",
        IntegratedSecurity = true,
        Pooling = true,
        TrustServerCertificate = true
    };
    return  builder.ToString();
}

string GetPgSqlConnectionString()
{ // "Host=localhost;Username=postgres;Password=my_pass;Database=kladr";
    NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder
    {
        ApplicationName = "GEO DataPipe Application",
        Host = "localhost",
        Username = "postgres",
        Password = "my_pass",
        Database = "kladr"
    };
    return  builder.ToString();
}

void Import(string importKind = "mssql")
{
    DateTime StartDt = DateTime.Now;
    log.Debug("StartDt: {dt}", StartDt);

    switch (importKind)
    {
        case "mssql":
            log.Information("Импорт DBF => Ms SQL");
            importDbfToMsSql();
            break;
        case "pg":
            log.Information("Импорт DBF => PG DB");
            importDbfToPg();
            break;
    }

    DateTime FinishDt = DateTime.Now;
    TimeSpan Duration = FinishDt - StartDt;
    log.Information("Обработка завершена в {finishDt}, продолжительность {Duration}", FinishDt, Duration);
}

void importDbfToPg()
{
    if (DBUtils.TryPgDbConnection(pgConnectionString))
    {
        ObjectInfo SocrBaseObjectInfo = new ObjectInfo
        {
            DestinationTableName = "socrbase",
            DestinationSchemaName = "public",
            SourceDBFFileName = "SOCRBASE.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = pgConnectionString
        };
        PgImportSocrBase sb = new PgImportSocrBase(SocrBaseObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(SocrBaseObjectInfo);
        sb.DoImport();

        ObjectInfo AltNamesObjectInfo = new ObjectInfo
        {
            DestinationTableName = "altnames",
            DestinationSchemaName = "public",
            SourceDBFFileName = "ALTNAMES.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = pgConnectionString
        };
        PgImportAltNames an = new PgImportAltNames(AltNamesObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(AltNamesObjectInfo);
        an.DoImport();

        ObjectInfo KladrObjectInfo = new ObjectInfo
        {
            DestinationTableName = "kladr",
            DestinationSchemaName = "public",
            SourceDBFFileName = "KLADR.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = pgConnectionString
        };
        PgImportKladr kl = new PgImportKladr(KladrObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(KladrObjectInfo);
        kl.DoImport();

        ObjectInfo StreetObjectInfo = new ObjectInfo
        {
            DestinationTableName = "street",
            DestinationSchemaName = "public",
            SourceDBFFileName = "STREET.DBF",
            BufferRecs = 200000,
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = pgConnectionString
        };
        PgImportStreet st = new PgImportStreet(StreetObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(StreetObjectInfo);
        st.DoImport();

        ObjectInfo DomaObjectInfo = new ObjectInfo
        {
            DestinationTableName = "doma",
            DestinationSchemaName = "public",
            SourceDBFFileName = "DOMA.DBF",
            BufferRecs = 1000000,
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = pgConnectionString
        };
        PgImportDoma dm = new PgImportDoma(DomaObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(DomaObjectInfo);
        dm.DoImport();
    }
}

void importDbfToMsSql()
{
    if (DBUtils.TryMsSqlDbConnection(MsSqlConnectionString))
    {
        ObjectInfo SocrBaseObjectInfo = new ObjectInfo
        {
            DestinationTableName = "[SOCRBASE]",
            DestinationSchemaName = "dbo",
            SourceDBFFileName = "SOCRBASE.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = MsSqlConnectionString
        };
        MsSqlImportDbfCommon sb = new MsSqlImportDbfCommon(SocrBaseObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(SocrBaseObjectInfo);
        sb.BulkImport();

        ObjectInfo NamesMapObjectInfo = new ObjectInfo
        {
            DestinationTableName = "[NameMap]",
            DestinationSchemaName = "dbo",
            SourceDBFFileName = "NAMEMAP.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = MsSqlConnectionString
        };
        MsSqlImportDbfCommon nm = new MsSqlImportDbfCommon(NamesMapObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(NamesMapObjectInfo);
        nm.BulkImport();

        ObjectInfo AltNamesObjectInfo = new ObjectInfo
        {
            DestinationTableName = "[ALTNAMES]",
            DestinationSchemaName = "dbo",
            SourceDBFFileName = "ALTNAMES.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = MsSqlConnectionString
        };
        MsSqlImportDbfCommon an = new MsSqlImportDbfCommon(AltNamesObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(AltNamesObjectInfo);
        an.BulkImport();

        ObjectInfo KladrObjectInfo = new ObjectInfo
        {
            DestinationTableName = "[KLADR]",
            DestinationSchemaName = "dbo",
            SourceDBFFileName = "KLADR.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = MsSqlConnectionString
        };
        MsSqlImportDbfCommon kl = new MsSqlImportDbfCommon(KladrObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(KladrObjectInfo);
        kl.BulkImport();

        ObjectInfo StreetObjectInfo = new ObjectInfo
        {
            DestinationTableName = "[STREET]",
            DestinationSchemaName = "dbo",
            SourceDBFFileName = "STREET.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = MsSqlConnectionString
        };
        MsSqlImportDbfCommon st = new MsSqlImportDbfCommon(StreetObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(StreetObjectInfo);
        st.BulkImport();

        ObjectInfo DomaObjectInfo = new ObjectInfo
        {
            DestinationTableName = "[DOMA]",
            DestinationSchemaName = "dbo",
            SourceDBFFileName = "DOMA.DBF",
            SourceDirPath = kladrSourceDirPath,
            ConnectionString = MsSqlConnectionString
        };
        MsSqlImportDbfCommon dm = new MsSqlImportDbfCommon(DomaObjectInfo)
        {
            Log = log
        };
        DBUtils.ReadDbfInfo(DomaObjectInfo);
        dm.BulkImport();
    }
}

void importGarToMsSql()
{
    ObjectInfo NormativeDocsKinds = new ObjectInfo
    {
        DestinationTableName = "[NormativeDocsKinds]",
        DestinationSchemaName = "gar",
        SourceDBFFileName = "AS_NORMATIVE_DOCS_KINDS_20240815_a1dc61a2-9b9a-4de6-bbac-350a7a04cddf.XML",
        SourceDirPath = garSourceDirPath,
        ConnectionString = MsSqlConnectionString
    };
    ImportOneFromXML ndk = new ImportOneFromXML(NormativeDocsKinds)
    {
        Log = log
    };
    ndk.DoImportNormativeDocsKinds();

    ObjectInfo AddHouseTypes = new ObjectInfo
    {
        DestinationTableName = "[AddHouseTypes]",
        DestinationSchemaName = "gar",
        SourceDBFFileName = "AS_ADDHOUSE_TYPES_20240815_656d52c9-09af-4b12-8d3c-5732f358506b.XML",
        SourceDirPath = garSourceDirPath,
        ConnectionString = MsSqlConnectionString
    };
    ImportOneFromXML aht = new ImportOneFromXML(AddHouseTypes)
    {
        Log = log
    };
    aht.DoImportAddHouseTypes();

    ObjectInfo AppartmentTypes = new ObjectInfo
    {
        DestinationTableName = "[AppartmentTypes]",
        DestinationSchemaName = "gar",
        SourceDBFFileName = "AS_APARTMENT_TYPES_20240815_6d8bafd4-c0b1-4185-a786-300406fc8bc1.XML",
        SourceDirPath = garSourceDirPath,
        ConnectionString = MsSqlConnectionString
    };
    ImportOneFromXML apt = new ImportOneFromXML(AppartmentTypes)
    {
        Log = log
    };
    apt.DoImportTypes1();

    ObjectInfo HouseTypes = new ObjectInfo
    {
        DestinationTableName = "[HouseTypes]",
        DestinationSchemaName = "gar",
        SourceDBFFileName = "AS_HOUSE_TYPES_20240815_5d97947a-230c-4bc8-ad24-6077a0be2d21.XML",
        SourceDirPath = garSourceDirPath,
        ConnectionString = MsSqlConnectionString
    };
    ImportOneFromXML ht = new ImportOneFromXML(HouseTypes)
    {
        Log = log
    };
    ht.DoImportTypes1();

    ObjectInfo NormativeDocsTypes = new ObjectInfo
    {
        DestinationTableName = "[NormativeDocsTypes]",
        DestinationSchemaName = "gar",
        SourceDBFFileName = "AS_NORMATIVE_DOCS_TYPES_20240815_a1706595-44dc-4344-92d4-bea8f42dada7.XML",
        SourceDirPath = garSourceDirPath,
        ConnectionString = MsSqlConnectionString
    };
    ImportOneFromXML ndt = new ImportOneFromXML(NormativeDocsTypes)
    {
        Log = log
    };
    ndt.DoImportTypes2();
}
