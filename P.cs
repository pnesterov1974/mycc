using CommandLine;
using Options;
using ImportKladr;
using ImportGar;
using Shared;
using Shared.Transformations.mssql;
using static Shared.MyLogger;
using DatameshMsSql;

//--------------

// TODO:
// ObjectInfo -> ImportObjectInfo
// gat - use new sourcefilename stru, add xsd files to stru
// gar import - validate xml by schema
//      https://learn.microsoft.com/ru-ru/dotnet/standard/linq/validate-xsd
//      https://www.c-sharpcorner.com/article/how-to-validate-xml-using-xsd-in-c-sharp/
// using cli
// gar facts source file procesing...
// mvc make simple repository class (SocrBase)
// common objects to shared = folders ??
// kladr 3 steps after import aka dbt.
// socrbase, altnames, nmaps into steps raw vs stage vs ods shemas gar vs kladr dbs

GeoType gtp = 0;
TargetDb tdb = 0;
List<string> kladrObjectList = new List<string>();
string[] allKladrObjects = new string[] { "SocrBase", "AltNames", "Kladr", "Street", "Doma", "NameMap" };
List<string> garObjectList = new List<string>();
string[] allGarObjects = new string[] {
    "AddHouseTypes", "AddrObjTypes", "AppartmentTypes", "HouseTypes", "NormativeDocsKinds", "NormativeDocsTypes",
    "ObjectLevels", "OperationTypes", "ParamTypes", "RoomTypes"
    };
/*
Parser.Default.ParseArguments<CliOptions>(args)
    .WithParsed<CliOptions>(o =>
        {
            if (o.GeoType.Equals("kladr"))
            {
                gtp = GeoType.kladr;
                string[] allKladrObjectsLowercase = new string[allKladrObjects.Length];
                for (int i = 0; i < allKladrObjects.Length; i++)
                    allKladrObjectsLowercase[i] = allKladrObjects[i].ToLower();
                foreach (var s in o.Objects)
                {
                    int i = Array.IndexOf(allKladrObjectsLowercase, s.ToLower());
                    if (i >= 0)
                    {
                        if (!kladrObjectList.Contains<string>(s))
                            kladrObjectList.Add(allKladrObjects[i]);
                    }
                }
            }
            else if (o.GeoType.Equals("gar"))
            {
                gtp = GeoType.gar;
                string[] allGarObjectsLowercase = new string[allGarObjects.Length];
                for (int i = 0; i < allKladrObjects.Length; i++)
                    allGarObjectsLowercase[i] = allGarObjects[i].ToLower();
                foreach (var s in o.Objects)
                {
                    int i = Array.IndexOf(allGarObjectsLowercase, s.ToLower());
                    if (i >= 0)
                    {
                        if (!garObjectList.Contains<string>(s))
                            garObjectList.Add(allGarObjects[i]);
                    }
                }
            }

            if (o.TargetDb.Equals("mssql"))
            {
                tdb = TargetDb.mssql;
            }
            else if (o.GeoType.Equals("pgsql"))
            {
                tdb = TargetDb.pgsql;
            }
        }
    );
*/
InitLogger();

// Log.Information("Классификатор: {geotype}", gtp);
// Log.Information("Целевая БД: {targetDb}", tdb);
// if (gtp == GeoType.kladr)
//     Log.Information("Объекты: {kladrObjects}", kladrObjectList.ToString());
// if (gtp == GeoType.gar)
//     Log.Information("Объекты: {garObjects}", garObjectList.ToString());

string destDirPath = Path.Join(Directory.GetCurrentDirectory(), "_dest");
if (!Path.Exists(destDirPath))
{
    Directory.CreateDirectory(destDirPath);
}

//string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "my_dev", "files", "kladr");
string kladrSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "kladr");
string garSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "dotNetProjects", "files", "gar");
//string garSourceDirPath = Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "my_dev", "files", "gar");

string MsSqlConnectionString = ConnectionString.GetConnectionString(TargetDb.mssql);
string PgSqlConnectionString = ConnectionString.GetConnectionString(TargetDb.pgsql);

TestMech();
//ReadJson();
//ImportKladr(DBSource.pgsql);
//ImportGar(TargetDb.pgsql);

void ImportKladr(TargetDb tdb = TargetDb.mssql)
{
    DateTime StartDt = DateTime.Now;
    Log.Debug("StartDt: {dt}", StartDt);

    switch (tdb)
    {
        case TargetDb.mssql:
            Log.Information("Импорт DBF => Ms SQL");
            ImportKladrToMsSql.DoImport(MsSqlConnectionString, kladrSourceDirPath);
            break;
        case TargetDb.pgsql:
            Log.Information("Импорт DBF => PG DB");
            ImportKladrToPgSql.DoImport(PgSqlConnectionString, kladrSourceDirPath);
            break;
    }

    DateTime FinishDt = DateTime.Now;
    TimeSpan Duration = FinishDt - StartDt;
    Log.Information("Обработка завершена в {finishDt}, продолжительность {Duration}", FinishDt, Duration);
}

void ImportGar(TargetDb tdb = TargetDb.mssql)
{
    DateTime StartDt = DateTime.Now;
    Log.Debug("StartDt: {dt}", StartDt);

    switch (tdb)
    {
        case TargetDb.mssql:
            Log.Information("Импорт XML => MS SQL");
            ImportGarToMsSql impMsSql = new ImportGarToMsSql(garSourceDirPath);
            impMsSql.DoImport(MsSqlConnectionString);
            break;
        case TargetDb.pgsql:
            Log.Information("Импорт XML => PG DB");
            ImportGarToPgSql impPgSql = new ImportGarToPgSql(garSourceDirPath);
            impPgSql.DoImport(PgSqlConnectionString);
            break;
    }

    DateTime FinishDt = DateTime.Now;
    TimeSpan Duration = FinishDt - StartDt;
    Log.Information("Обработка завершена в {finishDt}, продолжительность {Duration}", FinishDt, Duration);
}

void ReadJson()
{
    string selectSql = SocrBaseMsSql.SQL;
    MsSqlData msd = new MsSqlData(MsSqlConnectionString, selectSql);
    string js = msd.AsJson;
    Console.WriteLine(js);
}

void TestMech()
{
    TransformObjectInfo toiKladr1 = new TransformObjectInfo
    {
        TargetSchemaName = Kladr1MsSql.TargetSchemaName,
        TargetTableName = Kladr1MsSql.TargetTableName,
        ConnectionString = MsSqlConnectionString,
        SelectSql = Kladr1MsSql.SQL,

    };
    InsertIntoMsSql insKladr1 = new InsertIntoMsSql(toiKladr1);
    insKladr1.InsertInto();

    TransformObjectInfo toiStreet1 = new TransformObjectInfo
    {
        TargetSchemaName = Street1MsSql.TargetSchemaName,
        TargetTableName = Street1MsSql.TargetTableName,
        ConnectionString = MsSqlConnectionString,
        SelectSql = Street1MsSql.SQL,

    };
    InsertIntoMsSql insStreet1 = new InsertIntoMsSql(toiStreet1);
    insStreet1.InsertInto();

    TransformObjectInfo toiDoma1 = new TransformObjectInfo
    {
        TargetSchemaName = Doma1MsSql.TargetSchemaName,
        TargetTableName = Doma1MsSql.TargetTableName,
        ConnectionString = MsSqlConnectionString,
        SelectSql = Doma1MsSql.SQL,

    };
    InsertIntoMsSql insDoma1 = new InsertIntoMsSql(toiDoma1);
    insDoma1.InsertInto();

}
