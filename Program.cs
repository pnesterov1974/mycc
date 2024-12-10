using static Shared.MyLogger;
using Shared;
using Shared.connections;
using Shared.importModels;
using Datamech;
//using Datamech.pgsql;
//using Datamech.pgsql.etlmodels;
using Datamech.mssql;
using Datamech.mssql.etlmodels;
using Datareader.mssql;
using importGeoEfc;
using ImportGeo.kladr.mssql;
//using ImportGeo.kladr.pgsql;

// ===== model validation
// check if targetschema exists
// parameters in sqls {@Param1} {@Param2}  ! arch
// same server instance vs same db ->
//    model execution options   insertinto/bulkinsert/batsches
// -- model name uniquness


// ==== model
// -- targetFullName exclude from etlModelBase (need for sirialization)
// -- presql  --postsql  --pullmotivation -- pushmotivation
// -- execute-model-as   table vs view
// -- techfields list
// -- batchSizeInModel Batch vs Page names

// TODO
// pg: when create table with pk Cannot define PRIMARY KEY constraint on nullable column in table 'kladr_1'.
// datameshmodels
//
// -- 

// tech_db  <-use delegates and events
// -- executions
// -- locks

// in pg:
// efc_import_kladr delete rows in advanced
// efc_import_kladr CombineImport use batch count

// Review conrolflow with Exceptions Validation pre execute No validation while execute pg + ms
// model yaml & json deserialization

// efc_import_kladr FirstImportModels + BusinessModels ?? NotMappedFields in models

// model xml serialization
// pgsql: RunBatch ... using bulkInsert

// Многопоточность

InitLogger();
if (!ConnectionStrings.ConnectionStringsInitialized())
{
    //ConnectionStrings.InitConnectionStrings(Shared.TargetDb.pgsql);
    ConnectionStrings.InitConnectionStrings(Shared.TargetDb.mssql, useAD:true);
}
string etlModelsFolder = Path.Join(Directory.GetCurrentDirectory(), "etl_models");
string readModelsFolder = Path.Join(Directory.GetCurrentDirectory(), "read_models");
Log.Information("Папка etl-моделей: {etlModelsfolder}", etlModelsFolder);
Log.Information("Папка read-моделей: {readModelsfolder}", readModelsFolder);

//SerilizeEtlModels();
//RunClientFaceEtlModels();
//RunEtlModels();
RunReadModelsLinq();
//ValidateEtlModels();
//ReadEf();
//ImportWithoutEfMsSql();
//TestOneEtlModel();

void RunEtlModels()
{
    var KladrModelsCat = new KladrModelsCatalog();

    foreach (var etlModel in KladrModelsCat.Models)
    {
        try
        {
            RunEtlModel rm = new RunEtlModel(etlModel);
            rm.Run();
        }
        catch (Exception ex)
        {
            Log.Information("Модель {etl} пропущена из-за ошибки", etlModel);
            Log.Error("\n{errorMessage}", ex.Message);
        }
    }
}

void RunClientFaceEtlModels()
{
    // var ClientFaceModels = new ClientFaceModelsCatalog();

    // foreach (var etlModel in ClientFaceModels.Models)
    // {
    //     try
    //     {
    //         RunEtlModel rm = new RunEtlModel(etlModel);
    //         rm.Run();
    //     }
    //     catch (Exception ex)
    //     {
    //         Log.Information("Модель {etl} пропущена из-за ошибки", etlModel);
    //         Log.Error("\n{errorMessage}", ex.Message);
    //     }
    // }
}

void SerilizeEtlModels()
{
    KladrModelsCatalog mcatalog = new KladrModelsCatalog();
    foreach (var model in mcatalog.Models)
    {
        Serializator szr = new Serializator(model);
        szr.SerializeToJson();
        szr.SerializeToYaml();
    }
}

void RunReadModelsLinq()
{
    Analizator.Analize1();
}

void ValidateEtlModels()
{

    var KladrModelsCat = new KladrModelsCatalog();

    foreach (var etlModel in KladrModelsCat.Models)
    {
        try
        {
            //var v = new ValidateEtlModel(TargetDb.pgsql, etlModel);
            var v = new ValidateEtlModel(TargetDb.mssql, etlModel);
            v.Validate();
        }
        catch (Exception ex)
        {
            Log.Information("Модель {etl} пропущена из-за ошибки", etlModel);
            Log.Error("\n{errorMessage}", ex.Message);
        }
    }
}

void ValidateReadModels()
{

    var KladrModelsCat = new KladrModelsCatalog();

    foreach (var etlModel in KladrModelsCat.Models)
    {
        try
        {
            var v = new ValidateEtlModel(TargetDb.pgsql, etlModel);
            v.Validate();
        }
        catch (Exception ex)
        {
            Log.Information("Модель {etl} пропущена из-за ошибки", etlModel);
            Log.Error("\n{errorMessage}", ex.Message);
        }
    }
}

void ReadEf()
{
    //KladrEfcImport.SaveKladr(TargetDb.pgsql);
    //var worker = new KladrEfcImportWorker{ tdb = TargetDb.mssql };
    var worker = new KladrEfcImportWorker{ tdb = TargetDb.pgsql };
    worker.SaveSocrBaseUsingEnumerator();
    worker.SaveAltNamesUsingEnumerator();
    worker.SaveKladrUsingEnumerator();
    worker.SaveKladrUsingEnumerator();
    worker.SaveStreetUsingEnumerator();
    worker.SaveDomaUsingEnumerator();
}

void TestOneEtlModel()
{
    //NewModel nm = new NewModel();
    //var t = nm.GetParameterNames();
}

void ImportWithoutEfPg()
{
    // var altnames = new PgImportAltNames()
    // {
    //     TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
    //     TargetTableName = "altnames",
    //     TargetSchemaName = "kladr"
    // };
    // altnames.DoImport();

    // var socrbase = new PgImportSocrBase()
    // {
    //     TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
    //     TargetTableName = "socrbase",
    //     TargetSchemaName = "kladr"
    // };
    // socrbase.DoImport();

    // var kladr = new PgImportKladr()
    // {
    //     TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
    //     TargetTableName = "kladr",
    //     TargetSchemaName = "kladr"
    // };
    // kladr.DoImport();

    // var street = new PgImportStreet()
    // {
    //     TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
    //     TargetTableName = "street",
    //     TargetSchemaName = "kladr"
    // };
    // street.DoImport();

    // var doma = new PgImportDoma()
    // {
    //     TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
    //     TargetTableName = "doma",
    //     TargetSchemaName = "kladr"
    // };
    // doma.DoImport();
}

void ImportWithoutEfMsSql()
{
    var altnames = new MsSqlImport()
    {
        ImportModel = new AltNamesImportModel(),
        TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
        TargetTableName = "ALTNAMES",
        TargetSchemaName = "dbo"
    };
    altnames.BulkImport();

    var socrbase = new MsSqlImport()
    {
        ImportModel = new SocrBaseImportModel(),
        TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
        TargetTableName = "SOCRBASE",
        TargetSchemaName = "dbo"
    };
    socrbase.BulkImport();

    var kladr = new MsSqlImport()
    {
        ImportModel = new KladrImportModel(),
        TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
        TargetTableName = "KLADR",
        TargetSchemaName = "dbo"
    };
    kladr.BulkImport();

    var street = new MsSqlImport()
    {
        ImportModel = new StreetImportModel(),
        TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
        TargetTableName = "STREET",
        TargetSchemaName = "dbo"
    };
    street.BulkImport();

    var doma = new MsSqlImport()
    {
        ImportModel = new DomaImportModel(),
        TargetConnectionString = ConnectionStrings.GetConnectionStringByName("kladrStage"),
        TargetTableName = "DOMA",
        TargetSchemaName = "dbo"
    };
    doma.BulkImport();
}

