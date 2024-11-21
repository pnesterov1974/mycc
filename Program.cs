using static Shared.MyLogger;
using Shared.connections;
using Datamech;
//using Datamech.pgsql;
//using Datamech.pgsql.etlmodels;
using Datamech.mssql;
using Datamech.mssql.etlmodels;
//using System.Data;
//using Dataread.pgsql;
//using Dataread.mssql;
//using import_kladr_efcpg;
using import_kladr_efcms;

using Shared;

// TODO
// pg: when create table with pk Cannot define PRIMARY KEY constraint on nullable column in table 'kladr_1'.
// datameshmodels
// -- targetFullName exclude from etlModelBase (need for sirialization)
// -- presql  --postsql  --pullmotivation -- pushmotivation
// -- parameters in sqls {@Param1} {@Param2}
// -- execute-model-as   table vs view
// -- techfields list
// -- validate model name unikness whithin pack
// -- sameServer sameDb
// -- create target schema in not exists
// -- batchSizeInModel Batch vs Page names

// tech_db  <-use delegates and events
// -- executions
// -- locks
// -- target_table_status
// -- masters of target ???

// -- same-server  --same-dbtype => cross-connections import mssql->mssql mssql->pg pg->pg pg->mssql

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
    ConnectionStrings.InitConnectionStrings(Shared.TargetDb.mssql);
}
//ConnectionStrings.InitConnectionStrings(Shared.TargetDb.pgsql);
string etlModelsFolder = Path.Join(Directory.GetCurrentDirectory(), "etl_models");
string readModelsFolder = Path.Join(Directory.GetCurrentDirectory(), "read_models");
Log.Information("Папка etl-моделей: {etlModelsfolder}", etlModelsFolder);
Log.Information("Папка read-моделей: {readModelsfolder}", readModelsFolder);

//SerilizeEtlModels();
RunClientFaceEtlModels();
//RunEtlModels();
//RunReadModels();
//ValidateEtlModels();
//ReadEf();
//TestOneEtlModel();

void RunEtlModels()
{
    /*
    //ssisModel ssm = new ssisModel();
    //RunModel rm = new RunModel(ssm);
    //rm.Run();
    */
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
    /*
    //ssisModel ssm = new ssisModel();
    //RunModel rm = new RunModel(ssm);
    //rm.Run();
    */
    var ClientFaceModels = new ClientFaceModelsCatalog();

    foreach (var etlModel in ClientFaceModels.Models)
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

void SerilizeEtlModels()
{
    KladrModelsCatalog mcatalog = new KladrModelsCatalog();
    foreach (var model in mcatalog.Models)
    {
        Serializator szr = new Serializator(model);
        szr.SerializeToJson();
        szr.SerializeToYaml();
    }
    // Kladr1Model klm1 = new Kladr1Model();
    // Serializator klm1s = new Serializator(klm1);
    // klm1s.SerializeToXml();
}

void RunReadModels()
{
    // var kladr = new KladrReadModelsCatalog();

    // foreach (var readModel in kladr.Models)
    // {
    //     try
    //     {
    //         RunReadModel rm = new RunReadModel(readModel);
    //         string ss = rm.SchemaAsJson;
    //         Log.Information(ss);
    //     }
    //     catch (Exception ex)
    //     {
    //         Log.Information("Модель {readModel} пропущена из-за ошибки", readModel);
    //         Log.Error("\n{errorMessage}", ex.Message);
    //     }
    // }
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

    //Log.Information("Модель 1 ----------------------------------");
    //Kladr1Model klm1 = new Kladr1Model();
    //var d = new ValidateModel(TargetDb.mssql, klm1);
    //d.Validate();

    //Log.Information("Модель 2 ----------------------------------");
    //ssisModel sm = new ssisModel();
    //var smd = new ValidateModel(TargetDb.mssql, sm);
    //smd.Validate();
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
    //KladrEfcImport.SaveKladrSimple();
    //KladrEfcImport.SaveStreetSimple();
    //KladrEfcImport.SaveKladrCombined();
    //KladrEfcImport.SaveStreetCombined();
}

void TestOneEtlModel()
{
    NewModel nm = new NewModel();
    var t = nm.GetParameterNames();
}
