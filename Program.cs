using static Shared.MyLogger;
using Shared.connections;
using Datamech;
using Datamech.pgsql;
using Datamech.pgsql.etlmodels;
//using Datamech.mssql;
//using System.Data;
using Dataread.pgsql;
//using Dataread.mssql;
//using import_kladr_efcpg;
using import_kladr_efcms;

using Shared;

// TODO
// execute model as table or as view materialize_type
// model xml serializatiom
// validate etl model = same instance
// model-tree pull_motivation, push_motivation
// cross-connections import mssql->mssql mssql->pg pg->pg pg->mssql
// pgsql: RunBatch ... using bulkInsert
// models serialize / deserialize in batch.
// Тенические поля
// Review with Exceptions
// Многопоточность
// mssql - connections to business objects
// techdbs: table_status locks executions (1. stru)
// model ppl: +how to mart first base model

InitLogger();
if (!ConnectionStrings.ConnectionStringsInitialized())
{
    ConnectionStrings.InitConnectionStrings(Shared.TargetDb.mssql);
}
//ConnectionStrings.InitConnectionStrings(Shared.TargetDb.pgsql);
string etlModelsFolder = Path.Join(Directory.GetCurrentDirectory(), "etl_models");
string readModelsFolder = Path.Join(Directory.GetCurrentDirectory(), "read_models");
Log.Information("Папка etl-моделей: {etlModelsfolder}", etlModelsFolder);
Log.Information("Папка read-моделей: {readModelsfolder}", readModelsFolder);

//SerilizeEtlModels();
//RunEtlModels();
//RunReadModels();
//ValidateEtlModels();
ReadEf();

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
    var kladr = new KladrReadModelsCatalog();

    foreach (var readModel in kladr.Models)
    {
        try
        {
            RunReadModel rm = new RunReadModel(readModel);
            string ss = rm.SchemaAsJson;
            Log.Information(ss);
        }
        catch (Exception ex)
        {
            Log.Information("Модель {readModel} пропущена из-за ошибки", readModel);
            Log.Error("\n{errorMessage}", ex.Message);
        }
    }
}

void ValidateEtlModels()
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
    //FirstDBRun.SaveKladr();
    //FirstDBRun.SaveStreet();
    //FirstDBRun.SaveKladr();
    //FirstDBRun.SaveKladrCombined();
    FirstDBRun.SaveStreetCombined();
    //FirstDBRun.KladrData();
}
