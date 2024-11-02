using static Shared.MyLogger;
using Shared.connections;
using Datamech;
//using Datamech.pgsql;
//using System.Data;
//using Dataread.pgsql;
//using Dataread.mssql;
using Datamech.mssql;
using Shared;

// TODO
// execute model as table or as view materialize_type
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
ConnectionStrings.InitConnectionStrings(Shared.TargetDb.mssql);
string modelsFolder = Path.Join(Directory.GetCurrentDirectory(), "models");
Log.Information("Папка моделей: {modelsfolder}", modelsFolder);

//SerilizeModels();
RunModels();
//RunReadModels();
//ValidateEtlModel();

void RunModels()
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
            RunModel rm = new RunModel(etlModel);
            rm.Run();
        }
        catch (Exception ex)
        {
            Log.Information("Модель {etl} пропущена из-за ошибки", etlModel);
            Log.Error("\n{errorMessage}", ex.Message);
        }
    }
}

void SerilizeModels()
{
    /*
    Kladr1Model klm1 = new Kladr1Model();
    Serializator klm1s = new Serializator(klm1);
    klm1s.SerializeToJson();
    klm1s.SerializeToYaml();

    Street1Model str1 = new Street1Model();
    Serializator str1s = new Serializator(str1);
    str1s.SerializeToJson();
    str1s.SerializeToYaml();

    Doma1Model dm1 = new Doma1Model();
    Serializator dm1s = new Serializator(dm1);
    dm1s.SerializeToJson();
    dm1s.SerializeToYaml();
    */
}

void RunReadModels()
{
    // SocrBaseJustImportedModel sb = new SocrBaseJustImportedModel();
    // try
    // {
    //     RunModel rm1 = new RunModel(sb);
    //     //string s = rm1.DataAsJson;
    //     //Log.Information(s);
    //     string ss = rm1.SchemaAsJson;
    //     Log.Information(ss);
    // }
    // catch (Exception ex)
    // {
    //     Log.Information("Модель {klm1} пропущена из-за ошибки", sb);
    //     Log.Error("\n{errorMessage}", ex.Message);
    // }
}

void ValidateEtlModel()
{
    Log.Information("Модель 1 ----------------------------------");
    Kladr1Model klm1 = new Kladr1Model();
    var d = new ValidateModel(TargetDb.mssql, klm1);
    d.Validate();

    Log.Information("Модель 2 ----------------------------------");
    ssisModel sm = new ssisModel();
    var smd = new ValidateModel(TargetDb.mssql, sm);
    smd.Validate();
}
