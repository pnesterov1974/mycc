using static Shared.MyLogger;
using Datamech.mssql;

InitLogger();
string modelsFolder = Path.Join(Directory.GetCurrentDirectory(), "models");
Log.Information("Папка моделей: {modelsfolder}", modelsFolder);

SerilizeModels();
RunModels();

void RunModels()
{
    Kladr1Model klm1 = new Kladr1Model();
    RunModel rm1 = new RunModel(klm1);
    rm1.Run();

    Street1Model str1 = new Street1Model();
    RunModel rm2 = new RunModel(str1);
    rm2.Run();

    Doma1Model dm1 = new Doma1Model();
    RunModel rm3 = new RunModel(dm1);
    rm3.Run();
}
void SerilizeModels()
{
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
}
