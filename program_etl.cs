using static Shared.MyLogger;
using Datamech.mssql;

InitLogger();
string modelsFolder = Path.Join(Directory.GetCurrentDirectory(), "models");
Log.Information("Папка моделей: {modelsfolder}", modelsFolder);

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

