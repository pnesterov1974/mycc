
namespace Datamech.mssql.etlmodels
{
    public static class omodel
    {
        public static string TargetFolder = string.Empty;
        public static List<string> GetListOfModels()
        {
            if (Path.Exists(omodel.TargetFolder))
            {
                List<string> s = new List<string>();
                foreach(string file in Directory.EnumerateFiles(omodel.TargetFolder, "*.json", SearchOption.AllDirectories))
                {
                    //Console.WriteLine(file);
                    using (StreamReader r = new StreamReader(file))
                    {
                        string ss = r.ReadToEnd();
                        s.Add(ss);
                    }
                }
                return s;
            }
            else
            {
                Console.WriteLine($"Папка {omodel.TargetFolder} не существует");
                return null;
            }
        }
    }
}
