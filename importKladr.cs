using DbfDataReader;
using import_kladr_efcms.models;
using static Shared.MyLogger;

namespace import_kladr_efcms;

public static class FirstDBRun
{
    public static void KladrData()
    {
        using (KladrContext ctx = new KladrContext())
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                Console.WriteLine("Подключаюсь...");
                var kladrs = ctx.Kladrs.ToList();
                foreach (var kladr in kladrs)
                {
                    Console.WriteLine($"{kladr.NAME} {kladr.SOCR} {kladr.CODE} {kladr.INDEX} {kladr.GNINMB} {kladr.UNO} {kladr.OCATD} {kladr.STATUS}");
                }
            }
            else
            {
                Console.WriteLine("Подулючение недоступно...");
            }
        }
    }

    public static void SaveKladr()
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта STREET: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext())
        {
            List<Kladr> kladr = FirstDBRun.DoImportKladr();
            foreach (var k in kladr)
            {
                //ctx.Add<Kladr>(k);
                ctx.Kladrs.Add(k);
            }
            int recs = ctx.SaveChanges();
            Console.WriteLine($"Записано {recs} записей");
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта STREET: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта STREET: {tsp}", tsp);
    }

    public static void SaveStreet()
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта STREET: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext())
        {
            List<Street> street = FirstDBRun.DoImportStreet();
            foreach (var s in street)
            {
                ctx.Streets.Add(s);
            }
            int recs = ctx.SaveChanges();
            Console.WriteLine($"Записано {recs} записей");
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта STREET: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта STREET: {tsp}", tsp);
    }

    public static List<Kladr> DoImportKladr()
    {
        IImportObjectInfo oi = new KladrImportObjectInfo();
        List<Kladr> data = new List<Kladr>();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        Console.WriteLine(sourceFilePath);
        using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
        {
            while (dbfDataReader.Read())
            {
                var name = dbfDataReader.GetString(0);
                var socr = dbfDataReader.GetString(1);
                var code = dbfDataReader.GetString(2);
                var index = dbfDataReader.GetString(3);
                var gninmb = dbfDataReader.GetString(4);
                var uno = dbfDataReader.GetString(5);
                var ocatd = dbfDataReader.GetString(6);
                var status = dbfDataReader.GetString(7);

                var kladr = new Kladr
                {
                    NAME = name,
                    SOCR = socr,
                    CODE = code,
                    INDEX = index,
                    GNINMB = gninmb,
                    UNO = uno,
                    OCATD = ocatd,
                    STATUS = int.Parse(status)
                };
                data.Add(kladr);
            }
        }
        Console.WriteLine($"Считано {data.Count} записей");
        return data;
    }

    public static List<Street> DoImportStreet()
    {
        IImportObjectInfo oi = new StreetImportObjectInfo();
        List<Street> data = new List<Street>();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        Console.WriteLine(sourceFilePath);
        using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
        {
            while (dbfDataReader.Read())
            {
                var name = dbfDataReader.GetString(0);
                var socr = dbfDataReader.GetString(1);
                var code = dbfDataReader.GetString(2);
                var index = dbfDataReader.GetString(3);
                var gninmb = dbfDataReader.GetString(4);
                var uno = dbfDataReader.GetString(5);
                var ocatd = dbfDataReader.GetString(6);

                var street = new Street
                {
                    NAME = name,
                    SOCR = socr,
                    CODE = code,
                    INDEX = index,
                    GNINMB = gninmb,
                    UNO = uno,
                    OCATD = ocatd
                };
                data.Add(street);
            }
        }
        Console.WriteLine($"Считано {data.Count} записей");
        return data;
    }

    public static void SaveKladrCombined()
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта KLADR: {sdt}", startDt);
        IImportObjectInfo oi = new KladrImportObjectInfo();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        Console.WriteLine(sourceFilePath);
        using (KladrContext ctx = new KladrContext())
        {
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
            {
                while (dbfDataReader.Read())
                {
                    var name = dbfDataReader.GetString(0);
                    var socr = dbfDataReader.GetString(1);
                    var code = dbfDataReader.GetString(2);
                    var index = dbfDataReader.GetString(3);
                    var gninmb = dbfDataReader.GetString(4);
                    var uno = dbfDataReader.GetString(5);
                    var ocatd = dbfDataReader.GetString(6);
                    var status = dbfDataReader.GetString(7);

                    var kladr = new Kladr
                    {
                        NAME = name,
                        SOCR = socr,
                        CODE = code,
                        INDEX = index,
                        GNINMB = gninmb,
                        UNO = uno,
                        OCATD = ocatd,
                        STATUS = int.Parse(status)
                    };
                    ctx.Kladrs.Add(kladr);
                }
                int recs = ctx.SaveChanges();
                Console.WriteLine($"Записано {recs} записей");
            }
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта KLADR: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта KLADR: {tsp}", tsp);
    }

    public static void SaveStreetCombined()
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта STREET: {sdt}", startDt);
        IImportObjectInfo oi = new StreetImportObjectInfo();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        Console.WriteLine(sourceFilePath);
        using (KladrContext ctx = new KladrContext())
        {
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
            {
                while (dbfDataReader.Read())
                {
                    var name = dbfDataReader.GetString(0);
                    var socr = dbfDataReader.GetString(1);
                    var code = dbfDataReader.GetString(2);
                    var index = dbfDataReader.GetString(3);
                    var gninmb = dbfDataReader.GetString(4);
                    var uno = dbfDataReader.GetString(5);
                    var ocatd = dbfDataReader.GetString(6);
                    //var status = dbfDataReader.GetString(7);

                    var street = new Street
                    {
                        NAME = name,
                        SOCR = socr,
                        CODE = code,
                        INDEX = index,
                        GNINMB = gninmb,
                        UNO = uno,
                        OCATD = ocatd,
                        //STATUS = int.Parse(status)
                    };
                    ctx.Streets.Add(street);
                }
                int recs = ctx.SaveChanges();
                Console.WriteLine($"Записано {recs} записей");
            }
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта STREET: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта STREET: {tsp}", tsp);
    }
}
