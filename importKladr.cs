using DbfDataReader;
using import_kladr_efcms.models;
using Microsoft.EntityFrameworkCore;
using static Shared.MyLogger;

namespace import_kladr_efcms;

public static class KladrEfcImport
{
    public static int SaveKladrSimple(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        int recs = 0;
        Log.Information("Начало импорта KLADR: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext())
        {
            Log.Information("Очистка целевой таблицы KLADR...");

            int numberOfRowDeleted = ctx.Database.ExecuteSqlRaw("DELETE FROM dbo.[KLADR];");
            Log.Information("Очистка целевой таблицы KLADR успешно завершена");

            List<Kladr> kladr = KladrEfcImport.GetKladrImportList();
            foreach (var k in kladr)
            {
                ctx.Kladrs.Add(k);
            }
            recs = ctx.SaveChanges();
            Console.WriteLine($"Записано {recs} записей");
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта KLADR: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта KLADR: {tsp}", tsp);
        return recs;
    }

    public static List<Kladr> GetKladrImportList()
    {
        IImportObjectInfo oi = new KladrImportObjectInfo();
        List<Kladr> data = new List<Kladr>();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        Log.Information("Загрузка KLADR из {kladrPath}", sourceFilePath);
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
        Log.Information("Загружено {data.Count} записей", data.Count);
        return data;
    }

    public static List<Street> GetStreetImportList()
    {
        IImportObjectInfo oi = new StreetImportObjectInfo();
        List<Street> data = new List<Street>();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        Log.Information("Загрузка STREET из {streetPath}", sourceFilePath);
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
        Log.Information("Загружено {data.Count} записей", data.Count);
        return data;
    }

    public static void SaveKladrCombined(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта KLADR: {sdt}", startDt);
        IImportObjectInfo oi = new KladrImportObjectInfo();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        Console.WriteLine(sourceFilePath);
        using (KladrContext ctx = new KladrContext())
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы dbo.[KLADR]");
                    int numberOfRowDeleted = ctx.Database.ExecuteSqlRaw("DELETE FROM dbo.[KLADR];");
                    Log.Information("Удадено {deletedRows} записей", numberOfRowDeleted);
                }
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
            else
            {
                Log.Information("Подключение не возможно");
            }
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта KLADR: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта KLADR: {tsp}", tsp);
    }

    public static void SaveStreetCombined(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта STREET: {sdt}", startDt);
        IImportObjectInfo oi = new StreetImportObjectInfo();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        Console.WriteLine(sourceFilePath);
        using (KladrContext ctx = new KladrContext())
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы dbo.[STREET]");
                    int numberOfRowDeleted = ctx.Database.ExecuteSqlRaw("DELETE FROM dbo.[STREET];");
                    Log.Information("Удадено {deletedRows} записей", numberOfRowDeleted);
                }
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
                            OCATD = ocatd,
                        };
                        ctx.Streets.Add(street);
                    }
                    int recs = ctx.SaveChanges();
                    Console.WriteLine($"Записано {recs} записей");
                }
            }
            else
            {
                Log.Information("Подключение не возможно");
            }
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта STREET: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта STREET: {tsp}", tsp);
    }
}
