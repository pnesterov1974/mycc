using DbfDataReader;
using import_kladr_ef.models;
using Microsoft.EntityFrameworkCore;
using Shared;
using static Shared.MyLogger;

namespace import_kladr_ef;

// TODO  import from dbf use types ?? or convert in model
// try-except review
// get tablename and schema name from model
// error in doma

public static class KladrEfcImport
{
    public static int SaveKladrSimple(TargetDb tdb, bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        int recs = 0;
        Log.Information("Начало импорта KLADR: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext(tdb))
        {
            Log.Information("Очистка целевой таблицы KLADR...");
            int numberOfRowDeleted = ctx.Database.ExecuteSqlRaw("DELETE FROM dbo.[KLADR];");
            Log.Information("Из таблицы KLADR удалено {recs} записей", numberOfRowDeleted);

            List<Kladr> kladr = KladrEfcImport.GetKladrImportList();
            foreach (var k in kladr)
            {
                ctx.Kladrs.Add(k);
            }
            recs = ctx.SaveChanges();
            Log.Information($"Записано {recs} записей", recs);
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта KLADR: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта KLADR: {tsp}", tsp);
        return recs;
    }

    public static List<Kladr> GetKladrImportList()
    {
        IImportModel oi = new KladrImportModel();
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
        Log.Information("Загружено {dataCount} записей", data.Count);
        return data;
    }

    public static void SaveKladrCombined(TargetDb tdb, bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта KLADR: {sdt}", startDt);
        IImportModel oi = new KladrImportModel();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        int batchRecordCount = oi.BufferRecs;
        Log.Information("Источник: {sourcePath}", sourceFilePath);
        Log.Information("Размер батча: {batchSize}", batchRecordCount);
        using (KladrContext ctx = new KladrContext(tdb))
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы dbo.[KLADR]");
                    ctx.Database.ExecuteSqlRaw("TRUNCATE TABLE dbo.[KLADR];");
                    Log.Information("Предварительная очистка успешна...");
                }
                using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
                {
                    int currentBatchRecs = 0;
                    int totalRecs = 0;
                    while (dbfDataReader.Read())
                    {
                        currentBatchRecs += 1;
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
                        if (currentBatchRecs - batchRecordCount == 0)
                        {
                            ctx.SaveChanges();
                            totalRecs += currentBatchRecs;
                            currentBatchRecs = 0;
                            Log.Information("Импортировано {currentRec}", totalRecs);
                        }
                    }

                    if (currentBatchRecs > 0)
                    {
                        ctx.SaveChanges();
                        totalRecs += currentBatchRecs;
                        currentBatchRecs = 0;
                    }
                    Log.Information("Всего Импортировано {recs} записей", totalRecs);
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

    public static void SaveStreetCombined(TargetDb tdb, bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта STREET: {sdt}", startDt);
        IImportModel oi = new StreetImportModel();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        int batchRecordCount = oi.BufferRecs;
        Log.Information("Источник: {sourcePath}", sourceFilePath);
        Log.Information("Размер батча: {batchSize}", batchRecordCount);
        using (KladrContext ctx = new KladrContext(tdb))
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы dbo.[STREET]");
                    ctx.Database.ExecuteSqlRaw("TRUNCATE TABLE dbo.[STREET];");
                    Log.Information("Предварительная очистка успешна...");
                }
                using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
                {
                    int currentBatchRecs = 0;
                    int totalRecs = 0;
                    while (dbfDataReader.Read())
                    {
                        currentBatchRecs += 1;
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
                        if (currentBatchRecs - batchRecordCount == 0)
                        {
                            ctx.SaveChanges();
                            totalRecs += currentBatchRecs;
                            currentBatchRecs = 0;
                            Log.Information("Импортировано {currentRec}", totalRecs);
                        }
                    }

                    if (currentBatchRecs > 0)
                    {
                        ctx.SaveChanges();
                        totalRecs += currentBatchRecs;
                        currentBatchRecs = 0;
                    }
                    Log.Information("Всего Импортировано {recs} записей", totalRecs);
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

    public static void SaveDomaCombined(TargetDb tdb, bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта DOMA: {sdt}", startDt);
        IImportModel oi = new DomaImportModel();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        int batchRecordCount = oi.BufferRecs;
        Log.Information("Источник: {sourcePath}", sourceFilePath);
        Log.Information("Размер батча: {batchSize}", batchRecordCount);
        using (KladrContext ctx = new KladrContext(tdb))
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы dbo.[DOMA]");
                    ctx.Database.ExecuteSqlRaw("TRUNCATE TABLE dbo.[DOMA];");
                    Log.Information("Предварительная очистка успешна...");
                }
                using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
                {
                    int currentBatchRecs = 0;
                    int totalRecs = 0;
                    while (dbfDataReader.Read())
                    {
                        currentBatchRecs += 1;
                        var name = dbfDataReader.GetString(0);
                        var socr = dbfDataReader.GetString(1);
                        var code = dbfDataReader.GetString(2);
                        var index = dbfDataReader.GetString(3);
                        var gninmb = dbfDataReader.GetString(4);
                        var uno = dbfDataReader.GetString(5);
                        var ocatd = dbfDataReader.GetString(6);

                        var doma = new Doma
                        {
                            NAME = name,
                            SOCR = socr,
                            CODE = code,
                            INDEX = index,
                            GNINMB = gninmb,
                            UNO = uno,
                            OCATD = ocatd
                        };
                        ctx.Domas.Add(doma);
                        if (currentBatchRecs - batchRecordCount == 0)
                        {
                            ctx.SaveChanges();
                            totalRecs += currentBatchRecs;
                            currentBatchRecs = 0;
                            Log.Information("Импортировано {currentRec}", totalRecs);
                        }
                    }

                    if (currentBatchRecs > 0)
                    {
                        ctx.SaveChanges();
                        totalRecs += currentBatchRecs;
                        currentBatchRecs = 0;
                    }
                    Log.Information("Всего Импортировано {recs} записей", totalRecs);
                }
            }
            else
            {
                Log.Information("Подключение не возможно");
            }
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта DOMA: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта DOMA: {tsp}", tsp);
    }

    public static void SaveAltNamesCombined(TargetDb tdb, bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта ALTNAMES: {sdt}", startDt);
        IImportModel oi = new AltNamesImportModel();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        int batchRecordCount = oi.BufferRecs;
        Log.Information("Источник: {sourcePath}", sourceFilePath);
        Log.Information("Размер батча: {batchSize}", batchRecordCount);
        using (KladrContext ctx = new KladrContext(tdb))
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы dbo.[ALTNAMES]");
                    ctx.Database.ExecuteSqlRaw("TRUNCATE TABLE dbo.[ALTNAMES];");
                    Log.Information("Предварительная очистка успешна...");
                }
                using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
                {
                    int currentBatchRecs = 0;
                    int totalRecs = 0;
                    while (dbfDataReader.Read())
                    {
                        currentBatchRecs += 1;
                        var oldCode = dbfDataReader.GetString(0);
                        var newCode = dbfDataReader.GetString(1);
                        var level = dbfDataReader.GetString(2);

                        var altNames = new AltNames
                        {
                            OLDCODE = oldCode,
                            NEWCODE = newCode,
                            LEVEL = int.Parse(level)
                        };
                        ctx.AltNames.Add(altNames);
                        if (currentBatchRecs - batchRecordCount == 0)
                        {
                            ctx.SaveChanges();
                            totalRecs += currentBatchRecs;
                            currentBatchRecs = 0;
                            Log.Information("Импортировано {currentRec}", totalRecs);
                        }
                    }

                    if (currentBatchRecs > 0)
                    {
                        ctx.SaveChanges();
                        totalRecs += currentBatchRecs;
                        currentBatchRecs = 0;
                    }
                    Log.Information("Всего Импортировано {recs} записей", totalRecs);
                }
            }
            else
            {
                Log.Information("Подключение не возможно");
            }
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта ALTNAMES: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта ALTNAMES: {tsp}", tsp);
    }

    public static void SaveSocrBaseCombined(TargetDb tdb, int batchRecords = 10000, bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта SOCRBASE: {sdt}", startDt);
        IImportModel oi = new SocrBaseImportModel();
        DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
        string sourceFilePath = Path.Combine(oi.SourceDirPath, oi.SourceFileName);
        int batchRecordCount = oi.BufferRecs;
        Log.Information("Источник: {sourcePath}", sourceFilePath);
        Log.Information("Размер батча: {batchSize}", batchRecordCount);
        using (KladrContext ctx = new KladrContext(tdb))
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы dbo.[SOCRBASE]");
                    ctx.Database.ExecuteSqlRaw("TRUNCATE TABLE dbo.[SOCRBASE];");
                    Log.Information("Предварительная очистка успешна...");
                }
                using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, dbfOptions))
                {
                    int currentBatchRecs = 0;
                    int totalRecs = 0;
                    while (dbfDataReader.Read())
                    {
                        currentBatchRecs += 1;
                        var level = dbfDataReader.GetString(0);
                        var scName = dbfDataReader.GetString(1);
                        var socrName = dbfDataReader.GetString(2);
                        var kodTst = dbfDataReader.GetString(3);

                        var socrBase = new SocrBase
                        {
                            LEVEL = int.Parse(level),
                            SCNAME = scName,
                            SOCRNAME = socrName,
                            KOD_T_ST = kodTst
                        };
                        ctx.SocrBases.Add(socrBase);
                        if (currentBatchRecs - batchRecordCount == 0)
                        {
                            ctx.SaveChanges();
                            totalRecs += currentBatchRecs;
                            currentBatchRecs = 0;
                            Log.Information("Импортировано {currentRec}", totalRecs);
                        }
                    }

                    if (currentBatchRecs > 0)
                    {
                        ctx.SaveChanges();
                        totalRecs += currentBatchRecs;
                        currentBatchRecs = 0;
                    }
                    Log.Information("Всего Импортировано {recs} записей", totalRecs);
                }
            }
            else
            {
                Log.Information("Подключение не возможно");
            }
        }
        DateTime finishDt = DateTime.Now;
        Log.Information("Завершение импорта SOCRBASE: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта SOCRBASE: {tsp}", tsp);
    }
}
