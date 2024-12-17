using Microsoft.EntityFrameworkCore;
using Shared;
using static Shared.MyLogger;

namespace importGeoEfc;

// TODO
// try-except review
// touch mssql names
// objects names in logging from importModel
// date of data in txt file
// ctx.Database.ExecuteSqlRaw($"TRUNCATE TABLE {tableFullName};"); vs ExecuteSql ?? Intrusion ??

public class KladrEfcImportWorker
{
    public TargetDb tdb;
    public void SaveKladrUsingEnumerator(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта KLADR: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext(this.tdb))
        {
            var tableFullName = ctx.Kladrs.GetTableFullName();
            Log.Information("Имя целевлй таблицы: {tableName}", tableFullName);
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы {tableName}", tableFullName);
                    ctx.Database.ExecuteSqlRaw($"TRUNCATE TABLE {tableFullName};");
                    Log.Information("Предварительная очистка успешна...");
                }

                var kladrSource = new KladrSourceEnumerator();
                int batchRecordCount = kladrSource.ImportModel.BufferRecs;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = kladrSource.KladrIter();
                //foreach (var kladrRec in kladrSource)
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.Kladrs.Add(rec);
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

    public void SaveStreetUsingEnumerator(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта STREET: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext(this.tdb))
        {
            var tableFullName = ctx.Streets.GetTableFullName();
            Log.Information("Имя целевлй таблицы: {tableName}", tableFullName);
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы {tableName}", tableFullName);
                    ctx.Database.ExecuteSqlRaw($"TRUNCATE TABLE {tableFullName};");
                    Log.Information("Предварительная очистка успешна...");
                }

                var streetSource = new StreetSourceEnumerator();
                int batchRecordCount = streetSource.ImportModel.BufferRecs;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = streetSource.StreetIter();
                foreach (var streetRec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.Streets.Add(streetRec);
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

    public void SaveDomaUsingEnumerator(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта DOMA: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext(this.tdb))
        {
            var tableFullName = ctx.Domas.GetTableFullName();
            Log.Information("Имя целевлй таблицы: {tableName}", tableFullName);
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы {tableName}", tableFullName);
                    ctx.Database.ExecuteSqlRaw($"TRUNCATE TABLE {tableFullName};");
                    Log.Information("Предварительная очистка успешна...");
                }

                var domaSource = new DomaSourceEnumerator();
                int batchRecordCount = domaSource.ImportModel.BufferRecs;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = domaSource.DomaIter();
                foreach (var domaRec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.Domas.Add(domaRec);
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

    public void SaveAltNamesUsingEnumerator(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта ALTNAMES: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext(this.tdb))
        {
            var tableFullName = ctx.AltNames.GetTableFullName();
            Log.Information("Имя целевлй таблицы: {tableName}", tableFullName);
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы {tableName}", tableFullName);
                    ctx.Database.ExecuteSqlRaw($"TRUNCATE TABLE {tableFullName};");
                    Log.Information("Предварительная очистка успешна...");
                }

                var altNamesSource = new AltNamesSourceEnumerator();
                int batchRecordCount = altNamesSource.ImportModel.BufferRecs;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = altNamesSource.AltNamesIter();
                foreach (var altNamesRec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.AltNames.Add(altNamesRec);
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

    public void SaveSocrBaseUsingEnumerator(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта SOCRBASE: {sdt}", startDt);
        using (KladrContext ctx = new KladrContext(this.tdb))
        {
            var tableFullName = ctx.SocrBases.GetTableFullName();
            Log.Information("Имя целевлй таблицы: {tableName}", tableFullName);
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                if (clearTargetTableInAdvance)
                {
                    Log.Information("Предварительная очистка целевой таблицы {tableName}", tableFullName);
                    ctx.Database.ExecuteSqlRaw($"TRUNCATE TABLE {tableFullName};");
                    Log.Information("Предварительная очистка успешна...");
                }

                var socrBaseSource = new SocrBaseSourceEnumerator();
                int batchRecordCount = socrBaseSource.ImportModel.BufferRecs;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = socrBaseSource.SocrBaseIter();
                foreach (var socrBaseRec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.SocrBases.Add(socrBaseRec);
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
