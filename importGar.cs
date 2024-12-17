using importGeoEfc.models.garmodels;
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

public class GarEfcImportWorker
{
    public TargetDb tdb;
    public void SaveHouseTypes(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта HOUSE_TYPES: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.HouseTypes.GetTableFullName();
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

                var houseTypes = new HouseTypesEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = houseTypes.IterHouseTypes();
                foreach (var houseTypesRec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.HouseTypes.Add(houseTypesRec);
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
        Log.Information("Завершение импорта HOUSE_TYPES: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта HOUSE_TYPES: {tsp}", tsp);
    }

    public void SaveAddrObjTypes(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта ADDR_OBJ_TYPES: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.AddrObjTypes.GetTableFullName();
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

                var addrObjTypes = new AddrObjTypeEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = addrObjTypes.IterAddrObjTypes();
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.AddrObjTypes.Add(rec);
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
        Log.Information("Завершение импорта ADDR_OBJ_TYPES: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта ADDR_OBJ_TYPES: {tsp}", tsp);
    }

    public void SaveAppartmentTypes(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта APPARTMENT_TYPES: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.AppartmentTypes.GetTableFullName();
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

                var appartmentTypes = new AppartmentTypeEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = appartmentTypes.IterAppartmentType();
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.AppartmentTypes.Add(rec);
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
        Log.Information("Завершение импорта APPARTMENT_TYPES: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта APPARTMENT_TYPES: {tsp}", tsp);
    }

    public void SaveNormativeDocsKinds(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта NORMATIVE_DOCS_KINDS: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.NormativeDocsKinds.GetTableFullName();
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

                var normativeDocsKind = new NormativeDocsKindEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = normativeDocsKind.IterNormativeDocsKinds();
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.NormativeDocsKinds.Add(rec);
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
        Log.Information("Завершение импорта NORMATIVE_DOCS_KINDS: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта NORMATIVE_DOCS_KINDS: {tsp}", tsp);
    }

    public void SaveNormativeDocsTypes(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта NORMATIVE_DOCS_TYPES: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.NormativeDocsTypes.GetTableFullName();
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

                var normativeDocTypes = new NormativeDocsTypeEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = normativeDocTypes.IterNormativeDocsTypes();
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.NormativeDocsTypes.Add(rec);
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
        Log.Information("Завершение импорта NORMATIVE_DOCS_TYPES: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта NORMATIVE_DOCS_TYPES: {tsp}", tsp);
    }

    public void SaveObjectLevels(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта OBJECT_LEVELS: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.ObjectLevels.GetTableFullName();
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

                var objectLevels = new ObjectLevelsEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = objectLevels.IterObjectLevels();
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.ObjectLevels.Add(rec);
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
        Log.Information("Завершение импорта OBJECT_LEVELS: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта OBJECT_LEVELS: {tsp}", tsp);
    }

    public void SaveOperationTypes(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта OPERATION_TYPES: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.OparationTypes.GetTableFullName();
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

                var operationTypes = new OperationTypesEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = operationTypes.IterOperationTypes();
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.OparationTypes.Add(rec);
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
        Log.Information("Завершение импорта OBJECT_LEVELS: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта OBJECT_LEVELS: {tsp}", tsp);
    }

    public void SaveParamTypes(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта PARAM_TYPES: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.ParamTypes.GetTableFullName();
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

                var paramTypes = new ParamTypesEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = paramTypes.IterParamTypes();
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.ParamTypes.Add(rec);
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
        Log.Information("Завершение импорта PARAM_TYPES: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта PARAM_TYPES: {tsp}", tsp);
    }

    public void SaveRoomTypes(bool clearTargetTableInAdvance = true)
    {
        DateTime startDt = DateTime.Now;
        Log.Information("Начало импорта ROOM_TYPES: {sdt}", startDt);
        using (GarContext ctx = new GarContext(this.tdb))
        {
            var tableFullName = ctx.RoomTypes.GetTableFullName();
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

                var paramTypes = new RoomTypesEnumerator();
                int batchRecordCount = 10_000;
                int currentBatchRecs = 0;
                int totalRecs = 0;
                var iter = paramTypes.IterRoomTypes();
                foreach (var rec in iter)
                {
                    currentBatchRecs += 1;
                    ctx.RoomTypes.Add(rec);
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
        Log.Information("Завершение импорта ROOM_TYPES: {sdt}", finishDt);
        TimeSpan tsp = finishDt - startDt;
        Log.Information("Длительность импорта ROOM_TYPES: {tsp}", tsp);
    }
}
